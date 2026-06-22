"""
Unsupervised, model-free, in-stream anomaly detectors — executed on the Flink
TaskManager as vectorized pandas UDFs, alongside the supervised model UDFs.

Three detectors, each from a different family of the time-series anomaly taxonomy,
each keeping streaming state KEYED PER STAND (stands run at very different force /
torque regimes, so one global model would be meaningless) and each emitting a
STANDARDIZED score with a FIXED cutoff so a clean 2-of-3 majority vote can be
computed downstream in SQL:

  u_maha  distribution   Mahalanobis D² = (x-µ)ᵀ Σ⁻¹ (x-µ) over EWMA mean/cov.
                         Trigger D² > 23.2  (χ²₀.₉₉, 10 dof) — the tail rule.
  u_spc   control chart  max_i |z_i| over the per-feature EWMA mean/std.
                         Trigger > 3        (the 3σ rule).
  u_knn   distance       standardized mean distance to the k=5 nearest neighbours
                         in a rolling buffer of recent z-scored coils.
                         Trigger > 3.

  u_votes  = 1{u_maha>23.2} + 1{u_spc>3} + 1{u_knn>3}
  u_anomaly = u_votes >= 2          (computed in the Flink SQL, see processor.py)

These need NO trained model, so they score from the very first coil — they are the
live detector during the cold-start window before the supervised model is trained,
and a model-free second opinion afterwards. They run on the 10 PROCESS signals only
(measured + tensions), never the product set-points (thickness/width/ys) or `stand`,
so a different coil spec is not mistaken for a process fault.

State is module-global per stand (parallelism is 1, same pattern as model_udf's model
cache): adaptive (EWMA forgetting), not checkpointed (re-warms on restart), and gated
by a warmup so it never triggers before it has learnt the normal envelope.
"""

import numpy as np

import contract
import modeling

# 10 process signals the detectors watch (exclude 5 set-points + the stand index).
PROCESS_FEATURES = ["work_roll_diam", "work_roll_mileage", "reduction", "roll_speed",
                    "force", "torque", "gap", "motor_power", "tension_in", "tension_out"]
PROCESS_IDX = [contract.FEATURES.index(f) for f in PROCESS_FEATURES]
STAND_IDX = contract.FEATURES.index("stand")
P = len(PROCESS_IDX)

# Fixed cutoffs (kept in SQL too — keep in sync with processor.py).
MAHA_THRESHOLD = 23.21          # chi-square 0.99 quantile, 10 dof
SPC_THRESHOLD = 3.0             # 3 sigma
KNN_THRESHOLD = 3.0             # 3 sigma on the standardized kNN distance

# Streaming hyper-parameters.
ALPHA = 0.002                   # per-sample EWMA rate (~500-sample effective window)
WARMUP = 200                    # samples/stand before any detector may trigger
EPS = 1e-3                      # covariance ridge so Σ stays invertible
KNN_BUF = 300                   # rolling reference buffer size per stand
KNN_K = 5
KNN_MIN = 50                    # min buffer before kNN may score

_state = {}                     # stand -> dict of running state
_memo = {"key": None, "u_maha": None, "u_spc": None, "u_knn": None}


def _blank():
    return {"n": 0, "mean": np.zeros(P), "cov": np.eye(P),
            "buf": np.empty((0, P)), "dmean": 0.0, "dvar": 1.0}


def _update(st, Xs):
    """Batched exponentially-weighted update of the per-stand mean + covariance, and
    append the z-scored batch rows to the kNN buffer."""
    m = len(Xs)
    bmean = Xs.mean(axis=0)
    if st["n"] == 0:
        st["mean"] = bmean
        within = (Xs - bmean).T @ (Xs - bmean) / max(m, 1)
        st["cov"] = within + EPS * np.eye(P)
    else:
        beta = 1.0 - (1.0 - ALPHA) ** m
        diff = bmean - st["mean"]
        within = (Xs - bmean).T @ (Xs - bmean) / m
        st["cov"] = (1 - beta) * st["cov"] + beta * within + beta * (1 - beta) * np.outer(diff, diff)
        st["mean"] = st["mean"] + beta * diff
    st["n"] += m
    std = np.sqrt(np.maximum(np.diag(st["cov"]), 1e-12))
    z = (Xs - st["mean"]) / std
    st["buf"] = np.vstack([st["buf"], z])[-KNN_BUF:]


def score_batch(cols):
    """cols: list of FEATURES-ordered arrays/Series (the 16 model features). Returns the
    memoized (u_maha, u_spc, u_knn) for this batch — computed once, like model_udf."""
    first = np.asarray(cols[0])
    key = (len(first), hash(tuple(np.asarray(c).tobytes() for c in cols)))
    if _memo["key"] == key and _memo["u_maha"] is not None:
        return _memo

    X = modeling.clean_features(np.column_stack([np.asarray(c, dtype="float64") for c in cols]))
    stand = np.asarray(cols[STAND_IDX]).astype(int)
    Xp = X[:, PROCESS_IDX]
    n = len(first)
    u_maha = np.zeros(n)
    u_spc = np.zeros(n)
    u_knn = np.zeros(n)

    for s in np.unique(stand):
        mask = stand == s
        Xs = Xp[mask]
        st = _state.setdefault(int(s), _blank())

        if st["n"] >= WARMUP:
            std = np.sqrt(np.maximum(np.diag(st["cov"]), 1e-12))
            d = Xs - st["mean"]
            # distribution: Mahalanobis D²
            cov_r = st["cov"] + EPS * np.eye(P)
            try:
                inv = np.linalg.inv(cov_r)
            except np.linalg.LinAlgError:
                inv = np.linalg.pinv(cov_r)
            u_maha[mask] = np.einsum("ij,jk,ik->i", d, inv, d)
            # control chart: worst per-feature z
            z = d / std
            u_spc[mask] = np.abs(z).max(axis=1)
            # distance: standardized kNN distance to the rolling buffer
            buf = st["buf"]
            if len(buf) >= KNN_MIN:
                dist = np.sqrt(((z[:, None, :] - buf[None, :, :]) ** 2).sum(axis=2))
                k = min(KNN_K, dist.shape[1])
                knn_d = np.partition(dist, k - 1, axis=1)[:, :k].mean(axis=1)
                dstd = np.sqrt(max(st["dvar"], 1e-9))
                u_knn[mask] = (knn_d - st["dmean"]) / dstd
                # EWMA-update the kNN distance distribution (for next standardization)
                beta = 1.0 - (1.0 - ALPHA) ** len(knn_d)
                bm = knn_d.mean()
                ddiff = bm - st["dmean"]
                bvar = knn_d.var() if len(knn_d) > 1 else 0.0
                st["dvar"] = (1 - beta) * st["dvar"] + beta * bvar + beta * (1 - beta) * ddiff ** 2
                st["dmean"] = st["dmean"] + beta * ddiff

        _update(st, Xs)

    _memo.update(key=key, u_maha=u_maha, u_spc=u_spc, u_knn=u_knn)
    return _memo


# --- PyFlink UDF wrappers (optional import so the core stays unit-testable offline) ---
try:
    import pandas as pd
    from pyflink.table import DataTypes
    from pyflink.table.udf import udf

    @udf(result_type=DataTypes.DOUBLE(), func_type="pandas")
    def u_maha(*cols):
        return pd.Series(score_batch(list(cols))["u_maha"])

    @udf(result_type=DataTypes.DOUBLE(), func_type="pandas")
    def u_spc(*cols):
        return pd.Series(score_batch(list(cols))["u_spc"])

    @udf(result_type=DataTypes.DOUBLE(), func_type="pandas")
    def u_knn(*cols):
        return pd.Series(score_batch(list(cols))["u_knn"])
except ImportError:                      # offline (no pyflink): core functions still usable
    pd = None
    u_maha = u_spc = u_knn = None
