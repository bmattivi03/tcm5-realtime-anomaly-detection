"""Model-free in-stream anomaly detectors, run as pandas UDFs on the Flink TaskManager.

Three detectors, state keyed per stand (stands run at different force/torque regimes, so
one global model is meaningless), each emitting a standardized score with a fixed cutoff so
a 2-of-3 majority vote can run downstream in SQL:

  u_maha  Mahalanobis D² over EWMA mean/cov.   trigger D² > 23.2 (chi-square 0.99, 10 dof)
  u_spc   max per-feature |z| over EWMA mean/std.  trigger > 3 (3 sigma)
  u_knn   standardized mean distance to k=5 nearest neighbours in a rolling z-scored buffer.  trigger > 3

  u_votes / u_anomaly (votes >= 2) are computed in the Flink SQL, see processor.py.

No trained model, so they score from the first coil: the live detector during cold-start,
a second opinion afterwards. They watch only the 10 process signals (measured + tensions),
never the product set-points or `stand`, so a different coil spec is not read as a fault.

Per-stand state is module-global (parallelism 1), EWMA-adaptive, not checkpointed (re-warms
on restart), and gated by a warmup so it can't trigger before it has the normal envelope.
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
    """EWMA update of per-stand mean + covariance; append z-scored rows to the kNN buffer."""
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
    """cols: FEATURES-ordered arrays (16 features). Returns memoized (u_maha, u_spc, u_knn),
    computed once per batch (memo keyed by batch content, like model_udf)."""
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
            # Mahalanobis D²
            cov_r = st["cov"] + EPS * np.eye(P)
            try:
                inv = np.linalg.inv(cov_r)
            except np.linalg.LinAlgError:
                inv = np.linalg.pinv(cov_r)
            u_maha[mask] = np.einsum("ij,jk,ik->i", d, inv, d)
            # SPC: worst per-feature z
            z = d / std
            u_spc[mask] = np.abs(z).max(axis=1)
            # standardized kNN distance to the rolling buffer
            buf = st["buf"]
            if len(buf) >= KNN_MIN:
                dist = np.sqrt(((z[:, None, :] - buf[None, :, :]) ** 2).sum(axis=2))
                k = min(KNN_K, dist.shape[1])
                knn_d = np.partition(dist, k - 1, axis=1)[:, :k].mean(axis=1)
                dstd = np.sqrt(max(st["dvar"], 1e-9))
                u_knn[mask] = (knn_d - st["dmean"]) / dstd
                # EWMA the kNN-distance mean/var so the next batch can standardize against it
                beta = 1.0 - (1.0 - ALPHA) ** len(knn_d)
                bm = knn_d.mean()
                ddiff = bm - st["dmean"]
                bvar = knn_d.var() if len(knn_d) > 1 else 0.0
                st["dvar"] = (1 - beta) * st["dvar"] + beta * bvar + beta * (1 - beta) * ddiff ** 2
                st["dmean"] = st["dmean"] + beta * ddiff

        _update(st, Xs)

    _memo.update(key=key, u_maha=u_maha, u_spc=u_spc, u_knn=u_knn)
    return _memo


# --- PyFlink UDF wrappers (optional import: core stays usable without pyflink) ---
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
except ImportError:                      # no pyflink: core functions still usable
    pd = None
    u_maha = u_spc = u_knn = None
