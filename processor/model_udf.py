"""
Vectorized Pandas scoring UDFs — executed on the Flink TaskManager.

Five DOUBLE UDFs (anomaly_score + 4 family probabilities) plus an INT model_version
UDF. All share ONE module-global model, loaded lazily and HOT-RELOADED when the
artifact's mtime changes (the retrain sidecar writes a new /models/model.pkl
atomically; we never restart the Flink job).

Consistency: the model artifact is resolved ONCE per Arrow batch. _proba() memoizes
(probabilities, model version) keyed by the batch CONTENT, and every UDF — including
model_version — reads that memo, so all 6 output columns of a batch always come from
the same artifact even if a hot reload lands mid-batch. (The memo is content-keyed,
not id()-keyed: CPython reuses ids after GC, which would alias a later batch.)

A reloaded artifact is validated against the contract (feature/label names + order)
before it replaces the live one; a bad artifact is rejected loudly and the previous
model keeps scoring — a corrupt retrain can never take down the running job.

PyFlink 1.18 vectorized pandas UDFs return a single pandas.Series each (ROW returns
are unsupported) — hence one UDF per output column. Varargs (*cols) keeps the call
in lockstep with contract.FEATURES so the feature matrix is always in training order.
"""

import os
import threading

import numpy as np
import pandas as pd
from pyflink.table import DataTypes
from pyflink.table.udf import udf

import contract
import modeling

MODEL_PATH = os.environ.get("MODEL_PATH", "/models/model.pkl")
N_FAMILIES = len(contract.LABELS)

_lock = threading.Lock()
_state = {"artifact": None, "mtime": None}              # cached model, keyed by file mtime
_memo = {"key": None, "proba": None, "version": None}   # per-batch cache (content-keyed)


def _validate(art) -> bool:
    """A model artifact is only accepted if its contract matches ours exactly."""
    return (isinstance(art, dict)
            and art.get("features") == contract.FEATURES
            and art.get("labels") == contract.LABELS
            and isinstance(art.get("models"), list)
            and len(art["models"]) == N_FAMILIES)


def _load():
    """Return the cached artifact, reloading iff /models/model.pkl mtime changed.
    Keeps the previous artifact if the new file is unreadable or contract-invalid."""
    try:
        mtime = os.path.getmtime(MODEL_PATH)
    except OSError:
        return _state["artifact"]               # not present yet (None on first calls)
    if _state["mtime"] == mtime:
        return _state["artifact"]
    with _lock:
        if _state["mtime"] == mtime:
            return _state["artifact"]
        try:
            art = modeling.load_artifact(MODEL_PATH)
        except Exception as ex:                 # partial/corrupt file: keep serving old
            # NOTE: PyFlink replaces the worker's print(); no flush= kwarg here.
            print(f"[model_udf] FAILED to load {MODEL_PATH}: {ex} — keeping current model")
            return _state["artifact"]
        if not _validate(art):
            print(f"[model_udf] REJECTED {MODEL_PATH}: feature/label contract mismatch "
                  f"(got features={art.get('features')!r:.120}) — keeping current model")
            _state["mtime"] = mtime             # don't retry the same bad file every batch
            return _state["artifact"]
        _state["artifact"], _state["mtime"] = art, mtime
        _memo["key"] = None                     # invalidate memo on reload
        print(f"[model_udf] loaded model version {art.get('version')} "
              f"(threshold {art.get('threshold')}, mtime {mtime})")
    return _state["artifact"]


def _score_batch(cols):
    """cols: list of 16 pandas.Series in FEATURES order. Ensures the memo holds this
    batch's (proba matrix, model version), computed with ONE artifact resolution."""
    first = cols[0]
    key = (len(first), hash(tuple(np.asarray(c).tobytes() for c in cols)))
    if _memo["key"] == key and _memo["proba"] is not None:
        return _memo
    art = _load()
    X = np.column_stack([np.asarray(c, dtype="float64") for c in cols])
    X = modeling.clean_features(X)
    if art is None:
        proba = np.zeros((len(first), N_FAMILIES), dtype="float64")
        version = 0
    else:
        proba = modeling.predict_proba_matrix(art, X)
        version = int(art.get("version", 0))
    _memo["key"], _memo["proba"], _memo["version"] = key, proba, version
    return _memo


@udf(result_type=DataTypes.DOUBLE(), func_type="pandas")
def score_anomaly(*cols):
    return pd.Series(modeling.anomaly_scores(_score_batch(list(cols))["proba"]))


def _family_udf(index):
    @udf(result_type=DataTypes.DOUBLE(), func_type="pandas")
    def fn(*cols):
        return pd.Series(_score_batch(list(cols))["proba"][:, index])
    return fn


p_electric = _family_udf(0)
p_bearing = _family_udf(1)
p_workroll = _family_udf(2)
p_reduction = _family_udf(3)


@udf(result_type=DataTypes.INT(), func_type="pandas")
def model_version(*cols):
    m = _score_batch(list(cols))
    return pd.Series(np.full(len(cols[0]), m["version"], dtype="int32"))
