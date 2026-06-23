"""Vectorized pandas scoring UDFs, run on the Flink TaskManager.

One UDF per output column (PyFlink 1.18 pandas UDFs can't return a ROW). The model loads lazily and
hot-reloads on mtime change, so a retrain swaps in with no job restart. The per-batch memo is keyed by
batch content, not id() (CPython reuses ids after GC), so every column of a batch comes from one model
resolution even if a hot reload lands mid-batch. A reloaded artifact is validated against the contract before replacing the live one; a bad
one is rejected and the old model keeps scoring.
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
_memo = {"key": None, "proba": None, "version": None}   # per-batch cache, content-keyed


def _validate(art) -> bool:
    """accept an artifact only if its features/labels match the contract exactly."""
    return (isinstance(art, dict)
            and art.get("features") == contract.FEATURES
            and art.get("labels") == contract.LABELS
            and isinstance(art.get("models"), list)
            and len(art["models"]) == N_FAMILIES)


def _load():
    """cached artifact, reloaded only when mtime changes; keeps the old one if the new
    file is unreadable or contract-invalid."""
    try:
        mtime = os.path.getmtime(MODEL_PATH)
    except OSError:
        return _state["artifact"]               # no model yet (None on first calls)
    if _state["mtime"] == mtime:
        return _state["artifact"]
    with _lock:
        if _state["mtime"] == mtime:
            return _state["artifact"]
        try:
            art = modeling.load_artifact(MODEL_PATH)
        except Exception as ex:                 # partial/corrupt file: keep serving old
            # PyFlink replaces the worker's print(); no flush= kwarg accepted here.
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
    """cols: 16 pandas.Series in FEATURES order. Fills the memo with this batch's
    (proba matrix, version) from a single artifact resolution."""
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
