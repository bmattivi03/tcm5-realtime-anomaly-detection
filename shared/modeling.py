"""
Shared modeling logic — the ONE definition of how the model is trained, scored,
thresholded and persisted. Imported by the retrain sidecar and the Flink scoring
UDF, so they stay in lockstep with shared/contract.py.

The model is a per-family ensemble: one HistGradientBoostingClassifier per fault
family (electric, bearing, workroll, reduction), each trained with class-balanced
sample weights (HGB in sklearn 1.3 has no class_weight, so we weight samples). The
artifact is a plain dict of fitted estimators + metadata — no custom classes — so it
pickles/unpickles cleanly across the retrain sidecar and the Flink TaskManager (identical
numpy/scikit-learn versions are pinned in both images).

    anomaly_score = max over the 4 family probabilities.
"""

from __future__ import annotations

import os
import json
import pickle
import numpy as np

from sklearn.ensemble import HistGradientBoostingClassifier
from sklearn.utils.class_weight import compute_sample_weight
from sklearn.metrics import average_precision_score

import contract

MODEL_FILE = "model.pkl"
META_FILE = "model.meta"

# Postgres advisory-lock key shared by every model publisher (the retrain sidecar API
# and a manual `python retrain.py` CLI run) so version allocation + artifact writes
# serialize across processes.
PUBLISH_LOCK_KEY = 815001


# ---------------------------------------------------------------------------
# Training
# ---------------------------------------------------------------------------
def train_models(X: np.ndarray, Y: np.ndarray, **hgb_kwargs):
    """Train one balanced HGB classifier per family. X:(n,16) float, Y:(n,4) bool.

    Returns a list of 4 fitted estimators in contract.LABELS order. A family with a
    single class present (e.g. a rare fault with no positives yet in an early cold-start
    train) gets a constant estimator so predict_proba still returns a valid (n,2)-shaped result.
    """
    params = dict(max_iter=200, learning_rate=0.08, max_leaf_nodes=31,
                  l2_regularization=1.0, random_state=0)
    params.update(hgb_kwargs)

    models = []
    for j in range(Y.shape[1]):
        y = Y[:, j].astype(int)
        if len(np.unique(y)) < 2:
            # Only one class seen for this family (e.g. a rare fault absent from an early
            # cold-start train): a degenerate estimator that always predicts the seen class.
            models.append(_ConstantClassifier(seen=int(y[0]) if len(y) else 0))
            continue
        sw = compute_sample_weight("balanced", y)
        clf = HistGradientBoostingClassifier(**params)
        clf.fit(X, y, sample_weight=sw)
        models.append(clf)
    return models


class _ConstantClassifier:
    """Minimal stand-in for a family never observed positive: P(positive)=0."""
    def __init__(self, seen: int = 0):
        self.seen = seen
        self.classes_ = np.array([0, 1])

    def predict_proba(self, X):
        n = len(X)
        p = np.zeros((n, 2), dtype="float64")
        p[:, 0] = 1.0
        return p


# ---------------------------------------------------------------------------
# Feature hygiene — THE one NaN/inf policy, applied identically at train time
# (the retrain sidecar) and at scoring time (Flink UDF). The lenient JSON
# source nulls missing fields, so streamed rows can carry NaN; training and
# serving must see the same values or the model silently skews.
# ---------------------------------------------------------------------------
def clean_features(X: np.ndarray) -> np.ndarray:
    return np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)


# ---------------------------------------------------------------------------
# Scoring
# ---------------------------------------------------------------------------
def predict_proba_matrix(artifact: dict, X: np.ndarray) -> np.ndarray:
    """(n,4) matrix of per-family positive-class probabilities, in LABELS order."""
    models = artifact["models"]
    cols = []
    for m in models:
        p = m.predict_proba(X)
        cols.append(p[:, 1] if p.ndim == 2 and p.shape[1] == 2 else np.asarray(p).ravel())
    return np.column_stack(cols)


def anomaly_scores(proba: np.ndarray) -> np.ndarray:
    """Overall anomaly score per event = max family probability."""
    return proba.max(axis=1)


# ---------------------------------------------------------------------------
# Threshold tuning (on natural-prevalence data — never class-balanced)
# ---------------------------------------------------------------------------
def tune_threshold(Y, scores: np.ndarray) -> float:
    """Pick the decision threshold on anomaly_score that maximizes overall F1 at the
    natural prevalence of the supplied sample.

    ``Y`` is the (n, 4) label matrix (a 1-D y_any vector is also accepted for backward
    compatibility); only the any-fault indicator is used here.
    """
    Y = np.asarray(Y)
    y_any = (Y.any(axis=1) if Y.ndim > 1 else Y).astype(bool)
    if not y_any.any() or y_any.all():
        # Degenerate validation slice (no positives, or no negatives): every threshold ties
        # at F1 0 and the sweep would return 0.05 (alarm on everything). Neutral fallback.
        return 0.5
    best_t, best_f1 = 0.5, -1.0
    for t in np.linspace(0.05, 0.95, 19):
        pred = scores >= t
        tp = int(np.sum(pred & y_any))
        fp = int(np.sum(pred & ~y_any))
        fn = int(np.sum(~pred & y_any))
        f1 = (2 * tp) / (2 * tp + fp + fn) if (2 * tp + fp + fn) > 0 else 0.0
        if f1 > best_f1:
            best_f1, best_t = f1, float(t)
    return best_t


# ---------------------------------------------------------------------------
# Evaluation on the fixed held-out set (consistent across model versions)
# ---------------------------------------------------------------------------
def evaluate(artifact: dict, X: np.ndarray, Y: np.ndarray, threshold: float) -> dict:
    proba = predict_proba_matrix(artifact, X)
    scores = anomaly_scores(proba)
    y_any = Y.any(axis=1)
    pred = scores >= threshold

    tp = int(np.sum(pred & y_any)); fp = int(np.sum(pred & ~y_any)); fn = int(np.sum(~pred & y_any))
    precision = tp / (tp + fp) if (tp + fp) else 0.0
    recall = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = (2 * precision * recall) / (precision + recall) if (precision + recall) else 0.0
    pr_auc = float(average_precision_score(y_any, scores)) if y_any.any() and (~y_any).any() else 0.0

    # per-family recall: of events truly of family F, fraction whose anomaly_score >= threshold
    fam_recall = {}
    for j, fam in enumerate(contract.LABELS):
        yf = Y[:, j]
        fam_recall[fam] = float(np.mean(pred[yf])) if yf.any() else 0.0

    return {
        "precision": float(precision), "recall": float(recall), "f1": float(f1),
        "pr_auc": pr_auc,
        "recall_electric": fam_recall["electric"], "recall_bearing": fam_recall["bearing"],
        "recall_workroll": fam_recall["workroll"], "recall_reduction": fam_recall["reduction"],
        "n_eval": int(len(Y)),
    }


# ---------------------------------------------------------------------------
# Persistence — atomic write (metadata BEFORE model, then os.replace).
# The Flink UDF triggers a hot-reload on model.pkl's mtime, so the model file is
# swapped LAST; metadata is always already in place.
# Temp names are unique per writer (pid) so concurrent sidecar writes (API + CLI)
# degrade to last-writer-wins instead of corrupting each other's temp files.
# ---------------------------------------------------------------------------
def atomic_pickle(path: str, obj) -> None:
    """Write a pickle atomically: unique temp file in the same dir, fsync, replace."""
    d, base = os.path.split(path)
    tmp = os.path.join(d, f".{base}.{os.getpid()}.tmp")
    with open(tmp, "wb") as f:
        pickle.dump(obj, f, protocol=pickle.HIGHEST_PROTOCOL)
        f.flush(); os.fsync(f.fileno())
    os.replace(tmp, path)


def atomic_write_artifact(model_dir: str, artifact: dict) -> None:
    meta = {
        "version": artifact["version"],
        "threshold": artifact["threshold"],
        "features": artifact["features"],
        "labels": artifact["labels"],
        "trained_at": artifact["trained_at"],
        "trained_on": artifact.get("trained_on", ""),
        "metrics": artifact.get("metrics", {}),
    }
    meta_tmp = os.path.join(model_dir, f".model.meta.{os.getpid()}.tmp")
    with open(meta_tmp, "w") as f:
        json.dump(meta, f, indent=2)
        f.flush(); os.fsync(f.fileno())
    os.replace(meta_tmp, os.path.join(model_dir, META_FILE))

    atomic_pickle(os.path.join(model_dir, MODEL_FILE), artifact)


def load_artifact(path: str) -> dict:
    with open(path, "rb") as f:
        return pickle.load(f)
