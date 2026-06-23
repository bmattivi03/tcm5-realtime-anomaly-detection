#!/usr/bin/env python
"""Retrain sidecar: train a new model on accumulated streamed data, hot-swap with no processor restart.

Pulls deduped features + labels from scored_events, EXCLUDING the frozen held-out coils (no leakage),
trains the per-family model, evaluates on the same held-out set every time (comparable across versions),
writes the artifact atomically, and appends a model_versions row. The Flink UDF reloads on model.pkl mtime.

Runnable as a one-shot CLI (`python retrain.py`) or via the FastAPI app (POST /retrain).
"""

import os
import time
import logging
import warnings
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import psycopg2
from sklearn.model_selection import train_test_split

import contract
import modeling

logging.basicConfig(level=logging.INFO, format="%(asctime)s (%(levelname).1s) %(message)s")
log = logging.getLogger("retrain")
warnings.filterwarnings("ignore", message=".*pandas only supports SQLAlchemy.*")

DB_DSN = os.environ.get("DB_DSN", "host=localhost port=35432 dbname=db user=user password=user")
MODEL_DIR = os.environ.get("MODEL_DIR", "models")
HOLDOUT_FILE = os.path.join(MODEL_DIR, "holdout.pkl")


def _connect(retries=10):
    last = None
    for _ in range(retries):
        try:
            return psycopg2.connect(DB_DSN)
        except psycopg2.OperationalError as ex:
            last = ex
            time.sleep(2.0)
    raise last


def load_streamed(holdout_ids: set) -> pd.DataFrame:
    """Deduped per-(coil,stand) features + labels, minus held-out coils."""
    df = _load_all_streamed()
    if df.empty:
        return df
    return df[~df.coil_id.isin(holdout_ids)].reset_index(drop=True)


def _load_all_streamed() -> pd.DataFrame:
    """All deduped per-(coil,stand) features+labels (no holdout filter)."""
    cols = ", ".join(contract.FEATURES + contract.LABEL_KEYS)
    sql = (f"SELECT DISTINCT ON (coil_id, stand) coil_id, {cols} "
           f"FROM scored_events ORDER BY coil_id, stand, reading_id DESC")
    conn = _connect()
    try:
        return pd.read_sql_query(sql, conn)
    finally:
        conn.close()


# min positive coils per family before freezing the holdout. freezing too early (first TRAIN at
# ~400 coils) locks in 0 rare-family coils -> permanent 0% recall for electric/bearing/workroll.
MIN_HOLDOUT_PER_FAMILY = 25


def _carve_holdout(df: pd.DataFrame) -> dict:
    """Coil-level 15% held-out split, stratified by primary fault family."""
    fam = df.groupby("coil_id")[contract.LABEL_KEYS].any()

    def primary(r):
        for k in contract.LABEL_KEYS:
            if r[k]:
                return k
        return "normal"

    stratum = fam.apply(primary, axis=1).to_numpy()
    coil_ids = fam.index.to_numpy()
    try:
        _, hold = train_test_split(coil_ids, test_size=0.15, random_state=42, stratify=stratum)
    except ValueError:                       # stratum too small to stratify
        _, hold = train_test_split(coil_ids, test_size=0.15, random_state=42)
    hold_ids = sorted(int(c) for c in hold)
    hold_df = df[df.coil_id.isin(set(hold_ids))]
    return {"coil_ids": hold_ids,
            "X": modeling.clean_features(hold_df[contract.FEATURES].to_numpy(dtype="float64")),
            "Y": contract.labels_matrix(hold_df),
            "features": contract.FEATURES, "labels": contract.LABELS}


def get_holdout():
    """Return (holdout_dict, frozen).

    Carved from the stream and frozen to disk only once every family has >= MIN_HOLDOUT_PER_FAMILY
    positive coils. Before that it is re-carved each TRAIN (temporary, not comparable across versions)
    so an early train is still evaluated without locking in a degenerate 0%-recall holdout.
    """
    if os.path.exists(HOLDOUT_FILE):
        h = modeling.load_artifact(HOLDOUT_FILE)
        if h.get("features") == contract.FEATURES and h.get("labels") == contract.LABELS:
            return h, True
        log.warning("holdout.pkl built with a different contract — re-carving from the stream")

    df = _load_all_streamed()
    coils_n = int(df.coil_id.nunique()) if not df.empty else 0
    if coils_n < 200:
        raise RuntimeError(f"not enough streamed data yet to carve a held-out set "
                           f"({coils_n} coils) — let the stream run a little, then Train")
    fam_coils = {k: int(df[df[k]].coil_id.nunique()) for k in contract.LABEL_KEYS}
    h = _carve_holdout(df)
    if all(v >= MIN_HOLDOUT_PER_FAMILY for v in fam_coils.values()):
        modeling.atomic_pickle(HOLDOUT_FILE, h)
        log.info(f"froze representative held-out set: {len(h['coil_ids'])} coils; "
                 f"family coils = {fam_coils} -> {HOLDOUT_FILE}")
        return h, True
    log.info(f"family coverage still thin {fam_coils} (need >= {MIN_HOLDOUT_PER_FAMILY} each); "
             f"using a TEMPORARY held-out set for this train, not frozen yet")
    return h, False


def next_version() -> int:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT COALESCE(max(version), 0) FROM model_versions")
            return cur.fetchone()[0] + 1
    finally:
        conn.close()


def write_model_version(version, n_train, threshold, metrics, trained_on):
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO model_versions
                  (version, trained_at, n_train_samples, threshold, overall_f1, overall_pr_auc,
                   "precision", recall, recall_electric, recall_bearing, recall_workroll,
                   recall_reduction, trained_on)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (version) DO UPDATE SET
                  trained_at=EXCLUDED.trained_at, n_train_samples=EXCLUDED.n_train_samples,
                  threshold=EXCLUDED.threshold, overall_f1=EXCLUDED.overall_f1,
                  overall_pr_auc=EXCLUDED.overall_pr_auc, "precision"=EXCLUDED."precision",
                  recall=EXCLUDED.recall, recall_electric=EXCLUDED.recall_electric,
                  recall_bearing=EXCLUDED.recall_bearing, recall_workroll=EXCLUDED.recall_workroll,
                  recall_reduction=EXCLUDED.recall_reduction, trained_on=EXCLUDED.trained_on
                """,
                (version, datetime.now(timezone.utc).replace(tzinfo=None), n_train, threshold,
                 metrics["f1"], metrics["pr_auc"], metrics["precision"], metrics["recall"],
                 metrics["recall_electric"], metrics["recall_bearing"],
                 metrics["recall_workroll"], metrics["recall_reduction"], trained_on),
            )
    finally:
        conn.close()


def run_retrain() -> dict:
    contract.assert_contract()
    holdout, holdout_frozen = get_holdout()
    holdout_ids = set(holdout["coil_ids"])
    X_hold, Y_hold = holdout["X"], holdout["Y"]    # already cleaned at carve time

    df = load_streamed(holdout_ids)
    if df.empty or len(df) < 1000:
        raise RuntimeError(f"not enough streamed data yet ({len(df)} rows) — start the producer")

    coils = df.coil_id.unique()
    tr_c, val_c = train_test_split(coils, test_size=0.15, random_state=7)
    tr = df[df.coil_id.isin(set(tr_c))]
    val = df[df.coil_id.isin(set(val_c))]

    X_tr = modeling.clean_features(tr[contract.FEATURES].to_numpy(dtype="float64"))
    Y_tr = contract.labels_matrix(tr)
    fam_pos = {fam: int(Y_tr[:, j].sum()) for j, fam in enumerate(contract.LABELS)}
    log.info(f"retrain on {len(coils)} streamed coils / {len(X_tr)} train events; family positives = {fam_pos}")

    # hold the advisory lock across allocate-train-publish so a concurrent API+CLI run can't
    # mint the same version or race the artifact files.
    lock_conn = _connect()
    try:
        with lock_conn.cursor() as cur:
            cur.execute("SELECT pg_advisory_lock(%s)", (modeling.PUBLISH_LOCK_KEY,))

        t0 = time.time()
        models = modeling.train_models(X_tr, Y_tr)
        version = next_version()
        artifact = {
            "models": models, "features": contract.FEATURES, "labels": contract.LABELS,
            "version": version, "trained_at": datetime.now(timezone.utc).isoformat(),
            "trained_on": f"streamed {len(coils)} coils",
        }

        X_val = modeling.clean_features(val[contract.FEATURES].to_numpy(dtype="float64"))
        Y_val = contract.labels_matrix(val)
        scores_val = modeling.anomaly_scores(modeling.predict_proba_matrix(artifact, X_val))
        threshold = modeling.tune_threshold(Y_val, scores_val)   # macro-balanced across families
        artifact["threshold"] = threshold

        metrics = modeling.evaluate(artifact, X_hold, Y_hold, threshold)
        artifact["metrics"] = metrics
        log.info(f"v{version} trained in {time.time()-t0:.1f}s; threshold {threshold:.2f}; held-out {metrics}")

        # DB row first, artifact last: model.pkl mtime is the hot-swap trigger, so the
        # model_versions row must exist before the UDF reloads.
        write_model_version(version, len(X_tr), threshold, metrics, artifact["trained_on"])
        modeling.atomic_write_artifact(MODEL_DIR, artifact)
        log.info(f"hot-swapped model -> v{version}")
    finally:
        lock_conn.close()                     # close releases the advisory lock

    return {"version": version, "threshold": round(threshold, 3), "n_train": len(X_tr),
            "n_coils": int(len(coils)), "family_positives": fam_pos, "metrics": metrics}


if __name__ == "__main__":
    import json
    print(json.dumps(run_retrain(), indent=2))
