"""
THE data contract — single source of truth shared by the producer, the processor
UDF and the retrain sidecar.

A reordered feature list = silently garbage scores, so
this module is the *only* place the feature/label names and order are defined, and
``assert_contract()`` guards them. Each component COPYs this file into its image.

One CSV row = one coil through a 5-stand tandem cold mill. We unfold it into 5
per-stand events (stand 1..5). ``build_per_stand_frame`` is the one vectorized
definition of that unfold, used by the producer to build the stream.
"""

from __future__ import annotations

import glob
import os

import numpy as np
import pandas as pd

# ---------------------------------------------------------------------------
# Kafka topic
# ---------------------------------------------------------------------------
TOPIC = "tcm5_readings"

# ---------------------------------------------------------------------------
# Model FEATURES — 16, FIXED ORDER. These are the exact columns the model is
# trained on and scored on, in this order.
#   8 per-stand measured · 5 global set-points · 2 tensions · 1 stand index
# ---------------------------------------------------------------------------
FEATURES = [
    "work_roll_diam", "work_roll_mileage", "reduction", "roll_speed",
    "force", "torque", "gap", "motor_power",
    "thickness_entry", "thickness_exit", "width", "ys_entry", "ys_exit",
    "tension_in", "tension_out",
    "stand",
]

# Fault families — 4, FIXED ORDER. The model predicts one probability per family.
LABELS = ["electric", "bearing", "workroll", "reduction"]

# JSON/DB key for each family's ground-truth boolean (y_ prefix avoids the
# collision between the feature `reduction` and the label `reduction`).
LABEL_KEYS = [f"y_{fam}" for fam in LABELS]  # y_electric, y_bearing, y_workroll, y_reduction

# Map each family to its per-stand CSV column prefix (reduction is global).
_FAMILY_CSV = {
    "electric": "Anomaly_Electric_{s}",
    "bearing":  "Anomaly_Bearing_{s}",
    "workroll": "Anomaly_WorkRoll_{s}",
    "reduction": "Anomaly_Reduction",          # one column, applies to every stand of the coil
}

COIL_ID_STRIDE = 1_000_000                       # coil_id = file_no * STRIDE + row_index_within_file


def coil_id_array(file_no: int, n_rows: int) -> np.ndarray:
    """Deterministic coil ids for a file, identical offline and in the stream."""
    return file_no * COIL_ID_STRIDE + np.arange(n_rows, dtype=np.int64)


def source_csv_files(data_dir: str) -> list[str]:
    """The raw per-coil CSVs (tcm5_dataset_*.csv) in numeric file order."""
    return sorted(glob.glob(os.path.join(data_dir, "tcm5_dataset_*.csv")),
                  key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0]))


def build_merged_frame(data_dir: str, seed: int = 42) -> pd.DataFrame:
    """Concatenate the six raw CSVs and shuffle them once into the single source.

    One CSV row is one coil, so a plain row shuffle interleaves every fault family
    across the whole run. Two columns are prepended so provenance survives the
    shuffle: a globally unique ``coil_id`` and the source ``file_no``. This is the
    ONE definition of the merge, used both by scripts/merge_datasets.py (to write
    data/tcm5_merged.parquet offline) and by the producer (to build it on demand
    when the file is absent), so both produce a byte-identical source for a given seed.
    """
    files = source_csv_files(data_dir)
    if not files:
        raise FileNotFoundError(f"No tcm5_dataset_*.csv found in {data_dir}")
    parts = []
    for path in files:
        file_no = int(path.rsplit("_", 1)[1].split(".")[0])
        df = pd.read_csv(path)
        df.insert(0, "coil_id", coil_id_array(file_no, len(df)))
        df.insert(1, "file_no", file_no)
        parts.append(df)
    merged = pd.concat(parts, ignore_index=True)
    return merged.sample(frac=1.0, random_state=seed).reset_index(drop=True)


def build_per_stand_frame(df: pd.DataFrame, file_no: int | None = None) -> pd.DataFrame:
    """Unfold a wide per-coil CSV frame into a long per-stand frame.

    Returns columns: coil_id, file_no, then every name in FEATURES (incl. `stand`),
    then every name in LABEL_KEYS. One row per (coil, stand).

    ``file_no`` is the source file number, used for coil ids and kept as provenance.
    A merged/shuffled CSV (see scripts/merge_datasets.py) carries its
    own per-row ``coil_id`` and ``file_no`` columns; when present they are used
    verbatim so provenance and ids survive the shuffle. Otherwise the scalar
    ``file_no`` is required and coil ids are derived deterministically.
    """
    n = len(df)
    if "coil_id" in df.columns:
        cid = df["coil_id"].to_numpy().astype(np.int64)
    else:
        if file_no is None:
            raise ValueError("file_no is required when df has no coil_id column")
        cid = coil_id_array(file_no, n)
    if "file_no" in df.columns:
        fno = df["file_no"].to_numpy().astype(np.int64)
    else:
        fno = np.full(n, int(file_no), dtype=np.int64)
    parts = []
    for s in range(1, 6):
        rec = {
            "coil_id": cid,
            "file_no": fno,
            # 8 per-stand measured signals for THIS stand
            "work_roll_diam":    df[f"work_roll_diam_{s}"].to_numpy(),
            "work_roll_mileage": df[f"work_roll_mileage_{s}"].to_numpy(),
            "reduction":         df[f"reduction_{s}"].to_numpy(),
            "roll_speed":        df[f"roll_speed_{s}"].to_numpy(),
            "force":             df[f"force_{s}"].to_numpy(),
            "torque":            df[f"torque_{s}"].to_numpy(),
            "gap":               df[f"gap_{s}"].to_numpy(),
            "motor_power":       df[f"motor_power_{s}"].to_numpy(),
            # 5 global set-points (same for all stands of the coil)
            "thickness_entry":   df["thickness_entry"].to_numpy(),
            "thickness_exit":    df["thickness_exit"].to_numpy(),
            "width":             df["width"].to_numpy(),
            "ys_entry":          df["ys_entry"].to_numpy(),
            "ys_exit":           df["ys_exit"].to_numpy(),
            # 2 tensions bracketing this stand (position s-1 in, s out)
            "tension_in":        df[f"tension_{s - 1}"].to_numpy(),
            "tension_out":       df[f"tension_{s}"].to_numpy(),
            # stand index (feature #16)
            "stand":             np.full(n, s, dtype=np.int64),
        }
        # ground-truth labels for this stand, derived from _FAMILY_CSV (the single
        # label -> CSV-column mapping; reduction's column is global, no {s})
        for fam in LABELS:
            rec[f"y_{fam}"] = _as_bool(df[_FAMILY_CSV[fam].format(s=s)])
        parts.append(pd.DataFrame(rec))
    out = pd.concat(parts, ignore_index=True)
    return out


def _as_bool(series: pd.Series) -> np.ndarray:
    """CSV labels arrive as bool or as the strings 'True'/'False'. Normalize to bool."""
    if series.dtype == bool:
        return series.to_numpy()
    return series.astype(str).str.strip().str.lower().isin(["true", "1", "t", "yes"]).to_numpy()


def labels_matrix(frame: pd.DataFrame) -> np.ndarray:
    """(n, 4) bool matrix of ground-truth labels in LABELS order."""
    return np.column_stack([frame[k].to_numpy().astype(bool) for k in LABEL_KEYS])


def assert_contract() -> None:
    """Fail fast if the contract is internally inconsistent."""
    assert len(FEATURES) == 16, f"expected 16 features, got {len(FEATURES)}"
    assert len(set(FEATURES)) == 16, "duplicate feature names"
    assert len(LABELS) == 4, f"expected 4 labels, got {len(LABELS)}"
    assert LABELS == ["electric", "bearing", "workroll", "reduction"], "label order changed"
    assert LABEL_KEYS == ["y_electric", "y_bearing", "y_workroll", "y_reduction"]
    assert "reduction" in FEATURES and "stand" in FEATURES


if __name__ == "__main__":
    assert_contract()
    print("contract OK")
    print(f"TOPIC      = {TOPIC}")
    print(f"FEATURES({len(FEATURES)}) = {FEATURES}")
    print(f"LABELS({len(LABELS)})    = {LABELS}")
    print(f"LABEL_KEYS    = {LABEL_KEYS}")
