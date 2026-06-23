"""Data contract shared by producer, processor UDF and retrain sidecar.

The only place feature/label names and order are defined; a reordered FEATURES list
silently produces garbage scores. assert_contract() guards it. Each component COPYs
this file into its image. One CSV row is one coil through a 5-stand tandem cold mill;
build_per_stand_frame unfolds it into 5 per-stand events.
"""

from __future__ import annotations

import glob
import os

import numpy as np
import pandas as pd

TOPIC = "tcm5_readings"

# FEATURES: 16, fixed order — model trains and scores on exactly these columns in
# this order. 8 per-stand measured, 5 global set-points, 2 tensions, 1 stand index.
FEATURES = [
    "work_roll_diam", "work_roll_mileage", "reduction", "roll_speed",
    "force", "torque", "gap", "motor_power",
    "thickness_entry", "thickness_exit", "width", "ys_entry", "ys_exit",
    "tension_in", "tension_out",
    "stand",
]

# Fault families: 4, fixed order. One predicted probability per family.
LABELS = ["electric", "bearing", "workroll", "reduction"]

# y_ prefix avoids the collision between the feature `reduction` and the label `reduction`.
LABEL_KEYS = [f"y_{fam}" for fam in LABELS]  # y_electric, y_bearing, y_workroll, y_reduction

# label -> per-stand CSV column ({s} formatted); reduction is one global column.
_FAMILY_CSV = {
    "electric": "Anomaly_Electric_{s}",
    "bearing":  "Anomaly_Bearing_{s}",
    "workroll": "Anomaly_WorkRoll_{s}",
    "reduction": "Anomaly_Reduction",          # applies to every stand of the coil
}

COIL_ID_STRIDE = 1_000_000                       # coil_id = file_no * STRIDE + row_index_within_file


def coil_id_array(file_no: int, n_rows: int) -> np.ndarray:
    """Deterministic coil ids for a file; identical offline and in the stream."""
    return file_no * COIL_ID_STRIDE + np.arange(n_rows, dtype=np.int64)


def source_csv_files(data_dir: str) -> list[str]:
    """Raw per-coil CSVs (tcm5_dataset_*.csv) in numeric file order."""
    return sorted(glob.glob(os.path.join(data_dir, "tcm5_dataset_*.csv")),
                  key=lambda p: int(p.rsplit("_", 1)[1].split(".")[0]))


def build_merged_frame(data_dir: str, seed: int = 42) -> pd.DataFrame:
    """Concatenate the raw CSVs and shuffle once into the single source.

    Row shuffle interleaves every fault family across the run (one row = one coil).
    coil_id and file_no are prepended so provenance survives the shuffle. Producer
    builds data/tcm5_merged.parquet from this on demand; byte-identical for a seed.
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

    Columns: coil_id, file_no, FEATURES (incl. stand), LABEL_KEYS. One row per
    (coil, stand). A merged/shuffled CSV carries its own coil_id/file_no columns;
    when present they are used verbatim so ids survive the shuffle. Otherwise the
    scalar file_no is required and coil ids are derived deterministically.
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
            # per-stand measured signals for this stand
            "work_roll_diam":    df[f"work_roll_diam_{s}"].to_numpy(),
            "work_roll_mileage": df[f"work_roll_mileage_{s}"].to_numpy(),
            "reduction":         df[f"reduction_{s}"].to_numpy(),
            "roll_speed":        df[f"roll_speed_{s}"].to_numpy(),
            "force":             df[f"force_{s}"].to_numpy(),
            "torque":            df[f"torque_{s}"].to_numpy(),
            "gap":               df[f"gap_{s}"].to_numpy(),
            "motor_power":       df[f"motor_power_{s}"].to_numpy(),
            # global set-points, same for every stand of the coil
            "thickness_entry":   df["thickness_entry"].to_numpy(),
            "thickness_exit":    df["thickness_exit"].to_numpy(),
            "width":             df["width"].to_numpy(),
            "ys_entry":          df["ys_entry"].to_numpy(),
            "ys_exit":           df["ys_exit"].to_numpy(),
            # tensions bracketing this stand: position s-1 in, s out
            "tension_in":        df[f"tension_{s - 1}"].to_numpy(),
            "tension_out":       df[f"tension_{s}"].to_numpy(),
            "stand":             np.full(n, s, dtype=np.int64),
        }
        for fam in LABELS:
            rec[f"y_{fam}"] = _as_bool(df[_FAMILY_CSV[fam].format(s=s)])
        parts.append(pd.DataFrame(rec))
    out = pd.concat(parts, ignore_index=True)
    return out


def _as_bool(series: pd.Series) -> np.ndarray:
    """CSV labels arrive as bool or as strings ('True'/'False'); normalize to bool."""
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
