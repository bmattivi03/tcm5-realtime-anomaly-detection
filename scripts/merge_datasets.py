#!/usr/bin/env python
"""
Merge the 6 per-coil CSVs into ONE randomly-shuffled CSV for streaming.

This is an offline preprocessing step (NOT part of the stream): it concatenates
data/tcm5_dataset_1..6.csv and shuffles the rows. One CSV row = one coil, so a
plain row shuffle interleaves the fault families across the whole run instead of
the default "files 1-2 reduction-only first, then 3-6" order.

Each row keeps its origin via two prepended columns:
  - coil_id : file_no * COIL_ID_STRIDE + row_index  (globally unique, survives shuffle)
  - file_no : the source file number (1..6)
The producer reads these verbatim (see --data-file), so the shuffled order is
streamed as-is and provenance/labels stay correct.

  python scripts/merge_datasets.py                 # -> data/tcm5_merged.parquet (seed 42)
  python scripts/merge_datasets.py --out data/tcm5_merged.csv   # CSV instead of Parquet
  python scripts/merge_datasets.py --seed 7

The producer reads data/tcm5_merged.{parquet,csv} as its single source and rebuilds it
from the raw CSVs (same shared merge, same seed) if it is missing, so running this
script by hand is optional.
"""

import argparse
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "shared"))
import contract  # noqa: E402


def main():
    ap = argparse.ArgumentParser(description="Merge + shuffle the TCM-5 coil CSVs")
    ap.add_argument("--data-dir", default="data")
    ap.add_argument("--out", default="data/tcm5_merged.parquet",
                    help="output path; .parquet (default, smaller + typed) or .csv")
    ap.add_argument("--seed", type=int, default=42, help="shuffle seed (reproducible demos)")
    args = ap.parse_args()

    # the merge itself lives in the shared contract so the producer can build the
    # exact same source on demand when the file is absent
    merged = contract.build_merged_frame(args.data_dir, seed=args.seed)
    os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
    if args.out.endswith(".parquet"):
        merged.to_parquet(args.out, index=False)
    else:
        merged.to_csv(args.out, index=False)

    mix = merged["file_no"].value_counts().sort_index().to_dict()
    print(f"merged {len(merged)} coils -> {args.out} (seed={args.seed})")
    print(f"  coils per source file: {mix}")


if __name__ == "__main__":
    main()
