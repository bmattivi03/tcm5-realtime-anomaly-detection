#!/usr/bin/env python
"""
TCM-5 producer — replays the coil CSVs as a per-stand Kafka stream.

Each coil (one CSV row) is unfolded into its 5 per-stand events (stand 1..5) using
the shared contract, keyed by coil_id, with `ts_ms = now()` stamped at emission so
Grafana "last N minutes" filters stay populated. The single input is the merged +
shuffled source (data/tcm5_merged.parquet); if it is missing the producer builds it
from the six raw CSVs with the shared merge, so every fault family is interleaved
from the first minute.

Pacing: --speedup is the approximate number of *coils* emitted per second (each coil
= 5 events). A self-correcting rate limiter keeps the wall-clock spread realistic so
the event-time windows fire repeatedly.
"""

import os
import sys
import time
import logging
from argparse import ArgumentParser
from json import dumps

import numpy as np
import pandas as pd
from confluent_kafka import Producer

import contract
from common import setup_topic, produce_or_block


def _np_default(o):
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return float(o)
    if isinstance(o, (np.bool_, bool)):
        return bool(o)
    raise TypeError(f"not JSON serializable: {type(o)}")


def main():
    parser = ArgumentParser(description="Replay TCM-5 coils as a per-stand Kafka stream")
    parser.add_argument("--bootstrap-servers", default="localhost:39092", type=str)
    parser.add_argument("--topic", default=contract.TOPIC, type=str)
    parser.add_argument("--data-dir", default="data", type=str, help="directory with tcm5_dataset_*.csv")
    parser.add_argument("--data-file", default="", type=str,
                        help="stream ONE merged CSV (scripts/merge_datasets.py output) in its "
                             "shuffled row order instead of the numeric file sequence")
    parser.add_argument("--speedup", default=30.0, type=float, help="approx coils per second")
    parser.add_argument("--max-coils", default=0, type=int, help="stop after N coils (0 = all)")
    parser.add_argument("--loop", action="store_true",
                        help="replay the dataset continuously (each pass re-stamps ts=now, "
                             "so after a retrain the stream re-scores with the new model)")
    parser.add_argument("--num-partitions", default=1, type=int)
    parser.add_argument("--replication-factor", default=1, type=int)
    parser.add_argument("--recreate-topic", action="store_true",
                        help="delete + recreate the topic if it exists with a different "
                             "partition/replication layout (destroys streamed data)")
    parser.add_argument("--dry-run", action="store_true", help="print events instead of producing")
    parser.add_argument("--log-level", default="info", type=str)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.getLevelName(args.log_level.upper()),
        format="%(asctime)s (%(levelname).1s) %(message)s",
        stream=sys.stdout if sys.stdout.isatty() else sys.stderr)

    contract.assert_contract()

    # The producer's single input is the merged + shuffled source: one file that
    # interleaves every fault family from the first minute. It auto-detects the
    # merged file in data-dir; if it is absent the producer BUILDS it from the six
    # raw CSVs with the shared merge (seed 42) and writes it back, so the run is
    # reproducible and the file is reused next time. It never streams the raw files
    # in their reduction-first order.
    preloaded = {}                                   # path -> in-memory frame (read-only data dir)
    data_file = args.data_file
    if not data_file:
        parquet = os.path.join(args.data_dir, "tcm5_merged.parquet")
        csv = os.path.join(args.data_dir, "tcm5_merged.csv")
        if os.path.exists(parquet):
            data_file = parquet
        elif os.path.exists(csv):
            data_file = csv
        else:
            logging.info(f"No merged file in {args.data_dir}; building it from the "
                         f"six raw CSVs (shared merge, seed 42)")
            merged_df = contract.build_merged_frame(args.data_dir)
            data_file = parquet
            try:
                merged_df.to_parquet(parquet, index=False)
                logging.info(f"Wrote {parquet}: {len(merged_df)} coils")
            except OSError as e:                      # e.g. data dir mounted read-only
                logging.warning(f"Could not write {parquet} ({e}); streaming the "
                                f"merged data from memory")
                preloaded[parquet] = merged_df
    files = [data_file]
    logging.info(f"Streaming ONE merged source in shuffled order: {data_file}")

    producer = None
    delivery = {"ok": 0, "failed": 0}

    def on_delivery(err, msg):
        if err is not None:
            delivery["failed"] += 1
            if delivery["failed"] <= 5:
                logging.error(f"delivery failed: {err} (key={msg.key()})")
        else:
            delivery["ok"] += 1

    if not args.dry_run:
        setup_topic(args.topic, bootstrap_servers=args.bootstrap_servers,
                    num_partitions=args.num_partitions, replication_factor=args.replication_factor,
                    recreate_on_mismatch=args.recreate_topic)
        producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "compression.type": "gzip",
            "acks": "1",
            "linger.ms": "50",
            "queue.buffering.max.messages": "200000",
            "on_delivery": on_delivery,
        })

    event_feature_keys = contract.FEATURES               # includes 'stand'
    label_keys = contract.LABEL_KEYS

    coils_emitted = 0
    events_emitted = 0
    start = time.time()
    stop = False

    logging.info(f"Replaying to topic '{args.topic}' @ ~{args.speedup} coils/s"
                 + (" (dry-run)" if args.dry_run else ""))

    pass_no = 0
    while not stop:
        pass_no += 1
        if args.loop and pass_no > 1:
            logging.info(f"--- replay pass {pass_no} (re-scores with whatever model is live now) ---")
        for path in files:
            if stop:
                break
            df = preloaded.get(path)
            if df is None:
                df = pd.read_parquet(path) if path.endswith(".parquet") else pd.read_csv(path)
            # the merged source carries its own coil_id/file_no; stream it in the file's
            # shuffled row order (NOT coil_id order, which would re-group by file).
            frame = contract.build_per_stand_frame(df)
            n_coils = len(df)
            frame["__ord"] = np.tile(np.arange(n_coils), 5)
            frame = (frame.sort_values(["__ord", "stand"])
                          .reset_index(drop=True).drop(columns="__ord"))

            # pull columns to numpy once for a fast, low-memory emit loop
            arrs = {c: frame[c].to_numpy() for c in
                    (["coil_id", "file_no"] + event_feature_keys + label_keys)}
            n = len(frame)
            logging.info(f"{path.rsplit('/', 1)[-1]}: {n // 5} coils ({n} events)")

            i = 0
            while i < n:
                ts_ms = int(time.time() * 1000)
                for j in range(5):                            # 5 stands of one coil (sorted)
                    idx = i + j
                    ev = {k: arrs[k][idx] for k in event_feature_keys}
                    ev["coil_id"] = arrs["coil_id"][idx]
                    ev["file_no"] = arrs["file_no"][idx]
                    ev["ts_ms"] = ts_ms
                    for k in label_keys:
                        ev[k] = arrs[k][idx]
                    payload = dumps(ev, default=_np_default)
                    if args.dry_run:
                        if events_emitted < 10:
                            print(payload)
                    else:
                        produce_or_block(producer, topic=args.topic,
                                         key=str(int(ev["coil_id"])).encode("utf-8"),
                                         value=payload.encode("utf-8"))
                    events_emitted += 1
                i += 5
                coils_emitted += 1

                if not args.dry_run:
                    producer.poll(0)

                if args.max_coils and coils_emitted >= args.max_coils:
                    stop = True
                    break

                # self-correcting rate limiter: keep coils_emitted ≈ speedup * elapsed
                if args.speedup > 0:
                    expected = coils_emitted / args.speedup
                    behind = expected - (time.time() - start)
                    if behind > 0.002:
                        time.sleep(behind)

                if coils_emitted % 5000 == 0:
                    logging.info(f"... {coils_emitted} coils / {events_emitted} events "
                                 f"({coils_emitted / max(time.time() - start, 1e-9):.0f} coils/s)")
        if not args.loop:
            break

    if producer is not None:
        remaining = producer.flush(60.0)
        while remaining > 0:
            logging.warning(f"flush: {remaining} messages still in queue, retrying...")
            prev, remaining = remaining, producer.flush(60.0)
            if remaining >= prev:                       # no progress: broker is gone
                break
        if remaining > 0 or delivery["failed"] > 0:
            logging.error(f"DELIVERY INCOMPLETE: {delivery['failed']} failed, "
                          f"{remaining} undelivered of {events_emitted} produced")
            raise SystemExit(1)
    elapsed = time.time() - start
    logging.info(f"Done: {coils_emitted} coils, {events_emitted} events in {elapsed:.1f}s "
                 f"({coils_emitted / max(elapsed, 1e-9):.0f} coils/s, "
                 f"{delivery['ok']} deliveries confirmed)")


if __name__ == "__main__":
    main()
