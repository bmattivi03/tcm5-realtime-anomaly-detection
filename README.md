# TCM-5 — Real-Time Anomaly Detection on a Tandem Cold-Rolling Mill

An end-to-end **real-time data pipeline**. Historical coil data from a 5-stand tandem cold
mill is **replayed as a live per-stand Kafka stream**, scored in-stream by a machine-learning model
running inside **PyFlink**, persisted to **PostgreSQL**, and visualised on a single **Grafana**
dashboard. A sidecar **watches the live stream for model drift, retrains automatically and hot-swaps
the model into the running job with no restart**, and a custom **control-room web UI**
(FastAPI + server-sent events + RxJS) shows the mill running live.

```
 producer ──JSON──► Kafka ──► PyFlink (Table API) ──► PostgreSQL ──► Grafana (one dashboard)
 (unfold each       tcm5_     • pandas UDFs score each event ONCE         │       ▲
  coil into 5       readings    (shared sub-plan across both sinks)       ├──► control room
  per-stand events)           • event-time tumbling-window KPIs (1 min)   │    (FastAPI + SSE,
                              • checkpoints every 10 s → auto-restart      │     animated mill)
 retrain sidecar (FastAPI) ── TRAIN button / drift policy ◄──────────────────┘
   first train builds v1 from the streamed data (cold start, no pre-trained model); each retrain
   trains on more of the stream, evaluates on a fixed held-out set, atomic hot-swap, model_versions row
```

---

## Quickstart

Requires Docker and Docker Compose. From this folder:

```bash
# 1 · start the WHOLE pipeline — cold start, no model. (.env enables the processor + producer
#     profiles, so this one command also submits the Flink job and begins the ~67 min stream.)
docker compose up -d --build

# 2 · open the control room and click TRAIN whenever you like — it trains v1 on the coils
#     streamed so far (read back from Postgres) and hot-swaps it into the live Flink job.
#     Click again any time for v2, v3…; auto-retrain then keeps watching drift on its own.

# stop it:            docker compose down       (keeps the data + trained model in volumes)
# stop AND erase it:  docker compose down -v    (wipes data, topic, model -> true cold start next up)
```

| service | URL | notes |
|---|---|---|
| Control room | http://localhost:38500 | live animated mill, alarm ticker, hot-swap choreography, TRAIN button |
| Grafana | http://localhost:33000 | login `user` / `user`; the TCM-5 dashboard is the home page |
| Flink UI | http://localhost:38081 | running job, checkpoints, watermarks, backpressure |
| Kafka UI | http://localhost:38080 | the `tcm5_readings` topic and consumer-group lag |
| Retrain | http://localhost:38000 | `Retrain now`, `POST /retrain`, `GET /versions`, `GET /policy` |
| Postgres | `localhost:35432` | db `db`, `user` / `user` |

Ports are set in `.env` and chosen to avoid the usual defaults so this stack coexists with others.

There is no offline bootstrap: the stream starts with **no model** (anomaly columns NULL/0, the
control-room ticker and "coils to inspect" stay empty), and the first **TRAIN** click builds v1 from
whatever has been streamed so far. Because the input is fully shuffled, every fault family is present
from the start, so even an early train sees them all. After v1 exists, the sidecar's drift policy
(auto-retrain, on by default) also fires retrains by itself; the button stays available for manual runs.

To replay the stream continuously instead of once:

```bash
PRODUCER_EXTRA=--loop docker compose up -d producer
```

After a code change, rebuild and restart with `docker compose up -d --build`. For a processor/UDF change
specifically, use `./scripts/processor-submit.sh` so the old Flink job is cancelled before the new one
is submitted (a plain `up --build` would leave both running and double-count).

---

## Walkthrough

1. **Cold start — no model.** The stream begins with no model at all: Flink persists every event
   *unscored* (anomaly columns 0 / NULL), so the control-room ticker and "coils to inspect" stay empty.
   This mirrors a real line on day one: you have data, but nothing trained yet.
2. **Train on demand, from the stream.** Click **TRAIN** in the control room. The sidecar pulls the
   coils streamed so far from Postgres, carves a fixed held-out set on the first train, fits the
   per-family model, and atomically hot-swaps it into the live Flink job — same job ID, no restart.
   A purple **hot-swap annotation** appears, and the ticker and "coils to inspect" come alive with
   predicted faults.
3. **It keeps learning.** Click again any time for v2, v3… (each trained on more accumulated data); the
   **retrain flywheel** grows a bar and the held-out metrics step up. Auto-retrain (on by default) also
   watches live per-family recall (`GET /policy` shows its reasoning) and fires retrains by itself once
   a model exists.
4. **It is measurably real-time.** The **End-to-end latency** panel charts p50/p95 of `ingested_at - ts`
   (producer emission → Kafka → Flink scoring → JDBC sink → Postgres): about 0.5 s p50 in steady state.
   `./scripts/demo_backpressure.sh surge` overloads the pipeline 10x — watch p95 climb, consumer lag
   build in Kafka UI and the Flink busy gauges light up, then `drain` and watch it recover.
5. **It survives failure.** Checkpointing runs every 10 s (exactly-once mode) with a fixed-delay restart
   strategy: `docker compose restart flink-taskmanager` mid-stream and the job auto-recovers from the
   last checkpoint and keeps counting — the readers' dedup conventions absorb the at-least-once replay.

---

## The dashboard (one board, three sections)

Open Grafana → the **TCM-5 Real-Time Anomaly Detection** dashboard. Controls: **Stand** (SPC / wear),
**Alarm threshold** (recompute precision/recall live), **Coil** (drill-down, set by the table). Purple
vertical annotations mark every model hot-swap.

- **MODELS** — anomaly-score distribution (latest model, faults vs normal), precision/recall/F1 gauges,
  recall per fault family, confusion matrix, the **retrain flywheel** (held-out metrics per version),
  **precision and recall over stream time** (the drift-and-recovery story on one axis), and the **live
  scoring version** stat (registered vs actually-scoring version — the hot-swap gap).
- **KPIs** — throughput (coils/min, event-time windows), anomaly rate over time, **end-to-end latency
  p50/p95**, alarm rate per stand, fault-family mix, **fault attribution** (predicted vs true family),
  first-flagged-stand histogram, and a "coils to inspect" table (click a coil to drill in).
- **PROCESS** — median force/torque/gap per stand, an **SPC** control chart on rolling force
  (mean ± 3σ), the inter-stand **tension profile**, and **roll-wear** (work-roll mileage saw-tooths
  while force tracks it).

Every panel shows *measured, varying* signals — set-points (thickness, width, yield stress) are never
charted as time-series.

## The control room (the live operational view)

http://localhost:38500 — a buildless FastAPI + SSE + RxJS single page (vendored RxJS, no build step):
an animated SVG schematic of the 5-stand mill whose strip speed tracks live events/s, per-stand alarm
lamps with dominant-fault labels and roll-wear rings, a live anomaly ticker with per-coil drill-down,
throughput/freshness/latency vitals, the hot-swap banner choreography, and a retrain button. One
background task polls Postgres once per second and fans the same snapshot to every connected browser
(server-sent events, O(1) DB load in the number of viewers). Grafana remains the single analytical
dashboard; this is the live operational complement.

---

## Components

| path | what it is |
|---|---|
| `shared/contract.py` | **the one data contract** — `FEATURES` (16, fixed order), `LABELS` (4), the per-stand unfold, the `coil_id` scheme. Imported by every component. |
| `shared/modeling.py` | shared train / predict / threshold / NaN-policy / atomic-write logic, plus the cross-process publish lock key (one source of truth). |
| `producer/` | replays the CSVs, unfolding each coil into 5 per-stand JSON events keyed by `coil_id`, `ts=now()` at emission, paced by `--speedup` (≈ coils/sec), `--loop` to replay continuously. Delivery callbacks and a verified flush mean message loss cannot pass silently. |
| `scripts/merge_datasets.py` | merges the 6 CSVs and randomly shuffles them into a single `data/tcm5_merged.parquet` (keeping `coil_id`/`file_no` per row). The producer auto-detects this file and rebuilds it from the 6 raw CSVs if it is missing, so running this by hand is optional. |
| `processor/` | the PyFlink job: Kafka source (event-time, earliest-offset, idle-timeout), vectorized pandas UDFs scoring each event ONCE (shared sub-plan via `table.optimizer.reuse-optimize-block-with-digest-enabled`), 10 s exactly-once checkpoints + fixed-delay restart, NULL-event guards, two sinks (`scored_events` + `kpi_windows`) in one StatementSet. Cold-start: submits with no model (events persist unscored until the first TRAIN); `REQUIRE_MODEL=1` restores block-until-model behaviour. |
| `retrain/` | FastAPI sidecar: the **drift policy** (live per-family recall watcher, `GET /policy`) plus manual `POST /retrain`; trains on streamed `scored_events` (held-out excluded), evaluates on the fixed held-out set, publishes under a Postgres advisory lock (DB row first, artifact last, atomic). |
| `controlroom/` | the live mill control-room web UI: FastAPI + SSE snapshot bus + buildless RxJS front-end (read-only on Postgres). |
| `postgres/init.sql` | sink schema: `scored_events` (features + scores + labels + `ingested_at` for true e2e latency), **RANGE-partitioned on `ts` by day** with a DEFAULT catch-all; `kpi_windows`, `model_versions`; dedup-supporting indexes. |
| `grafana/` | provisioned Postgres datasource + the single `tcm5.json` dashboard. |
| `scripts/` | `reset.sh` (clean slate), `processor-submit.sh` (safe re-submit: rebuilds images, cancels + waits), `demo_backpressure.sh` (surge/drain), `partitions.sh` (list / retention). |

**The model**: one `HistGradientBoostingClassifier` per fault family (class-balanced sample weights),
bundled in a plain dict; `anomaly_score = max` of the four family probabilities. Identical
numpy / pandas / scikit-learn versions are pinned in the Flink image and the retrain image so the
pickled model loads cleanly inside the UDF. A hot-reloaded artifact is validated against the
feature/label contract before it replaces the live one — a bad artifact is rejected loudly and the old
model keeps scoring.

---

## Operational notes

- **Re-submitting the processor:** removing the `processor` *container* does not cancel the Flink *job*.
  Use `./scripts/processor-submit.sh` — it rebuilds the images (the Flink image bakes
  `shared/contract.py`, `shared/modeling.py` and `model_udf.py` onto the worker import path, so code
  edits need a rebuild), cancels any running job and **waits for the cancellation to complete** before
  resubmitting.
- **Delivery semantics:** the per-event sink is append-only at-least-once; readers dedup with
  `DISTINCT ON (coil_id, stand) ... ORDER BY reading_id DESC` (index-assisted). The window sink upserts
  on its `(window_start, window_end)` key, so replays are idempotent. Checkpoints commit Kafka offsets
  every 10 s — consumer lag is visible in Kafka UI under group `tcm5-processor`.
- **Windows fire on live data:** the event-time windows fire as the watermark advances; a bounded
  backlog with no later data leaves the final window unfired until more events arrive (expected).
- **Hot-reload:** publishers write `model.meta` then `model.pkl` atomically (`os.replace`, unique temp
  names, advisory-locked); the UDF polls the mtime and reloads — the running job picks up the new model
  with no restart. All output columns of an Arrow batch always come from the same model version.
- **Auto-retrain tuning:** `RETRAIN_AUTO=0` disables the drift policy (button-only retrains); the floor,
  window, and minimum interval are env-tunable (see `retrain/app.py`).
- **Time-partitioned fact table:** `scored_events` is RANGE-partitioned on `ts` (one partition per day,
  plus a DEFAULT catch-all so an insert never fails). `./scripts/partitions.sh list` shows the
  partitions; `./scripts/partitions.sh maintain [keep_days]` extends the forward window and drops
  partitions past the horizon. Because `init.sql` only runs on a fresh data directory, picking up a
  schema change needs `docker compose down -v` followed by re-submitting the processor and producer.
- **Reset without a full restart** (keep the containers up): `./scripts/reset.sh --cold` cancels the job,
  truncates the tables and model registry, deletes the topic and clears the model volume; plain
  `./scripts/reset.sh` keeps the model for a re-stream with scoring live.

---

## Dataset

`data/*.csv` — 6 files, ~20 000 rows each, ~120 000 rows total. One row is one coil passing through a
5-stand tandem cold mill; the producer unfolds each coil into 5 per-stand events. 16 features in a fixed
order, 4 fault families (electric, bearing, work-roll, reduction), overall positive rate ≈ 5 %.
