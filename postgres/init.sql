-- TCM-5 anomaly-detection sink schema.
-- snake_case column names (Flink JDBC sink maps by name).
-- TIMESTAMP not TIMESTAMPTZ: Flink session runs UTC, keep it that way or Grafana
-- "last N minutes" filters drift.

-- ---------------------------------------------------------------------------
-- One row per scored per-stand reading (append-only).
-- reading_id is Postgres-generated; the Flink sink never inserts it, so readers
-- dedup at-least-once replay with DISTINCT ON (coil_id, stand) ORDER BY reading_id DESC.
--
-- RANGE-partitioned on ts (native declarative partitioning, no extension): daily
-- partitions let $__timeFilter(ts) panels prune and let retention DROP a whole day
-- instead of a bulk DELETE. Partition key must be in the PK, hence PK (reading_id, ts);
-- the identity sequence still makes reading_id globally unique.
-- ---------------------------------------------------------------------------
CREATE TABLE scored_events (
  reading_id        BIGINT GENERATED ALWAYS AS IDENTITY,
  ts                TIMESTAMP(3) NOT NULL,
  coil_id           BIGINT,
  stand             INT,
  file_no           INT,
  -- all 16 model features logged so the retrain sidecar can train off the stream directly
  work_roll_diam    DOUBLE PRECISION,
  work_roll_mileage DOUBLE PRECISION,
  reduction         DOUBLE PRECISION,
  roll_speed        DOUBLE PRECISION,
  force             DOUBLE PRECISION,
  torque            DOUBLE PRECISION,
  gap               DOUBLE PRECISION,
  motor_power       DOUBLE PRECISION,
  thickness_entry   DOUBLE PRECISION,
  thickness_exit    DOUBLE PRECISION,
  width             DOUBLE PRECISION,
  ys_entry          DOUBLE PRECISION,
  ys_exit           DOUBLE PRECISION,
  tension_in        DOUBLE PRECISION,
  tension_out       DOUBLE PRECISION,
  -- model outputs
  anomaly_score     DOUBLE PRECISION,
  p_electric        DOUBLE PRECISION,
  p_bearing         DOUBLE PRECISION,
  p_workroll        DOUBLE PRECISION,
  p_reduction       DOUBLE PRECISION,
  -- ground-truth labels from the producer; the model never overwrites them
  y_electric        BOOLEAN,
  y_bearing         BOOLEAN,
  y_workroll        BOOLEAN,
  y_reduction       BOOLEAN,
  y_any             BOOLEAN,
  model_version     INT,
  -- model-free detectors (Mahalanobis / SPC / kNN) + 2-of-3 vote, computed in Flink;
  -- available before any model exists
  u_maha            DOUBLE PRECISION,
  u_spc             DOUBLE PRECISION,
  u_knn             DOUBLE PRECISION,
  u_votes           INT,
  u_anomaly         BOOLEAN,
  -- stamped by Postgres on insert (Flink sink never lists it, like reading_id).
  -- ingested_at - ts = end-to-end pipeline latency, both UTC.
  ingested_at       TIMESTAMP(3) DEFAULT (now() AT TIME ZONE 'utc'),
  PRIMARY KEY (reading_id, ts)
) PARTITION BY RANGE (ts);

-- Daily partitions around bootstrap time + a DEFAULT partition so an out-of-window
-- ts never fails an insert.
DO $$
DECLARE d date;
BEGIN
  FOR d IN SELECT generate_series(CURRENT_DATE - 1, CURRENT_DATE + 14, interval '1 day')::date
  LOOP
    EXECUTE format(
      'CREATE TABLE IF NOT EXISTS scored_events_%s PARTITION OF scored_events '
      'FOR VALUES FROM (%L) TO (%L)', to_char(d, 'YYYYMMDD'), d, d + 1);
  END LOOP;
END $$;
CREATE TABLE IF NOT EXISTS scored_events_default PARTITION OF scored_events DEFAULT;

-- parent indexes propagate to every existing and future partition.
CREATE INDEX idx_scored_events_ts         ON scored_events (ts);
-- serves the DISTINCT ON (coil_id, stand) ORDER BY reading_id DESC dedup without a sort.
CREATE INDEX idx_scored_events_dedup      ON scored_events (coil_id, stand, reading_id DESC);
CREATE INDEX idx_scored_events_version    ON scored_events (model_version);

-- ---------------------------------------------------------------------------
-- Event-time tumbling-window KPIs (1-minute). Upserted by Flink.
-- ---------------------------------------------------------------------------
CREATE TABLE kpi_windows (
  window_start  TIMESTAMP(3),
  window_end    TIMESTAMP(3),
  coils         BIGINT,
  events        BIGINT,
  anomalies     BIGINT,
  anomaly_rate  DOUBLE PRECISION,
  threshold     DOUBLE PRECISION,   -- score threshold used for the anomalies count
  PRIMARY KEY (window_start, window_end)
);

-- ---------------------------------------------------------------------------
-- Retrain history, one row per model version (written by the retrain sidecar).
-- ---------------------------------------------------------------------------
CREATE TABLE model_versions (
  version          INT PRIMARY KEY,
  trained_at       TIMESTAMP(3),
  n_train_samples  INT,
  threshold        DOUBLE PRECISION,
  overall_f1       DOUBLE PRECISION,
  overall_pr_auc   DOUBLE PRECISION,
  "precision"      DOUBLE PRECISION,
  recall           DOUBLE PRECISION,
  recall_electric  DOUBLE PRECISION,
  recall_bearing   DOUBLE PRECISION,
  recall_workroll  DOUBLE PRECISION,
  recall_reduction DOUBLE PRECISION,
  trained_on       VARCHAR
);
