#!/usr/bin/env python
"""PyFlink Table API processor: score the per-stand Kafka stream and compute event-time
KPI windows, writing both grains to PostgreSQL.

Source needs `ts` as a real time-attribute + idle-timeout or the watermark never advances
and windows never fire. One shared `scored` view + reuse-optimize-block makes the model run
once per event, not once per sink (check with StatementSet.explain()). Sinks are at-least-once on restart; readers dedup
scored_events and kpi_windows upserts on (window_start, window_end). Keep session tz = UTC
and Postgres TIMESTAMP or Grafana "last N min" filters drift.
"""

import os
import time

from pathlib import Path
from argparse import ArgumentParser

from pyflink.common import Configuration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

from kafka_utils import wait_for_topics
import contract
import model_udf
import unsupervised


def connector(args, table):
    if args.dry_run:
        return f"'connector' = 'print', 'print-identifier' = '{table}'"
    return (f"'connector' = 'jdbc', 'table-name' = '{table}', 'url' = '{args.db_url}', "
            f"'username' = '{args.db_username}', 'password' = '{args.db_password}'")


def wait_for_model(path, timeout_s=120, require=False):
    """Cold-start by default: submit with no model. UDF scores 0 / version 0 until the first
    model.pkl lands, but events still persist (raw features + labels) so the Train button has
    data to learn from. REQUIRE_MODEL=1 blocks until an artifact exists instead."""
    if os.path.exists(path):
        print(f"Found model artifact {path}", flush=True)
        return
    if not require:
        print(f"No model artifact at {path} yet — COLD START: events persist unscored "
              f"until you click Train on the dashboard (then the model hot-swaps in).",
              flush=True)
        return
    deadline = time.time() + timeout_s
    while not os.path.exists(path):
        if time.time() > deadline:
            raise SystemExit(f"Model artifact {path} not found after {timeout_s}s "
                             "(REQUIRE_MODEL=1).")
        print(f"Waiting for model artifact {path}", flush=True)
        time.sleep(2.0)
    print(f"Found model artifact {path}", flush=True)


def main():
    parser = ArgumentParser(description="PyFlink: score TCM-5 per-stand stream + event-time KPIs")
    parser.add_argument("--bootstrap-servers", default="localhost:39092", type=str)
    parser.add_argument("--topic", default=contract.TOPIC, type=str)
    parser.add_argument("--db-url", default="jdbc:postgresql://localhost:35432/db", type=str)
    parser.add_argument("--db-username", default="user", type=str)
    parser.add_argument("--db-password", default="user", type=str)
    parser.add_argument("--threshold", default=0.85, type=float,
                        help="reference score threshold for the windowed anomaly KPI "
                             "(keep aligned with the dashboard default)")
    parser.add_argument("--window", default="1", type=str, help="tumbling window size in minutes")
    parser.add_argument("--model-path", default=os.environ.get("MODEL_PATH", "/models/model.pkl"),
                        type=str, help="artifact to wait for before submitting")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--ui-port", default=8081, type=int)
    args = parser.parse_args()

    contract.assert_contract()
    wait_for_model(args.model_path, require=os.environ.get("REQUIRE_MODEL", "0") == "1")
    wait_for_topics(args.bootstrap_servers, args.topic)

    conf = (Configuration()
            .set_integer("rest.port", args.ui_port)
            .set_integer("table.exec.source.idle-timeout", 1000)
            .set_string("table.local-time-zone", "UTC")
            # reuse scored sub-plan across sinks -> one PythonCalc, model scores once/event
            .set_string("table.optimizer.reuse-optimize-block-with-digest-enabled", "true")
            # checkpoint -> commits offsets + arms fixed-delay restart (recover, don't die)
            .set_string("execution.checkpointing.interval", "10 s")
            .set_string("execution.checkpointing.mode", "EXACTLY_ONCE")
            .set_string("restart-strategy.type", "fixed-delay")
            .set_string("restart-strategy.fixed-delay.attempts", "2147483647")
            .set_string("restart-strategy.fixed-delay.delay", "10 s"))
    script_dir = Path(__file__).parent.resolve()
    env = StreamExecutionEnvironment.get_execution_environment(conf)
    env.add_jars(Path(script_dir, "flink-sql-connector-kafka-3.1.0-1.18.jar").as_uri())
    env.add_jars(Path(script_dir, "flink-connector-jdbc-3.1.2-1.18.jar").as_uri())
    env.add_jars(Path(script_dir, "postgresql-42.7.3.jar").as_uri())
    tenv = StreamTableEnvironment.create(
        env, EnvironmentSettings.Builder().with_configuration(conf).build())

    for name, fn in [("score_anomaly", model_udf.score_anomaly),
                     ("p_electric", model_udf.p_electric),
                     ("p_bearing", model_udf.p_bearing),
                     ("p_workroll", model_udf.p_workroll),
                     ("p_reduction", model_udf.p_reduction),
                     ("model_version", model_udf.model_version),
                     # model-free unsupervised detectors (2-of-3 vote)
                     ("u_maha", unsupervised.u_maha),
                     ("u_spc", unsupervised.u_spc),
                     ("u_knn", unsupervised.u_knn)]:
        tenv.create_temporary_system_function(name, fn)

    # ---- Kafka source: per-stand reading; `ts` must stay a real time-attribute ----
    tenv.execute_sql(f"""
        CREATE TABLE `readings` (
            `coil_id` BIGINT,
            `stand` INT,
            `file_no` INT,
            `ts_ms` BIGINT,
            `work_roll_diam` DOUBLE,
            `work_roll_mileage` DOUBLE,
            `reduction` DOUBLE,
            `roll_speed` DOUBLE,
            `force` DOUBLE,
            `torque` DOUBLE,
            `gap` DOUBLE,
            `motor_power` DOUBLE,
            `thickness_entry` DOUBLE,
            `thickness_exit` DOUBLE,
            `width` DOUBLE,
            `ys_entry` DOUBLE,
            `ys_exit` DOUBLE,
            `tension_in` DOUBLE,
            `tension_out` DOUBLE,
            `y_electric` BOOLEAN,
            `y_bearing` BOOLEAN,
            `y_workroll` BOOLEAN,
            `y_reduction` BOOLEAN,
            `ts` AS TO_TIMESTAMP_LTZ(`ts_ms`, 3),
            WATERMARK FOR `ts` AS `ts` - INTERVAL '1' SECOND
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{args.topic}',
            'properties.bootstrap.servers' = '{args.bootstrap_servers}',
            'properties.group.id' = 'tcm5-processor',
            'scan.startup.mode' = 'earliest-offset',
            'format' = 'json',
            'json.fail-on-missing-field' = 'false',
            'json.ignore-parse-errors' = 'true'
        )
        """)

    feat = ", ".join(f"`{c}`" for c in contract.FEATURES)

    # ---- shared scored view: UDFs run once per event; `ts` kept verbatim ----
    tenv.execute_sql(f"""
        CREATE TEMPORARY VIEW `scored` AS
        SELECT
            `coil_id`, `stand`, `file_no`, `ts`,
            `work_roll_diam`, `work_roll_mileage`, `reduction`, `roll_speed`,
            `force`, `torque`, `gap`, `motor_power`,
            `thickness_entry`, `thickness_exit`, `width`, `ys_entry`, `ys_exit`,
            `tension_in`, `tension_out`,
            `y_electric`, `y_bearing`, `y_workroll`, `y_reduction`,
            (`y_electric` OR `y_bearing` OR `y_workroll` OR `y_reduction`) AS `y_any`,
            score_anomaly({feat}) AS `anomaly_score`,
            p_electric({feat})    AS `p_electric`,
            p_bearing({feat})     AS `p_bearing`,
            p_workroll({feat})    AS `p_workroll`,
            p_reduction({feat})   AS `p_reduction`,
            model_version({feat}) AS `model_version`,
            u_maha({feat})        AS `u_maha`,
            u_spc({feat})         AS `u_spc`,
            u_knn({feat})         AS `u_knn`
        FROM `readings`
        WHERE `ts_ms` IS NOT NULL AND `coil_id` IS NOT NULL AND `stand` IS NOT NULL
        """)

    stmts = tenv.create_statement_set()

    # ---- sink 1: per-event scored rows (append; reading_id is Postgres IDENTITY) ----
    tenv.execute_sql(f"""
        CREATE TABLE `scored_events` (
            `ts` TIMESTAMP(3),
            `coil_id` BIGINT, `stand` INT, `file_no` INT,
            `work_roll_diam` DOUBLE, `work_roll_mileage` DOUBLE, `reduction` DOUBLE, `roll_speed` DOUBLE,
            `force` DOUBLE, `torque` DOUBLE, `gap` DOUBLE, `motor_power` DOUBLE,
            `thickness_entry` DOUBLE, `thickness_exit` DOUBLE, `width` DOUBLE,
            `ys_entry` DOUBLE, `ys_exit` DOUBLE,
            `tension_in` DOUBLE, `tension_out` DOUBLE,
            `anomaly_score` DOUBLE,
            `p_electric` DOUBLE, `p_bearing` DOUBLE, `p_workroll` DOUBLE, `p_reduction` DOUBLE,
            `y_electric` BOOLEAN, `y_bearing` BOOLEAN, `y_workroll` BOOLEAN, `y_reduction` BOOLEAN,
            `y_any` BOOLEAN, `model_version` INT,
            `u_maha` DOUBLE, `u_spc` DOUBLE, `u_knn` DOUBLE, `u_votes` INT, `u_anomaly` BOOLEAN
        ) WITH ( {connector(args, "scored_events")} )
        """)
    mt, st_, kt = (unsupervised.MAHA_THRESHOLD, unsupervised.SPC_THRESHOLD,
                   unsupervised.KNN_THRESHOLD)
    votes_expr = (f"(CASE WHEN `u_maha` > {mt} THEN 1 ELSE 0 END"
                  f" + CASE WHEN `u_spc` > {st_} THEN 1 ELSE 0 END"
                  f" + CASE WHEN `u_knn` > {kt} THEN 1 ELSE 0 END)")
    stmts.add_insert("scored_events", tenv.sql_query(f"""
        SELECT
            CAST(`ts` AS TIMESTAMP(3)) AS `ts`,
            `coil_id`, `stand`, `file_no`,
            `work_roll_diam`, `work_roll_mileage`, `reduction`, `roll_speed`,
            `force`, `torque`, `gap`, `motor_power`,
            `thickness_entry`, `thickness_exit`, `width`, `ys_entry`, `ys_exit`,
            `tension_in`, `tension_out`,
            `anomaly_score`, `p_electric`, `p_bearing`, `p_workroll`, `p_reduction`,
            `y_electric`, `y_bearing`, `y_workroll`, `y_reduction`, `y_any`, `model_version`,
            `u_maha`, `u_spc`, `u_knn`,
            {votes_expr} AS `u_votes`,
            CASE WHEN {votes_expr} >= 2 THEN TRUE ELSE FALSE END AS `u_anomaly`
        FROM `scored`
        """))

    # ---- sink 2: event-time tumbling-window KPIs (upsert) ----
    tenv.execute_sql(f"""
        CREATE TABLE `kpi_windows` (
            `window_start` TIMESTAMP(3),
            `window_end` TIMESTAMP(3),
            `coils` BIGINT,
            `events` BIGINT,
            `anomalies` BIGINT,
            `anomaly_rate` DOUBLE,
            `threshold` DOUBLE,
            PRIMARY KEY (`window_start`, `window_end`) NOT ENFORCED
        ) WITH ( {connector(args, "kpi_windows")} )
        """)
    stmts.add_insert("kpi_windows", tenv.sql_query(f"""
        SELECT
            `window_start`, `window_end`,
            COUNT(DISTINCT `coil_id`) AS `coils`,
            COUNT(*) AS `events`,
            SUM(CASE WHEN `anomaly_score` >= {args.threshold} THEN 1 ELSE 0 END) AS `anomalies`,
            CAST(SUM(CASE WHEN `anomaly_score` >= {args.threshold} THEN 1 ELSE 0 END) AS DOUBLE)
                / COUNT(*) AS `anomaly_rate`,
            CAST({args.threshold} AS DOUBLE) AS `threshold`
        FROM TABLE(TUMBLE(TABLE `scored`, DESCRIPTOR(`ts`), INTERVAL '{args.window}' MINUTES))
        GROUP BY `window_start`, `window_end`
        """))

    print(f"Submitting job: topic '{args.topic}' on '{args.bootstrap_servers}' -> '{args.db_url}'"
          + (" (dry-run)" if args.dry_run else ""), flush=True)
    stmts.execute().wait()


if __name__ == "__main__":
    main()
