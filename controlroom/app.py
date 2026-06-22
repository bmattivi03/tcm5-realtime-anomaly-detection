#!/usr/bin/env python
"""
TCM-5 control room — the live *operational* view of the mill.

Grafana stays the analytical dashboard; this page is the operational view:
an animated tandem-mill schematic, alarm ticker and hot-swap choreography,
all fed by one SSE stream.

One background task polls Postgres (READ-ONLY) about once per second, builds a
single JSON snapshot and fans it out to every connected SSE client through
per-client queues, so the DB load is O(1) in the number of viewers.

  GET  /               the control-room page (static/index.html)
  GET  /api/snapshot   latest snapshot JSON
  GET  /stream         text/event-stream, named event "snapshot"
  GET  /api/coil/{id}  per-stand latest rows for one coil (ticker drill-down)
  POST /api/retrain    proxy to the retrain sidecar (avoids browser CORS)
  GET  /api/policy     proxy to the sidecar's auto-retrain policy, if present
"""

import asyncio
import json
import logging
import os
import time
import urllib.error
import urllib.request
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

logging.basicConfig(level=logging.INFO, format="%(asctime)s (%(levelname).1s) %(message)s")
log = logging.getLogger("controlroom")

DB_DSN = os.environ.get("DB_DSN", "host=localhost port=35432 dbname=db user=user password=user")
RETRAIN_URL = os.environ.get("RETRAIN_URL", "http://retrain:8000").rstrip("/")
POLL_S = float(os.environ.get("POLL_INTERVAL_S", "1.0"))
STATIC_DIR = Path(__file__).resolve().parent / "static"
FALLBACK_THRESHOLD = 0.85
FAMILIES = ("electric", "bearing", "workroll", "reduction")
# Unsupervised detector cutoffs — keep in sync with processor/unsupervised.py.
MAHA_T, SPC_T, KNN_T = 23.21, 3.0, 3.0

_conn = None              # persistent connection, used only by the poller thread
_has_ingested = None      # tri-state: None = unknown, re-check info_schema
_ing_checked_at = 0.0
_latest: "dict | None" = None
_clients: "set[asyncio.Queue]" = set()

MODEL_SQL = ('SELECT version, trained_at, n_train_samples, threshold, overall_f1, overall_pr_auc, '
             '"precision", recall, recall_electric, recall_bearing, recall_workroll, '
             'recall_reduction, trained_on FROM model_versions ORDER BY version')
STANDS_SQL = (f"SELECT stand, count(*), avg((anomaly_score >= %s)::int), "
              f"avg(p_electric), avg(p_bearing), avg(p_workroll), avg(p_reduction), "
              f"avg(force), avg(work_roll_mileage), max(model_version), "
              f"avg(CASE WHEN u_anomaly THEN 1 ELSE 0 END), "
              f"avg(CASE WHEN u_maha > {MAHA_T} THEN 1 ELSE 0 END), "
              f"avg(CASE WHEN u_spc  > {SPC_T} THEN 1 ELSE 0 END), "
              f"avg(CASE WHEN u_knn  > {KNN_T} THEN 1 ELSE 0 END) "
              f"FROM scored_events WHERE ts > %s::timestamp - interval '10 seconds' GROUP BY stand")
TICKER_SQL = ("SELECT reading_id, ts, coil_id, stand, anomaly_score, "
              "p_electric, p_bearing, p_workroll, p_reduction "
              "FROM scored_events WHERE anomaly_score >= %s ORDER BY reading_id DESC LIMIT 25")
# Coils to inspect: flagged by the supervised model (score >= threshold) OR the
# model-free 2-of-3 ensemble (u_anomaly). Predicted fault = argmax family prob at the
# worst stand (a prediction, never ground truth; only meaningful once a model exists).
INSPECT_SQL = (
    "WITH ev AS ("
    "  SELECT DISTINCT ON (coil_id, stand) coil_id, stand, anomaly_score,"
    "         p_electric, p_bearing, p_workroll, p_reduction, model_version, ts,"
    "         u_anomaly, u_votes"
    "  FROM scored_events WHERE ts > %(anchor)s::timestamp - interval '15 minutes'"
    "  ORDER BY coil_id, stand, reading_id DESC),"
    " worst AS ("
    "  SELECT DISTINCT ON (coil_id) coil_id, stand AS worst_stand, anomaly_score, model_version, ts,"
    "         CASE GREATEST(p_electric,p_bearing,p_workroll,p_reduction)"
    "              WHEN p_electric THEN 'electric' WHEN p_bearing THEN 'bearing'"
    "              WHEN p_workroll THEN 'workroll' ELSE 'reduction' END AS predicted_fault"
    "  FROM ev ORDER BY coil_id, anomaly_score DESC),"
    " agg AS ("
    "  SELECT coil_id, max(anomaly_score) AS max_score,"
    "         bool_or(u_anomaly) AS unsup_flag, max(u_votes) AS u_votes,"
    "         count(*) FILTER (WHERE anomaly_score>=%(thr)s) AS stands_alarmed,"
    "         min(stand) FILTER (WHERE anomaly_score>=%(thr)s) AS first_stand"
    "  FROM ev GROUP BY coil_id"
    "  HAVING max(anomaly_score) >= %(thr)s OR bool_or(u_anomaly))"
    " SELECT a.coil_id, w.anomaly_score, w.predicted_fault, w.worst_stand,"
    "        a.first_stand, a.stands_alarmed, w.model_version, w.ts,"
    "        (a.max_score >= %(thr)s) AS model_flag, a.unsup_flag, a.u_votes"
    " FROM agg a JOIN worst w USING (coil_id)"
    " ORDER BY (a.max_score >= %(thr)s) DESC, a.max_score DESC NULLS LAST, a.u_votes DESC NULLS LAST"
    " LIMIT 100")
KPI_SQL = ("SELECT window_start, window_end, coils, events, anomalies, anomaly_rate "
           "FROM kpi_windows ORDER BY window_end DESC LIMIT 1")
LATENCY_SQL = ("SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY d) * 1000, "
               "percentile_cont(0.95) WITHIN GROUP (ORDER BY d) * 1000 "
               "FROM (SELECT extract(epoch from (ingested_at - ts)) AS d FROM scored_events "
               "      WHERE ts > %s::timestamp - interval '60 seconds' "
               "        AND ingested_at > (now() at time zone 'utc') - interval '60 seconds') t")
COIL_SQL = ("SELECT DISTINCT ON (coil_id, stand) stand, ts, force, torque, gap, "
            "tension_in, tension_out, anomaly_score, p_electric, p_bearing, p_workroll, "
            "p_reduction, y_electric, y_bearing, y_workroll, y_reduction, y_any, model_version "
            "FROM scored_events WHERE coil_id = %s ORDER BY coil_id, stand, reading_id DESC")


def _connect(retries=10):
    """Reconnect-with-backoff, same shape as retrain.py; autocommit = plain read-only SELECTs."""
    last = None
    for _ in range(retries):
        try:
            conn = psycopg2.connect(DB_DSN)
            conn.autocommit = True
            return conn
        except psycopg2.OperationalError as ex:
            last = ex
            time.sleep(2.0)
    raise last


def _iso(v):
    return None if v is None else v.isoformat(timespec="milliseconds") + "Z"


def _f(v):
    return None if v is None else float(v)


def _argmax_family(ps):
    vals = [-1.0 if p is None else p for p in ps]
    return None if max(vals) < 0 else FAMILIES[vals.index(max(vals))]


def build_snapshot(conn) -> dict:
    global _has_ingested, _ing_checked_at
    with conn.cursor() as cur:
        cur.execute(MODEL_SQL)
        versions = [{"version": int(r[0]), "trained_at": _iso(r[1]),
                     "n_train_samples": None if r[2] is None else int(r[2]),
                     "threshold": _f(r[3]), "overall_f1": _f(r[4]), "overall_pr_auc": _f(r[5]),
                     "precision": _f(r[6]), "recall": _f(r[7]), "recall_electric": _f(r[8]),
                     "recall_bearing": _f(r[9]), "recall_workroll": _f(r[10]),
                     "recall_reduction": _f(r[11]), "trained_on": r[12]} for r in cur.fetchall()]
        latest = versions[-1] if versions else None
        threshold = (latest or {}).get("threshold") or FALLBACK_THRESHOLD

        cur.execute("SELECT max(ts) FROM scored_events")
        anchor = cur.fetchone()[0]

        # Per-stand state over the last 10 s of STREAM time (anchored to max(ts), not now()).
        stands = {s: {"stand": s, "events": 0, "alarm_rate": 0.0, "p_electric": None,
                      "p_bearing": None, "p_workroll": None, "p_reduction": None,
                      "dominant": None, "force": None, "work_roll_mileage": None,
                      "u_rate": 0.0, "u_maha": 0.0, "u_spc": 0.0, "u_knn": 0.0}
                  for s in range(1, 6)}
        live_version = None
        if anchor is not None:
            cur.execute(STANDS_SQL, (threshold, anchor))
            for (s, n, ar, pe, pb, pw, pr, force, mileage, mv,
                 ur, uma, usp, ukn) in cur.fetchall():
                ps = [_f(pe), _f(pb), _f(pw), _f(pr)]
                stands[s] = {"stand": s, "events": int(n), "alarm_rate": _f(ar) or 0.0,
                             "p_electric": ps[0], "p_bearing": ps[1], "p_workroll": ps[2],
                             "p_reduction": ps[3], "dominant": _argmax_family(ps),
                             "force": _f(force), "work_roll_mileage": _f(mileage),
                             "u_rate": _f(ur) or 0.0, "u_maha": _f(uma) or 0.0,
                             "u_spc": _f(usp) or 0.0, "u_knn": _f(ukn) or 0.0}
                if mv is not None:
                    live_version = mv if live_version is None else max(live_version, mv)

        cur.execute(TICKER_SQL, (threshold,))
        ticker = [{"reading_id": int(r[0]), "ts": _iso(r[1]), "coil_id": int(r[2]),
                   "stand": int(r[3]), "anomaly_score": _f(r[4]),
                   "family": _argmax_family([_f(v) for v in r[5:9]])} for r in cur.fetchall()]

        # Coils to inspect: flagged by the model OR the 2-of-3 ensemble.
        inspect = []
        if anchor is not None:
            cur.execute(INSPECT_SQL, {"anchor": anchor, "thr": threshold})
            for r in cur.fetchall():
                model_flag, unsup_flag = bool(r[8]), bool(r[9])
                inspect.append({
                    "coil_id": int(r[0]), "confidence": _f(r[1]),
                    "predicted_fault": r[2] if model_flag else None,
                    "worst_stand": int(r[3]),
                    "first_stand": None if r[4] is None else int(r[4]),
                    "stands_alarmed": int(r[5]),
                    "model_version": None if r[6] is None else int(r[6]),
                    "ts": _iso(r[7]),
                    "model_flag": model_flag, "unsup_flag": unsup_flag,
                    "u_votes": None if r[10] is None else int(r[10]),
                    "flagged_by": "both" if model_flag and unsup_flag
                                  else "model" if model_flag else "ensemble"})

        cur.execute("SELECT count(DISTINCT coil_id) FROM scored_events")
        coils_total = int(cur.fetchone()[0])

        cur.execute("SELECT extract(epoch from ((now() at time zone 'utc') - max(ts))) FROM scored_events")
        freshness = _f(cur.fetchone()[0])

        cur.execute(KPI_SQL)
        row = cur.fetchone()
        kpi, coils_per_min = None, None
        if row:
            dur = (row[1] - row[0]).total_seconds()
            coils_per_min = round(int(row[2]) * 60.0 / dur, 1) if dur > 0 else None
            kpi = {"window_start": _iso(row[0]), "window_end": _iso(row[1]), "coils": int(row[2]),
                   "events": int(row[3]), "anomalies": int(row[4]), "anomaly_rate": _f(row[5])}

        # Latency only if the ingested_at column exists (it is being added; degrade to nulls).
        p50 = p95 = None
        if _has_ingested is None or (_has_ingested is False and time.time() - _ing_checked_at > 60):
            cur.execute("SELECT count(*) FROM information_schema.columns "
                        "WHERE table_name = 'scored_events' AND column_name = 'ingested_at'")
            _has_ingested, _ing_checked_at = cur.fetchone()[0] > 0, time.time()
        if _has_ingested and anchor is not None:
            try:
                cur.execute(LATENCY_SQL, (anchor,))
                p50, p95 = (_f(v) for v in cur.fetchone())
            except psycopg2.Error as ex:           # column dropped mid-flight, etc.
                log.warning(f"latency query degraded: {ex}")
                _has_ingested, _ing_checked_at = False, time.time()

    events_10s = sum(st["events"] for st in stands.values())
    unsup_anoms = sum(st["events"] * st["u_rate"] for st in stands.values())
    unsup_rate_10s = round(unsup_anoms / events_10s, 4) if events_10s else 0.0
    return {
        "generated_at": _iso(datetime.now(timezone.utc).replace(tzinfo=None)),
        "anchor_ts": _iso(anchor),
        "stands": [stands[s] for s in range(1, 6)],
        "ticker": ticker,
        "inspect": inspect,
        "rates": {"events_10s": events_10s, "events_per_s": round(events_10s / 10.0, 1),
                  "coils_per_min": coils_per_min, "coils_total": coils_total,
                  "unsup_rate_10s": unsup_rate_10s, "freshness_s": freshness,
                  "latency_ms_p50": p50, "latency_ms_p95": p95, "kpi_window": kpi},
        "model": {"versions": versions, "latest": latest, "threshold": threshold,
                  "live_version": None if live_version is None else int(live_version)},
    }


def _poll_once():
    global _conn, _has_ingested
    if _conn is None or _conn.closed:
        _conn = _connect()
        _has_ingested = None
    try:
        return build_snapshot(_conn)
    except psycopg2.Error:
        try:
            _conn.close()
        except Exception:
            pass
        _conn = None
        raise


async def _poller():
    global _latest
    loop = asyncio.get_running_loop()
    while True:
        t0 = time.monotonic()
        try:
            snap = await loop.run_in_executor(None, _poll_once)
            _latest = snap
            payload = "event: snapshot\ndata: " + json.dumps(snap, separators=(",", ":")) + "\n\n"
            for q in list(_clients):               # fan the SAME payload out to every client
                if q.full():
                    try:
                        q.get_nowait()             # drop the oldest, never block the poller
                    except asyncio.QueueEmpty:
                        pass
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    pass
        except Exception as ex:
            log.warning(f"poll failed (will retry): {ex}")
        await asyncio.sleep(max(0.1, POLL_S - (time.monotonic() - t0)))


@asynccontextmanager
async def lifespan(_app):
    task = asyncio.create_task(_poller())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


app = FastAPI(title="TCM-5 control room", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


@app.get("/")
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/snapshot")
def snapshot():
    if _latest is None:
        return JSONResponse({"status": "warming up"}, status_code=503)
    return JSONResponse(_latest)


@app.get("/stream")
async def stream():
    q: asyncio.Queue = asyncio.Queue(maxsize=4)
    _clients.add(q)
    if _latest is not None:                        # greet new clients with the current state
        q.put_nowait("event: snapshot\ndata: " + json.dumps(_latest, separators=(",", ":")) + "\n\n")

    async def gen():
        try:
            while True:
                try:
                    yield await asyncio.wait_for(q.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:                                   # client disconnected (or shutdown)
            _clients.discard(q)

    return StreamingResponse(gen(), media_type="text/event-stream",
                             headers={"Cache-Control": "no-cache, no-transform",
                                      "Connection": "keep-alive", "X-Accel-Buffering": "no"})


def _coil_rows(coil_id: int):
    conn = _connect(retries=2)                     # short-lived, separate from the poller's conn
    try:
        with conn.cursor() as cur:
            cur.execute(COIL_SQL, (coil_id,))
            cols = [c.name for c in cur.description]
            out = []
            for row in cur.fetchall():
                d = dict(zip(cols, row))
                d["ts"] = _iso(d["ts"])
                out.append(d)
            return out
    finally:
        conn.close()


@app.get("/api/coil/{coil_id}")
async def coil(coil_id: int):
    loop = asyncio.get_running_loop()
    try:
        rows = await loop.run_in_executor(None, _coil_rows, coil_id)
    except psycopg2.Error as ex:
        return JSONResponse({"error": str(ex)}, status_code=503)
    return {"coil_id": coil_id, "stands": rows}


def _proxy(method: str, path: str, timeout: float):
    req = urllib.request.Request(RETRAIN_URL + path, method=method,
                                 data=b"" if method == "POST" else None)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as ex:           # 4xx/5xx still carry a JSON body
        return ex.code, ex.read()


@app.post("/api/retrain")
async def retrain():
    loop = asyncio.get_running_loop()
    try:                                            # retraining can take 60+ s
        status, body = await loop.run_in_executor(None, _proxy, "POST", "/retrain", 600.0)
    except Exception as ex:
        return JSONResponse({"error": f"retrain sidecar unreachable: {ex}"}, status_code=502)
    try:
        payload = json.loads(body)
    except ValueError:
        payload = {"detail": body.decode("utf-8", "replace")}
    return JSONResponse(payload, status_code=status)


@app.get("/api/policy")
async def policy():
    loop = asyncio.get_running_loop()
    try:
        status, body = await loop.run_in_executor(None, _proxy, "GET", "/policy", 5.0)
        data = json.loads(body)
        if status == 200 and isinstance(data, dict):
            # the sidecar nests last_check/last_reason under "state"; flatten them to
            # the top level so the control-room policy line can read them directly
            state = data.get("state", {})
            return {"available": True, "enabled": data.get("enabled"),
                    "config": data.get("config"), **state}
    except Exception:
        pass
    return {"available": False}
