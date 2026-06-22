#!/usr/bin/env python
"""
FastAPI wrapper around the retrain core + the drift-triggered auto-retrain policy.

  GET  /          small HTML page with a "Retrain now" button (clickable from a
                  Grafana dashboard link)
  GET  /health    liveness
  POST /retrain   train on streamed data, hot-swap the model, return {version, metrics}
  GET  /versions  the model_versions history (JSON, properly typed)
  GET  /policy    the auto-retrain policy state (enabled, last check, last reason)
  POST /policy/enable | /policy/disable

A single-flight lock makes concurrent retrains safe (the API returns 409 while one
runs; the policy thread just skips its tick).

The policy closes the MLOps loop: every POLICY_CHECK_S it measures LIVE per-family
recall over the last few minutes of streamed, labeled events (prediction =
anomaly_score >= the live model's threshold, truth = the y_* labels the producer
carries). When a family with enough positives in the window falls below the recall
floor — exactly what happens when files 3-6 introduce fault families the v1
bootstrap never saw — it fires retrain.run_retrain() automatically and the new model
hot-swaps into the running Flink job. Monitor -> detect drift -> retrain -> hot-swap,
no human in the loop (the button still works for manual runs).
"""

import os
import time
import logging
import threading
from datetime import datetime, timezone

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse

import contract
import retrain

log = logging.getLogger("retrain.app")

app = FastAPI(title="TCM-5 retrain sidecar")
_lock = threading.Lock()

POLICY = {
    "enabled": os.environ.get("RETRAIN_AUTO", "1") == "1",
    "check_every_s": float(os.environ.get("RETRAIN_CHECK_S", "20")),
    "window_minutes": float(os.environ.get("RETRAIN_WINDOW_MIN", "3")),
    "recall_floor": float(os.environ.get("RETRAIN_RECALL_FLOOR", "0.5")),
    "min_family_pos": int(os.environ.get("RETRAIN_MIN_FAMILY_POS", "200")),
    "min_interval_s": float(os.environ.get("RETRAIN_MIN_INTERVAL_S", "180")),
    "max_stream_idle_s": float(os.environ.get("RETRAIN_MAX_IDLE_S", "60")),
}
_policy_state = {
    "last_check": None, "last_reason": "not checked yet",
    "last_fired": None, "fires": 0, "live_recall": {},
}


def _policy_tick():
    """One drift check. Returns the reason string (also stored in the state)."""
    conn = retrain._connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT version, threshold, trained_at FROM model_versions "
                        "ORDER BY version DESC LIMIT 1")
            row = cur.fetchone()
            if row is None:
                return "no model yet — click Train on the dashboard to build v1"
            _, threshold, trained_at = row

            cur.execute("SELECT max(ts) FROM scored_events")
            max_ts = cur.fetchone()[0]
            if max_ts is None:
                return "no streamed events yet"
            idle_s = (datetime.now(timezone.utc).replace(tzinfo=None) - max_ts).total_seconds()
            if idle_s > POLICY["max_stream_idle_s"]:
                return f"stream idle for {idle_s:.0f}s — leaving retrains to the button"

            since_train_s = (datetime.now(timezone.utc).replace(tzinfo=None)
                             - trained_at).total_seconds()
            if since_train_s < POLICY["min_interval_s"]:
                return f"current model is only {since_train_s:.0f}s old"

            # live per-family recall over the last window of STREAM time
            cur.execute(f"""
                WITH w AS (
                  SELECT anomaly_score, y_electric, y_bearing, y_workroll, y_reduction
                  FROM scored_events
                  WHERE ts >= (SELECT max(ts) FROM scored_events)
                              - interval '{POLICY["window_minutes"]} minutes'
                )
                SELECT
                  count(*) FILTER (WHERE y_electric),
                  count(*) FILTER (WHERE y_electric  AND anomaly_score >= %(t)s),
                  count(*) FILTER (WHERE y_bearing),
                  count(*) FILTER (WHERE y_bearing   AND anomaly_score >= %(t)s),
                  count(*) FILTER (WHERE y_workroll),
                  count(*) FILTER (WHERE y_workroll  AND anomaly_score >= %(t)s),
                  count(*) FILTER (WHERE y_reduction),
                  count(*) FILTER (WHERE y_reduction AND anomaly_score >= %(t)s)
                FROM w""", {"t": threshold})
            c = cur.fetchone()
    finally:
        conn.close()

    drifted = []
    live = {}
    for j, fam in enumerate(contract.LABELS):
        pos, caught = c[2 * j], c[2 * j + 1]
        if pos:
            live[fam] = round(caught / pos, 3)
        if pos >= POLICY["min_family_pos"] and caught / pos < POLICY["recall_floor"]:
            drifted.append(f"{fam} (recall {caught / pos:.2f} over {pos} live faults)")
    _policy_state["live_recall"] = live

    if not drifted:
        return f"live recall healthy: {live or 'no labeled faults in window'}"

    reason = "drift detected — " + ", ".join(drifted)
    if not _lock.acquire(blocking=False):
        return reason + " — but a retrain is already running"
    try:
        log.info(f"AUTO-RETRAIN firing: {reason}")
        result = retrain.run_retrain()
        _policy_state["last_fired"] = datetime.now(timezone.utc).isoformat()
        _policy_state["fires"] += 1
        return reason + f" -> retrained to v{result['version']}"
    finally:
        _lock.release()


def _policy_loop():
    while True:
        time.sleep(POLICY["check_every_s"])
        if not POLICY["enabled"]:
            continue
        try:
            reason = _policy_tick()
        except Exception as ex:
            reason = f"check failed: {ex}"
            log.warning(f"policy tick failed: {ex}")
        _policy_state["last_check"] = datetime.now(timezone.utc).isoformat()
        _policy_state["last_reason"] = reason


@app.on_event("startup")
def _start_policy_thread():
    threading.Thread(target=_policy_loop, daemon=True, name="retrain-policy").start()
    log.info(f"auto-retrain policy thread started: {POLICY}")


PAGE = """<!doctype html><html><head><meta charset="utf-8">
<title>TCM-5 · Retrain</title>
<style>
 body{font-family:system-ui,Segoe UI,Roboto,sans-serif;background:#0b0e14;color:#e6e6e6;
      display:flex;min-height:100vh;align-items:center;justify-content:center;margin:0}
 .card{background:#161b22;border:1px solid #30363d;border-radius:14px;padding:36px 40px;max-width:680px;width:92%}
 h1{margin:0 0 4px;font-size:20px} p{color:#9aa4b2;margin:6px 0 20px;line-height:1.5}
 button{background:#2f81f7;color:#fff;border:0;border-radius:8px;padding:12px 22px;font-size:15px;
        cursor:pointer} button:disabled{opacity:.5;cursor:wait}
 pre{background:#0b0e14;border:1px solid #30363d;border-radius:8px;padding:14px;overflow:auto;
     font-size:12.5px;color:#c9d1d9;margin-top:20px;max-height:340px}
 .ok{color:#3fb950} .err{color:#f85149} .pol{font-size:12.5px;color:#9aa4b2;margin-top:14px}
</style></head><body><div class="card">
 <h1>TCM-5 — retrain on the live stream</h1>
 <p>Trains a fresh model on the coils streamed so far (held-out set excluded), evaluates it on the
 fixed held-out set, and hot-swaps it into the running Flink job — no restart. Watch the MODELS
 section of the dashboard update. An automatic drift policy also watches live per-family recall
 and fires this for you when a fault family the model has not learned starts streaming.</p>
 <button id="go" onclick="go()">Retrain now</button>
 <span id="status"></span>
 <div class="pol" id="policy">policy: …</div>
 <pre id="out">(no run yet)</pre>
</div>
<script>
async function go(){
 const b=document.getElementById('go'), s=document.getElementById('status'), o=document.getElementById('out');
 b.disabled=true; s.textContent=' training…'; o.textContent='';
 try{
   const r=await fetch('retrain',{method:'POST'});
   const j=await r.json();
   if(!r.ok){throw new Error(j.detail||r.statusText);}
   s.innerHTML=' <span class="ok">done → v'+j.version+'</span>';
   o.textContent=JSON.stringify(j,null,2);
 }catch(e){ s.innerHTML=' <span class="err">failed</span>'; o.textContent=String(e); }
 finally{ b.disabled=false; }
}
async function poll(){
 try{
   const j=await (await fetch('policy')).json();
   document.getElementById('policy').textContent=
     'policy: '+(j.enabled?'on':'off')+' · '+(j.state.last_reason||'');
 }catch(e){}
 setTimeout(poll, 5000);
}
poll();
</script></body></html>"""


@app.get("/", response_class=HTMLResponse)
def index():
    return PAGE


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/retrain")
def do_retrain():
    if not _lock.acquire(blocking=False):
        raise HTTPException(status_code=409, detail="a retrain is already in progress")
    try:
        return retrain.run_retrain()
    except RuntimeError as ex:
        # expected preconditions (no holdout yet / not enough streamed data)
        log.warning(f"retrain precondition failed: {ex}")
        raise HTTPException(status_code=409, detail=str(ex))
    except Exception as ex:
        log.exception("retrain failed")
        raise HTTPException(status_code=500, detail=str(ex))
    finally:
        _lock.release()


@app.get("/policy")
def policy():
    return {"enabled": POLICY["enabled"], "config": POLICY, "state": _policy_state}


@app.post("/policy/enable")
def policy_enable():
    POLICY["enabled"] = True
    return {"enabled": True}


@app.post("/policy/disable")
def policy_disable():
    POLICY["enabled"] = False
    return {"enabled": False}


@app.get("/versions")
def versions():
    conn = retrain._connect()
    try:
        with conn.cursor() as cur:
            cur.execute('SELECT version, trained_at, n_train_samples, threshold, overall_f1, '
                        'overall_pr_auc, "precision", recall, recall_electric, recall_bearing, '
                        'recall_workroll, recall_reduction, trained_on '
                        'FROM model_versions ORDER BY version')
            cols = [c[0] for c in cur.description]
            return [
                dict(zip(cols, [v.isoformat() if isinstance(v, datetime) else v for v in row]))
                for row in cur.fetchall()
            ]
    finally:
        conn.close()
