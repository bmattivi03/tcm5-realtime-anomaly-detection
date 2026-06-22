#!/usr/bin/env bash
# (Re)submit the PyFlink processor job.
#
# Removing the processor *container* does NOT cancel the Flink *job* running on the
# cluster — so always cancel any running job (and WAIT for the cancellation to
# complete) before resubmitting, or two jobs consume the topic in parallel and
# double-count. This script does that safely, and rebuilds the images first so the
# submit-side and worker-side copies of contract/modeling/model_udf never skew.
#
# Run from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then set -a; source .env; set +a; fi
FLINK_PORT="${PORT_FLINK:-38081}"

echo "Rebuilding images (no-op when nothing changed)..."
docker compose build -q flink-jobmanager flink-taskmanager processor
docker compose up -d flink-jobmanager flink-taskmanager

echo "Cancelling any non-terminal Flink jobs..."
jids=$(curl -s "localhost:${FLINK_PORT}/jobs" \
    | python3 -c "import sys,json;print(' '.join(j['id'] for j in json.load(sys.stdin).get('jobs',[]) if j['status'] in ('RUNNING','CREATED','RESTARTING','CANCELLING')))" 2>/dev/null || true)
for jid in $jids; do
  curl -s -X PATCH "localhost:${FLINK_PORT}/jobs/${jid}?mode=cancel" -o /dev/null && echo "  cancelling ${jid}"
done

# wait until every cancelled job reaches a terminal state before resubmitting
for jid in $jids; do
  for _ in $(seq 1 60); do
    state=$(curl -s "localhost:${FLINK_PORT}/jobs/${jid}" \
        | python3 -c "import sys,json;print(json.load(sys.stdin).get('state',''))" 2>/dev/null || echo "")
    case "$state" in CANCELED|FINISHED|FAILED|"") break ;; esac
    sleep 1
  done
  echo "  ${jid} -> ${state:-gone}"
done

docker compose rm -sf processor >/dev/null 2>&1 || true
docker compose up -d processor
echo "Processor (re)submitted. Flink UI: http://localhost:${FLINK_PORT}"
