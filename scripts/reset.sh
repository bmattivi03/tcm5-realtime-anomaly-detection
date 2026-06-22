#!/usr/bin/env bash
# Clean slate for a fresh demo run: cancel Flink jobs, empty the sink tables, and
# delete the Kafka topic.
#
#   ./scripts/reset.sh          soft reset: keeps the model (re-stream with the model live)
#   ./scripts/reset.sh --cold   TRUE cold start: also wipes model_versions + models/*.pkl,
#                               so the next run begins with NO model (first TRAIN builds v1)
#
# Does NOT touch volumes. Only `docker compose down -v` is needed, and ONLY when the DB
# schema (postgres/init.sql) changes, since init.sql runs only on a fresh volume.
# Run from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then set -a; source .env; set +a; fi
FLINK_PORT="${PORT_FLINK:-38081}"
COLD=0
[[ "${1:-}" == "--cold" ]] && COLD=1

# reset.sh talks to postgres (TRUNCATE) and the broker (topic delete), so make sure they
# are up even if the whole stack was just `docker compose stop`-ped — otherwise the
# TRUNCATE fails with "service postgres is not running" and the reset aborts half-done.
echo "Ensuring postgres + broker are up..."
docker compose up -d --wait postgres broker >/dev/null 2>&1 \
  || docker compose up -d postgres broker >/dev/null 2>&1 || true

echo "Cancelling running Flink jobs..."
for jid in $(curl -s "localhost:${FLINK_PORT}/jobs" \
    | python3 -c "import sys,json;print(' '.join(j['id'] for j in json.load(sys.stdin).get('jobs',[]) if j['status']=='RUNNING'))" 2>/dev/null); do
  curl -s -X PATCH "localhost:${FLINK_PORT}/jobs/${jid}?mode=cancel" -o /dev/null && echo "  cancelled ${jid}"
done

echo "Stopping producer/processor containers..."
docker compose rm -sf producer processor >/dev/null 2>&1 || true

if [[ $COLD == 1 ]]; then
  echo "Cold start: truncating sink tables + model registry..."
  docker compose exec -T postgres psql -U "${USERID:-user}" -d db -c "TRUNCATE scored_events, kpi_windows, model_versions;"
  echo "Cold start: removing model artifacts (models-data volume)..."
  docker compose run --rm --no-deps -T retrain \
    rm -f /models/model.pkl /models/holdout.pkl /models/model.meta 2>/dev/null || true
else
  echo "Truncating sink tables..."
  docker compose exec -T postgres psql -U "${USERID:-user}" -d db -c "TRUNCATE scored_events, kpi_windows;"
fi

echo "Deleting Kafka topic..."
docker compose exec -T broker kafka-topics --bootstrap-server broker:9092 --delete --topic "${TOPIC:-tcm5_readings}" 2>/dev/null || true

if [[ $COLD == 1 ]]; then
  echo "Cold reset complete. Next: ./scripts/processor-submit.sh && docker compose up -d producer"
  echo "  -> stream starts with NO model; click TRAIN MODEL in the control room to build v1."
else
  echo "Reset complete (model kept). Next: ./scripts/processor-submit.sh && docker compose up -d producer"
fi
