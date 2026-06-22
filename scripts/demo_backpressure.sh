#!/usr/bin/env bash
# Backpressure demo: overload the pipeline, watch it degrade gracefully, drain back.
#
#   ./scripts/demo_backpressure.sh surge    # relaunch the producer at 10x speed
#   ./scripts/demo_backpressure.sh drain    # restore the configured pace
#
# What to point at while it runs:
#   * Flink UI (http://localhost:38081): the source->score chain turns busy/back-pressured
#   * Grafana "End-to-end latency" panel: p95 climbs during the surge, drains after
#   * kafka-ui (http://localhost:38080): consumer-group lag builds, then empties
#   * the producer's own pacing logs: produce_or_block + the rate limiter absorb the
#     overload instead of crashing — graceful backpressure end to end
#
# Run from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then set -a; source .env; set +a; fi

mode="${1:-surge}"
base="${PRODUCER_SPEEDUP:-30}"
case "$mode" in
  surge) speed=$((base * 10)) ;;
  drain) speed=$base ;;
  *) echo "usage: $0 [surge|drain]"; exit 2 ;;
esac

echo "Relaunching producer at ~${speed} coils/s (${mode})..."
docker compose rm -sf producer >/dev/null 2>&1 || true
PRODUCER_SPEEDUP=$speed docker compose up -d producer
echo "Producer running at ~${speed} coils/s. Watch the latency panel + kafka-ui lag."
