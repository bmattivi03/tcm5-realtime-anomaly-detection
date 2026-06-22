#!/usr/bin/env bash
# Manage the scored_events time partitions (native PG declarative partitioning).
#
#   ./scripts/partitions.sh list                       # partitions + size + approx rows
#   ./scripts/partitions.sh maintain [keep_days] [ahead_days]
#                                                       # create a forward buffer of daily
#                                                       # partitions and DROP ones older
#                                                       # than keep_days (default 7 / 14)
#
# Retention via DROP partition is instant and reclaims disk immediately — the win over
# a bulk DELETE on the fact table. Run `maintain` from cron for a long-lived --loop run.
#
# Run from the repo root.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ -f .env ]]; then set -a; source .env; set +a; fi
PSQL=(docker compose exec -T postgres psql -U "${USERID:-user}" -d db)

case "${1:-list}" in
  list)
    "${PSQL[@]}" -c "
      SELECT c.relname AS partition,
             pg_get_expr(c.relpartbound, c.oid) AS range,
             pg_size_pretty(pg_total_relation_size(c.oid)) AS size,
             c.reltuples::bigint AS approx_rows
      FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = 'scored_events'::regclass
      ORDER BY c.relname;"
    ;;

  maintain)
    keep="${2:-7}"; ahead="${3:-14}"
    echo "Creating daily partitions up to CURRENT_DATE + ${ahead}..."
    "${PSQL[@]}" -c "
      DO \$\$
      DECLARE d date;
      BEGIN
        FOR d IN SELECT generate_series(CURRENT_DATE, CURRENT_DATE + ${ahead}, interval '1 day')::date
        LOOP
          EXECUTE format('CREATE TABLE IF NOT EXISTS scored_events_%s PARTITION OF scored_events '
                         'FOR VALUES FROM (%L) TO (%L)', to_char(d,'YYYYMMDD'), d, d + 1);
        END LOOP;
      END \$\$;"
    echo "Dropping partitions older than CURRENT_DATE - ${keep}..."
    "${PSQL[@]}" -tAc "
      SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid
      WHERE i.inhparent = 'scored_events'::regclass
        AND c.relname ~ '^scored_events_[0-9]{8}\$'
        AND to_date(right(c.relname, 8), 'YYYYMMDD') < CURRENT_DATE - ${keep}" \
      | while read -r part; do
          [ -n "$part" ] && "${PSQL[@]}" -c "DROP TABLE ${part};" >/dev/null && echo "  dropped ${part}"
        done
    echo "Done."
    ;;

  *)
    echo "usage: $0 [list | maintain [keep_days] [ahead_days]]"; exit 2 ;;
esac
