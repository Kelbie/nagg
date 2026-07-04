#!/usr/bin/env bash
# Cap ClickHouse's own observability tables at 3 days. Without a TTL they grow
# forever — measured ~7 GiB across text_log / trace_log / processors_profile_log
# / part_log / query_log / metric_log / asynchronous_metric_log on the Railway
# instance, pure disk pressure with no product value past a few days.
#
# This is an OPERATIONAL script, not a migration: system.* tables are created
# lazily by the server (a fresh instance may not have them yet), so putting
# these ALTERs in the deploy-blocking migration chain would fail exactly on new
# environments. TTLs persist in table metadata on the volume; re-running is
# idempotent. materialize_ttl_after_modify=0 keeps each ALTER a metadata-only
# change (background merges apply the TTL gradually).
#
# Usage:
#   CLICKHOUSE_URL=https://host CLICKHOUSE_USER=admin CLICKHOUSE_PASSWORD=... \
#     ./scripts/clickhouse-system-log-ttl.sh [days]
set -euo pipefail

DAYS="${1:-3}"
: "${CLICKHOUSE_URL:?set CLICKHOUSE_URL}"
: "${CLICKHOUSE_USER:?set CLICKHOUSE_USER}"
: "${CLICKHOUSE_PASSWORD:?set CLICKHOUSE_PASSWORD}"

TABLES=(text_log trace_log query_log part_log metric_log asynchronous_metric_log processors_profile_log)

for table in "${TABLES[@]}"; do
  printf '%s: ' "$table"
  if curl -sf --max-time 60 "$CLICKHOUSE_URL" -u "$CLICKHOUSE_USER:$CLICKHOUSE_PASSWORD" \
    --data-binary "ALTER TABLE system.$table MODIFY TTL event_date + INTERVAL $DAYS DAY SETTINGS materialize_ttl_after_modify=0"; then
    echo OK
  else
    echo "SKIPPED (table absent or ALTER failed)"
  fi
done
