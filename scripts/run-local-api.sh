#!/usr/bin/env bash
# Start the nagg GraphQL API locally against the running `nagg-db` ClickHouse container.
#
# Unlike docs/how_to_run.md (Docker-network flow), this assumes nagg-db already
# publishes ClickHouse to the host (default: 127.0.0.1:9000). It resolves the
# container's CLICKHOUSE_USER/PASSWORD at runtime so credentials are never stored
# on disk, builds the binary if needed, and serves GraphQL on :8080.
#
# Usage:  scripts/run-local-api.sh
# Env:    NAGG_DB_CONTAINER (default nagg-db), NAGG_API_ADDR (default :8080),
#         NAGG_CLICKHOUSE_ADDR (default 127.0.0.1:9000)
set -euo pipefail

cd "$(dirname "$0")/.."

container="${NAGG_DB_CONTAINER:-nagg-db}"
addr="${NAGG_API_ADDR:-:8080}"

if ! docker inspect "$container" >/dev/null 2>&1; then
  echo "ERROR: ClickHouse container '$container' not found. Start it first (set NAGG_DB_CONTAINER to override)." >&2
  exit 1
fi

if [ ! -x bin/nagg-api ]; then
  echo "building bin/nagg-api ..." >&2
  go build -o bin/nagg-api ./cmd/api
fi

# Pull credentials straight from the container env; never echo their values.
chenv="$(docker inspect "$container" --format '{{range .Config.Env}}{{println .}}{{end}}')"
NAGG_CLICKHOUSE_USERNAME="$(printf '%s\n' "$chenv" | sed -n 's/^CLICKHOUSE_USER=//p')"
NAGG_CLICKHOUSE_PASSWORD="$(printf '%s\n' "$chenv" | sed -n 's/^CLICKHOUSE_PASSWORD=//p')"
export NAGG_CLICKHOUSE_USERNAME NAGG_CLICKHOUSE_PASSWORD
export NAGG_CLICKHOUSE_ADDR="${NAGG_CLICKHOUSE_ADDR:-127.0.0.1:9000}"
export NAGG_API_ADDR="$addr"

echo "starting nagg-api on $addr (clickhouse $NAGG_CLICKHOUSE_ADDR, user '${NAGG_CLICKHOUSE_USERNAME:-default}')" >&2
exec ./bin/nagg-api
