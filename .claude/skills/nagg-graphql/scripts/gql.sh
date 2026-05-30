#!/usr/bin/env bash
# POST a GraphQL query to a nagg API (local OR remote), pretty-print, surface errors.
#
# Usage:
#   gql.sh '<graphql query>'      # query as a single argument
#   echo '<query>' | gql.sh       # or piped on stdin
#
# Environment:
#   NAGG_GRAPHQL_ENDPOINT  target URL (default http://127.0.0.1:8080/graphql).
#                          Any host works — set to https://relay.example/graphql for remote.
set -euo pipefail

endpoint="${NAGG_GRAPHQL_ENDPOINT:-http://127.0.0.1:8080/graphql}"
query="${1:-$(cat)}"

if [ -z "${query// }" ]; then
  echo "usage: gql.sh '<graphql query>'  (or pipe the query on stdin)" >&2
  exit 2
fi

# Is the endpoint local? Picks the right "can't connect" hint.
local_endpoint=0
if [[ "$endpoint" =~ ^[a-zA-Z]+://(127\.0\.0\.1|localhost|0\.0\.0\.0|\[::1\])(:|/|$) ]]; then
  local_endpoint=1
fi

print_unreachable() {
  echo "ERROR: could not reach the nagg API at $endpoint" >&2
  if [ "$local_endpoint" = 1 ]; then
    echo "Start it locally with:" >&2
    echo "  NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 NAGG_CLICKHOUSE_USERNAME=nagg \\" >&2
    echo "  NAGG_CLICKHOUSE_PASSWORD=nagg_secret go run ./cmd/api" >&2
  else
    echo "Check the URL is correct and the server is up, reachable, and TLS-valid." >&2
  fi
}

# Build the JSON body with jq so quoting/newlines in the query are safe.
body="$(jq -n --arg q "$query" '{query: $q}')"

# Capture body + HTTP status. Transport failure -> non-zero exit -> unreachable.
if ! raw="$(curl -sS --max-time 15 -w $'\n%{http_code}' \
  -H 'content-type: application/json' -d "$body" "$endpoint" 2>/dev/null)"; then
  print_unreachable
  exit 1
fi

code="${raw##*$'\n'}"
resp="${raw%$'\n'*}"

case "$code" in
  2??) : ;;
  000)
    print_unreachable
    exit 1 ;;
  *)
    echo "ERROR: HTTP $code from $endpoint" >&2
    [ -n "${resp// }" ] && printf '%s\n' "$resp" >&2
    exit 1 ;;
esac

# Surface GraphQL-level errors prominently (HTTP 200 can still carry errors).
if printf '%s' "$resp" | jq -e 'has("errors") and (.errors != null)' >/dev/null 2>&1; then
  echo "GraphQL errors:" >&2
  printf '%s' "$resp" | jq '.errors' >&2
fi

printf '%s' "$resp" | jq .
