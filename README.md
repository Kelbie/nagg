# Nagg

Nagg is a Nostr AppView aggregator. The first milestone is a Go ingester that subscribes to configured Nostr relays, validates events, and stores raw events, relay provenance, and flattened tags in ClickHouse.

## Prerequisites

### ClickHouse

Run a ClickHouse instance using the [official Docker image](https://hub.docker.com/_/clickhouse):

```sh
docker run -d \
  --name nagg-db \
  -e CLICKHOUSE_USER=nagg \
  -e CLICKHOUSE_PASSWORD=nagg_secret \
  -e CLICKHOUSE_DB=default \
  --ulimit nofile=262144:262144 \
  -p 8123:8123 \
  -p 9000:9000 \
  clickhouse/clickhouse-server:latest
```

## Run The Ingester

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/ingester
```

Useful configuration:

```sh
NAGG_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band
NAGG_KINDS=0,1,3,6,7,16,9735
NAGG_SINCE=24h
NAGG_BATCH_SIZE=1000
NAGG_FLUSH_INTERVAL=5s
NAGG_VERIFY_EVENTS=true
```

The default `NAGG_KINDS` is `0,1,3,6,7,16,9735`, which covers profiles, notes, contact lists, reposts, reactions, generic reposts, and zaps for the app-view API. Set `NAGG_KINDS` explicitly when you need a different relay subscription. Set `NAGG_SINCE=0` to omit the `since` filter.

## Backfill The App-View Tables

ClickHouse materialized views only populate from inserts after migration. Run the app-view backfill once after creating the schema or after changing app-view aggregates:

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/backfill
```

The command is idempotent: it truncates and rebuilds app-view counter/profile tables from `nostr_events` and `event_tags`.

## Run The API

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/api
```

The API listens on `:8080` by default and serves `POST /graphql`, `GET /healthz`, and the app-view REST routes under `/nostr/*`. Set `NAGG_API_ADDR=:9090` to change the bind address. If `NAGG_API_ADDR` is unset and Railway provides `PORT`, the API listens on that port.

The Vertex DVM proxy routes (`/nostr/profile`, `/nostr/search`, `/nostr/recommended`) require a funded/authorized 64-hex `NAGG_VERTEX_PRIVATE_KEY`. If the key is empty they return `503`; if Vertex rejects the key it returns the DVM kind-7000 error as a `502`.

```sh
NAGG_VERTEX_PRIVATE_KEY=<64-hex-secret> \
NAGG_VERTEX_RELAY=wss://relay.vertexlab.io \
NAGG_NIP05_VALIDATE=true \
go run ./cmd/api
```

App-view smoke checks:

```sh
curl http://127.0.0.1:8080/healthz
curl 'http://127.0.0.1:8080/nostr/feed?kind=trending&limit=20'
curl -X POST http://127.0.0.1:8080/nostr/notes/stats \
  -H 'content-type: application/json' \
  -d '{"ids":[]}'
```

## Deploy On Railway

This repo includes a `Dockerfile` and `railway.toml` for the API service. Railway builds the Dockerfile, runs `./nagg-migrate` as the pre-deploy command, starts `./nagg-api`, and checks `GET /healthz` before making the deployment active.

Required service variables:

```sh
NAGG_CLICKHOUSE_ADDR=<clickhouse-private-host>:9000
NAGG_CLICKHOUSE_DATABASE=default
NAGG_CLICKHOUSE_USERNAME=<clickhouse-user>
NAGG_CLICKHOUSE_PASSWORD=<clickhouse-password>
```

Optional service variables:

```sh
NAGG_RELAYS=wss://relay.damus.io,wss://nos.lol,wss://relay.nostr.band
NAGG_KINDS=0,1,3,6,7,16,9735
NAGG_VERTEX_PRIVATE_KEY=<64-hex-secret>
NAGG_VERTEX_RELAY=wss://relay.vertexlab.io
NAGG_NIP05_VALIDATE=true
```

Do not set `PORT` yourself on Railway; Railway injects it for the web service. Set `NAGG_API_ADDR` only when you intentionally want to override the bind address outside Railway.

The pre-deploy command only migrates schemas. After first deploy, or after changing app-view aggregate logic, run the backfill command once from the Railway shell or a one-off command:

```sh
./nagg-backfill
```

If you want the ingester as a separate Railway service, deploy the same image and override the start command to:

```sh
./nagg-ingester
```

Run the example GraphQL client:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql go run ./cmd/graphql-client
```

The read layer exposes raw Nostr events plus constrained generic aggregations over `EVENTS`, `TAGS`, and `RELAYS`. App concepts such as reactions, replies, profiles, follows, and zaps are expressed as client query recipes using `kinds` and tag filters rather than hard-coded GraphQL fields. When you need a server-side join, the API stays generic by exposing more raw events through primitive relations like `pubkeyEvents(kinds: [0], limit: 1)`.

If the ClickHouse container is not published to localhost, run API/client containers in the `nagg-db` network namespace:

```sh
docker run --rm --network container:nagg-db \
  -v "$PWD/bin/nagg-api-linux-arm64:/nagg-api:ro" \
  -e NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
  -e NAGG_CLICKHOUSE_USERNAME=nagg \
  -e NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
  alpine:3.20 /nagg-api
```

## Ask In Plain Language

The `nagg-graphql` skill (in `.claude/skills/nagg-graphql/`) turns plain-language questions into GraphQL queries and runs them against `$NAGG_GRAPHQL_ENDPOINT`. For example:

- "How many likes and reposts does `nevent1…` have?"
- "How many followers does `npub1…` have, and what's their nip05?"
- "Show the 10 newest comments in the thread rooted at `nevent1…`."
- "What are the top event kinds on the relay?"
- "Which events got the most reactions?"

## Backfill One Thread

Fetch a root event from configured Nostr relays, recursively crawl associated `e`-tagged events, and backfill kind-0 profiles for all discovered pubkeys:

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_THREAD_RELAY=wss://relay.damus.io \
NAGG_THREAD_EXTRA_RELAYS=wss://nos.lol,wss://relay.nostr.band \
go run ./cmd/thread-crawler nevent1...
```

Then demonstrate that GraphQL can resolve a display-ready thread summary using only raw events plus same-`pubkey` lookups for kind `0` metadata:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql \
go run ./cmd/thread-demo <root-event-id>
```

For a cleaner text UI that renders the thread tree with usernames, profile image URLs, comment counts, and like counts, use:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql \
go run ./cmd/thread-cli <root-event-id>
```
