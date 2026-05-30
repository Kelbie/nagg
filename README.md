# Nagg

Nagg is a Nostr AppView aggregator. The first milestone is a Go ingester that subscribes to configured Nostr relays, validates events, and stores raw events, relay provenance, and flattened tags in ClickHouse.

## Prerequisites

### ClickHouse

Run a ClickHouse instance using the [official Docker image](https://hub.docker.com/_/clickhouse):

```sh
docker run -d \
  --name nagg-db \
  --ulimit nofile=262144:262144 \
  -p 8123:8123 \
  -p 9000:9000 \
  clickhouse/clickhouse-server:lts-jammy
```

Create the `nagg` user:

```sh
docker exec nagg-db clickhouse-client --query "
  CREATE USER IF NOT EXISTS nagg IDENTIFIED BY 'nagg_secret';
  GRANT ALL ON default.* TO nagg;
"
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
NAGG_RELAYS=wss://relay.damus.io,wss://relay.primal.net,wss://nos.lol
NAGG_KINDS=0,1,3,5,6,7,9735,10002
NAGG_SINCE=24h
NAGG_BATCH_SIZE=1000
NAGG_FLUSH_INTERVAL=5s
NAGG_VERIFY_EVENTS=true
```

Leave `NAGG_KINDS` empty to request all event kinds. Set `NAGG_SINCE=0` to omit the `since` filter.

## Run The GraphQL API

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_CLICKHOUSE_USERNAME=nagg \
NAGG_CLICKHOUSE_PASSWORD=nagg_secret \
go run ./cmd/api
```

The API listens on `:8080` by default and serves `POST /graphql`. Set `NAGG_API_ADDR=:9090` to change the bind address.

Run the example GraphQL client:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql go run ./cmd/graphql-client
```

The read layer exposes raw Nostr events plus constrained generic aggregations over `EVENTS`, `TAGS`, and `RELAYS`. App concepts such as reactions, replies, profiles, follows, and zaps are expressed as client query recipes using `kinds` and tag filters rather than hard-coded GraphQL fields.

If the ClickHouse container is not published to localhost, run API/client containers in the `nagg-db` network namespace:

```sh
docker run --rm --network container:nagg-db \
  -v "$PWD/bin/nagg-api-linux-arm64:/nagg-api:ro" \
  -e NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
  clickhouse:lts-jammy /nagg-api
```

## Backfill One Thread

Fetch a root event, recursively crawl associated `e`-tagged events, fetch zaps from Primal's cache API, and backfill kind-0 profiles for all discovered pubkeys:

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
NAGG_THREAD_EXTRA_RELAYS=wss://nos.lol \
go run ./cmd/thread-crawler nevent1...
```

Then demonstrate that GraphQL can resolve a display-ready thread summary:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql \
go run ./cmd/thread-demo <root-event-id>
```

For a cleaner text UI that renders the thread tree with usernames, profile image URLs, comment counts, and like counts, use:

```sh
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql \
go run ./cmd/thread-cli <root-event-id>
```
