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
