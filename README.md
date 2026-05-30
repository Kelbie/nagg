# Nagg

Nagg is a Nostr AppView aggregator. The first milestone is a Go ingester that subscribes to configured Nostr relays, validates events, and stores raw events, relay provenance, and flattened tags in ClickHouse.

## Run The Ingester

```sh
NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 go run ./cmd/ingester
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
