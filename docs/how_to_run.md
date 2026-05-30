You can test it two ways: run the bundled example client, or send raw GraphQL with `curl`.

Because your `nagg-db` ClickHouse container is not published to localhost right now, the easiest verified path is the Docker-network version.

**1. Build API + Client**

```sh
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/nagg-api-linux-arm64 ./cmd/api
CGO_ENABLED=0 GOOS=linux GOARCH=arm64 go build -o bin/nagg-graphql-client-linux-arm64 ./cmd/graphql-client
```

**2. Start The API**

In one terminal:

```sh
docker run --rm --network container:nagg-db \
  -v "$PWD/bin/nagg-api-linux-arm64:/nagg-api:ro" \
  -e NAGG_CLICKHOUSE_ADDR=127.0.0.1:9000 \
  clickhouse:lts-jammy /nagg-api
```

You should see:

```text
graphql api listening addr=:8080
```

**3. Run The Example Client**

In another terminal:

```sh
docker run --rm --network container:nagg-db \
  -v "$PWD/bin/nagg-graphql-client-linux-arm64:/nagg-client:ro" \
  -e NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql \
  clickhouse:lts-jammy /nagg-client
```

The client demonstrates:

- events per kind
- tag key distribution
- "reactions" as kind `7` events with `e` tags, grouped by `TAG_VALUE` and `CONTENT`
- "followers" as kind `3` events with `p` tags, grouped by `TAG_VALUE`
- "comments" as kind `1` / `1111` events with `e` tags
- fetching full raw events matching those generic filters

**Raw `curl` Examples**

Tag-key distribution:

```sh
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  -d '{"query":"query { aggregateEvents(input: { dataset: \"TAGS\", groupBy: [\"TAG_KEY\"], metrics: [\"COUNT\", \"UNIQUE_EVENTS\"], limit: 5 }) { rows { dimensions metrics } } }"}' \
  | jq
```

Reaction-like recipe, without a `likes` field:

```sh
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  -d '{"query":"query { aggregateEvents(input: { dataset: \"TAGS\", kinds: [7], tags: [{key: \"e\"}], groupBy: [\"TAG_VALUE\", \"CONTENT\"], metrics: [\"UNIQUE_EVENTS\", \"UNIQUE_PUBKEYS\"], limit: 5 }) { rows { dimensions metrics } } }"}' \
  | jq
```

Follower-like recipe, without a `followers` field:

```sh
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  -d '{"query":"query { aggregateEvents(input: { dataset: \"TAGS\", kinds: [3], tags: [{key: \"p\"}], groupBy: [\"TAG_VALUE\"], metrics: [\"UNIQUE_PUBKEYS\", \"COUNT\"], limit: 5 }) { rows { dimensions metrics } } }"}' \
  | jq
```

If you publish ClickHouse/API ports locally, the non-Docker flow is simply:

```sh
go run ./cmd/api
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql go run ./cmd/graphql-client
```
