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

This currently returns real rows like:

```json
{
  "dimensions": { "kind": "21059" },
  "metrics": { "unique_events": 3379 }
}
```

and tag aggregation rows like:

```json
{
  "dimensions": { "tag_key": "p" },
  "metrics": { "count": 7813, "unique_events": 4450 }
}
```

It also demonstrates typed engagement for a real referenced event:

```json
{
  "event": {
    "id": "b58b6c0ec7593bc97f28b21c0c3912db4fda72ceb6b3c16e3ea0390c57a9a3f4",
    "commentCount": 2,
    "reactionsByContent": [{ "content": "🤙", "count": 2 }],
    "thread": { "directReplies": 2, "participants": 2 }
  }
}
```

**Raw `curl` Example**

If you shell into a container sharing the DB network, or expose the API on localhost, you can query directly:

```sh
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  -d '{"query":"query { aggregateEvents(input: { dataset: \"TAGS\", groupBy: [\"TAG_KEY\"], metrics: [\"COUNT\", \"UNIQUE_EVENTS\"], limit: 5 }) { rows { dimensions metrics } } }"}' \
  | jq
```

Another useful one:

```sh
curl -s http://127.0.0.1:8080/graphql \
  -H 'content-type: application/json' \
  -d '{"query":"query { aggregateEvents(input: { dataset: \"REACTIONS\", groupBy: [\"TARGET_EVENT\", \"REACTION\"], metrics: [\"UNIQUE_EVENTS\"], limit: 5 }) { rows { dimensions metrics } } }"}' \
  | jq
```

If you publish ClickHouse/API ports locally, the non-Docker flow is simply:

```sh
go run ./cmd/api
NAGG_GRAPHQL_ENDPOINT=http://127.0.0.1:8080/graphql go run ./cmd/graphql-client
```
