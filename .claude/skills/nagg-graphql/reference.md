# nagg GraphQL Schema Reference

Authoritative vocabulary for the current generic nagg GraphQL API. Source:
`internal/graphqlapi/schema.go` and `internal/clickhouse/read.go`.

## Transport

`POST $NAGG_GRAPHQL_ENDPOINT` with body `{"query":"...","variables":{...}}`.
Default endpoint: `http://127.0.0.1:8080/graphql`. `GET` returns 405. The server
uses a 10 second request context timeout.

## Query Roots

| Root | Args | Returns |
|---|---|---|
| `event` | `id: String!` | `NostrEvent` |
| `events` | `input: EventQueryInput!` | `EventConnection!` |
| `eventContext` | `id: String!`, `limit: Int = 1000` | `EventContext` |
| `aggregateEvents` | `input: EventAggregationInput!` | `AggregationResult!` |

`id` and `pubkeys` filters must be lowercase 64-character hex. Bech32 values
are rejected; decode with `scripts/to-hex.py` before querying.

## NostrEvent

| Field | Type | Meaning |
|---|---|---|
| `id` | String! | event id |
| `pubkey` | String! | author pubkey |
| `kind` | Int! | Nostr kind |
| `createdAt` | DateTime! | event timestamp |
| `content` | String! | raw event content |
| `tags` | [[String!]!]! | raw tag arrays |
| `sig` | String! | event signature |
| `updatedAt` | DateTime! | last-seen/update timestamp |
| `pubkeyEvents(kinds, limit)` | [NostrEvent!]! | latest events by the same pubkey, useful for kind-0 metadata |

`pubkeyEvents.limit` defaults to 1 and is capped at 20. Invalid or zero limits
fall back to 1.

## events(input)

```graphql
input EventQueryInput {
  ids: [String!]
  pubkeys: [String!]
  kinds: [Int!]
  tags: [TagFilterInput!]
  limit: Int = 50
}

input TagFilterInput {
  key: String!
  value: String
  values: [String!]
}
```

Returns `EventConnection { nodes, pageInfo }`. `limit` defaults to 50 and is
capped at 500. Results are ordered by `created_at DESC, id DESC`. `pageInfo` has
`hasNextPage: false` and an `endCursor`; cursor input is not implemented.

Tag filters match events with matching flattened tags. With `value`, the tag
value must equal it. With `values`, the tag value must be in the list. With
neither, only tag-key existence is required.

## eventContext(id, limit)

Fetches the root event and recursively follows events with `e` tags pointing at
the current frontier. It searches up to depth 8 and caps `limit` to the range
1-2000, defaulting invalid values to 1000.

Return shape:

```graphql
type EventContext {
  root: NostrEvent!
  events: [NostrEvent!]!
}
```

This is a generic context helper, not a semantic thread type. Clients decide
whether `e`-tagged events are replies, reactions, zaps, or something else.

## aggregateEvents(input)

```graphql
input EventAggregationInput {
  dataset: String = "EVENTS"  # EVENTS | TAGS | RELAYS
  groupBy: [String!]!
  metrics: [String!]          # default ["COUNT"]
  ids: [String!]
  pubkeys: [String!]
  kinds: [Int!]
  tags: [TagFilterInput!]
  limit: Int = 100            # max 1000; 0 -> 100
}
```

Returns `AggregationResult { rows: [AggregationRow!]! }`, each row containing
`{ dimensions: JSON, metrics: JSON }`. Rows are ordered by the first metric
descending. Output keys are lower-cased versions of requested names:
`KIND` -> `kind`, `UNIQUE_EVENTS` -> `unique_events`.

### Datasets

| dataset | source | supports filters |
|---|---|---|
| `EVENTS` | `nostr_events` | `ids`, `pubkeys`, `kinds`, `tags` |
| `TAGS` | `event_tags` joined to `nostr_events` | `ids`, `pubkeys`, `kinds`, `tags` |
| `RELAYS` | `event_seen_relays` | no event/tag filters |

### Dimension Validity

| dim | EVENTS | TAGS | RELAYS | meaning |
|---|:-:|:-:|:-:|---|
| `DAY` | yes | yes | yes | date bucket |
| `HOUR` | yes | yes | yes | hour bucket |
| `KIND` | yes | yes | no | event kind |
| `PUBKEY` | yes | yes | no | event author pubkey |
| `AUTHOR` | yes | yes | no | alias of `PUBKEY` |
| `EVENT_ID` | yes | yes | yes | event id |
| `CONTENT` | yes | yes | no | event content |
| `TAG_KEY` | no | yes | no | tag name, such as `e`, `p`, `t` |
| `TAG_VALUE` | no | yes | no | tag value |
| `RELAY` | no | no | yes | relay URL |

### Metric Validity

| metric | EVENTS | TAGS | RELAYS | meaning |
|---|:-:|:-:|:-:|---|
| `COUNT` | yes | yes | yes | row count |
| `UNIQUE_EVENTS` | yes | yes | yes | distinct event ids |
| `UNIQUE_PUBKEYS` | yes | yes | no | distinct event authors |
| `UNIQUE_AUTHORS` | yes | yes | no | alias of `UNIQUE_PUBKEYS` |
| `UNIQUE_TAG_VALUES` | no | yes | no | distinct tag values |
| `UNIQUE_RELAYS` | no | no | yes | distinct relays |

A dimension or metric outside the valid dataset column returns a GraphQL error.

## Recipes For App-Level Questions

The API does not encode app-level interpretations. Use these recipes instead.

| Question | Query recipe |
|---|---|
| Reaction count/breakdown for event | `aggregateEvents(dataset:"TAGS", kinds:[7], tags:[{key:"e", value:"<event-id>"}], groupBy:["CONTENT"], metrics:["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"])` |
| Reaction events for event | `events(input:{kinds:[7], tags:[{key:"e", value:"<event-id>"}]})` |
| Reply/comment events for event | `events(input:{kinds:[1,1111], tags:[{key:"e", value:"<event-id>"}]})` |
| Associated events by kind | `aggregateEvents(dataset:"TAGS", tags:[{key:"e", value:"<event-id>"}], groupBy:["KIND"], metrics:["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"])` |
| Contact-list events referencing pubkey | `events(input:{kinds:[3], tags:[{key:"p", value:"<pubkey>"}]})` |
| Profile metadata for pubkey | `events(input:{pubkeys:["<pubkey>"], kinds:[0], limit:1})` |
| Profile metadata next to event | query `pubkeyEvents(kinds:[0], limit:1)` on a returned `NostrEvent` |
| Zap receipt events for event | `events(input:{kinds:[9735], tags:[{key:"e", value:"<event-id>"}]})` |

## Limits

- No server-side social fields: app concepts are query recipes.
- No full-text search.
- No `since`, `until`, or arbitrary WHERE filters in GraphQL inputs.
- No parsed profile metadata type; parse kind-0 `content` client-side.
- No parsed zap amount metrics; query raw kind-9735 events/tags.
- No real cursor pagination yet; `hasNextPage` is always false.
