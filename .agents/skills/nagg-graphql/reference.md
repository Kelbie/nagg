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
| `rankedEvents` | `input: RankedEventsInput!` | `EventConnection!` |
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
| `references(input)` | EventConnection! | events named by this event's tags, commonly `e` or `q` |
| `referencedBy(input)` | EventConnection! | events whose selected tag points at this event, author, or address |
| `aggregateReferencedBy(input)` | AggregationResult! | generic metrics over events that point at this event, author, or address |

`pubkeyEvents.limit` defaults to 1 and is capped at 20. Invalid or zero limits
fall back to 1.

## events(input)

```graphql
input EventQueryInput {
  ids: [String!]
  pubkeys: [String!]
  kinds: [Int!]
  tags: [TagFilterInput!]
  since: Int
  until: Int
  limit: Int = 50
  offset: Int
}

input TagFilterInput {
  key: String!
  value: String
  values: [String!]
  marker: String
  index: Int
}
```

Returns `EventConnection { nodes, pageInfo }`. `limit` defaults to 50 and is
capped at 500. `since` is inclusive, `until` is exclusive, and both are Unix
seconds. `offset` is an integer row offset. Results are ordered by
`created_at DESC, id DESC`. `pageInfo` has `hasNextPage: false` and an
`endCursor`; cursor input is not implemented.

Tag filters match events with matching flattened tags. With `value`, the tag
value must equal it. With `values`, the tag value must be in the list. With
neither, only tag-key existence is required. `marker` and `index` are exposed
for generic clients; current ClickHouse filtering still matches the flattened
tag key/value representation.

## Event References

`references` resolves event ids already present in the source event's tags. It
defaults to `e` and `q` tags and only resolves 64-character hex ids.

```graphql
input EventReferenceInput {
  tags: [TagFilterInput!]
  limit: Int = 20
}
```

Example:

```graphql
query {
  event(id:"<event-id>") {
    references(input:{tags:[{key:"q"}], limit:1}) {
      nodes { id pubkey kind content }
    }
  }
}
```

`referencedBy` queries events whose selected tag points at the current event.
`target` defaults to `EVENT_ID`; `PUBKEY` targets the event author and `ADDRESS`
targets addressable event coordinates such as `30023:<pubkey>:<d-tag>`.

```graphql
input ReverseEventReferenceInput {
  events: EventQueryInput
  via: TagFilterInput!
  target: String = "EVENT_ID"
  limit: Int = 50
  offset: Int
}
```

Example:

```graphql
query {
  event(id:"<event-id>") {
    referencedBy(input:{via:{key:"e"}, events:{kinds:[1,1111], limit:20}}) {
      nodes { id pubkey kind content }
    }
  }
}
```

`aggregateReferencedBy` runs generic metrics over the same reverse-reference
query. It returns `AggregationResult { rows { dimensions metrics } }`.

```graphql
input ReverseEventReferenceAggregateInput {
  events: EventQueryInput
  via: TagFilterInput!
  target: String = "EVENT_ID"
  groupBy: [GenericDimensionInput!]
  metrics: [GenericMetricInput!]
  limit: Int = 500
  first: Int = 100
  orderBy: String
}

input GenericDimensionInput {
  name: String!
  field: String
  tagKey: String
  tagIndex: Int = 1
  derived: String
}

input GenericMetricInput {
  name: String!
  op: String!
  field: String
  tagKey: String
  tagIndex: Int = 1
  derived: String
  distinctField: String
}
```

Supported metric operations are `COUNT`, `COUNT_DISTINCT`, `SUM`, `AVG`, `MIN`,
and `MAX`. Selectors support event fields `ID`, `EVENT_ID`, `PUBKEY`, `AUTHOR`,
`KIND`, `CREATED_AT`, `CONTENT`; tag selectors with `tagKey`/`tagIndex`; and
derived zap selectors `nip57.amount_msat`, `nip57.amount_sats`, and
`nip57.sender_pubkey`.

Examples:

```graphql
query {
  event(id:"<event-id>") {
    likes: aggregateReferencedBy(input:{
      via:{key:"e"}
      events:{kinds:[7], limit:500}
      metrics:[{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]
    }) {
      rows { metrics }
    }
    topZappers: aggregateReferencedBy(input:{
      via:{key:"e"}
      events:{kinds:[9735], limit:500}
      groupBy:[{name:"pubkey", derived:"nip57.sender_pubkey"}]
      metrics:[{name:"sats", op:"SUM", derived:"nip57.amount_sats"}]
      first:3
      orderBy:"sats"
    }) {
      rows { dimensions metrics }
    }
  }
}
```

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
  since: Int
  until: Int
  limit: Int = 100            # max 1000; 0 -> 100
}
```

Returns `AggregationResult { rows: [AggregationRow!]! }`, each row containing
`{ dimensions: JSON, metrics: JSON }`. Rows are ordered by the first metric
descending. `since` is inclusive and `until` is exclusive over the source
event/tag/relay timestamp. Output keys are lower-cased versions of requested
names: `KIND` -> `kind`, `UNIQUE_EVENTS` -> `unique_events`.

## rankedEvents(input)

`rankedEvents` ranks target events by aggregating source events that reference
them through a tag. It is generic: likes are source events `kind:7` via `e`;
reposts are source events `kind:6/16` via `e`; quote rankings can use `q`.

```graphql
input RankedEventsInput {
  references: EventQueryInput!
  via: TagFilterInput!
  target: EventQueryInput
  metric: GenericMetricInput
  limit: Int = 30
  offset: Int
}
```

The resolver aggregates `references` over the flattened tag rows matching
`via.key`, groups by the referenced tag value, orders by the requested metric,
hydrates those tag values as event ids, applies optional `target` filters, and
returns the target events in aggregate rank order. Supported rank metrics map to
the generic aggregate metrics: `COUNT`, `COUNT_DISTINCT PUBKEY/AUTHOR`, and
`COUNT_DISTINCT ID/EVENT_ID`. The default is distinct pubkeys.

Top notes liked during the last 24 hours, while each returned note can still ask
for all-time like counts through `aggregateReferencedBy`:

```graphql
query($since: Int!) {
  rankedEvents(input: {
    references: { kinds: [7], since: $since }
    via: { key: "e" }
    target: { kinds: [1] }
    metric: { name: "likers", op: "COUNT_DISTINCT", distinctField: "PUBKEY" }
    limit: 20
  }) {
    nodes {
      id
      pubkey
      kind
      createdAt
      content
      likes: aggregateReferencedBy(input: {
        via: { key: "e" }
        events: { kinds: [7], limit: 500 }
        metrics: [{ name: "allTimeLikers", op: "COUNT_DISTINCT", distinctField: "PUBKEY" }]
      }) {
        rows { metrics }
      }
    }
  }
}
```

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
| Quoted event for note | `event(id:"<event-id>") { references(input:{tags:[{key:"q"}], limit:1}) { nodes { id content } } }` |
| Reverse references for note | `event(id:"<event-id>") { referencedBy(input:{via:{key:"e"}, events:{kinds:[1,7,9735]}}) { nodes { id kind pubkey } } }` |
| Distinct likers for note | `event(id:"<event-id>") { aggregateReferencedBy(input:{via:{key:"e"}, events:{kinds:[7]}, metrics:[{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]}) { rows { metrics } } }` |
| Top notes by likes in last 24h | `rankedEvents(input:{references:{kinds:[7], since:<24h-ago>}, via:{key:"e"}, target:{kinds:[1]}, metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}})` |
| Top zappers for note | `event(id:"<event-id>") { aggregateReferencedBy(input:{via:{key:"e"}, events:{kinds:[9735]}, groupBy:[{name:"pubkey", derived:"nip57.sender_pubkey"}], metrics:[{name:"sats", op:"SUM", derived:"nip57.amount_sats"}], first:3, orderBy:"sats"}) { rows { dimensions metrics } } }` |
| Associated events by kind | `aggregateEvents(dataset:"TAGS", tags:[{key:"e", value:"<event-id>"}], groupBy:["KIND"], metrics:["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"])` |
| Contact-list events referencing pubkey | `events(input:{kinds:[3], tags:[{key:"p", value:"<pubkey>"}]})` |
| Profile metadata for pubkey | `events(input:{pubkeys:["<pubkey>"], kinds:[0], limit:1})` |
| Profile metadata next to event | query `pubkeyEvents(kinds:[0], limit:1)` on a returned `NostrEvent` |
| Zap receipt events for event | `events(input:{kinds:[9735], tags:[{key:"e", value:"<event-id>"}]})` |

## Limits

- No server-side social fields: app concepts are query recipes.
- No full-text search.
- No arbitrary WHERE filters beyond ids, pubkeys, kinds, tags, time bounds,
  limit, and offset.
- No parsed profile metadata type; parse kind-0 `content` client-side.
- Parsed zap amount/sender values are only available as derived selectors in
  `aggregateReferencedBy`; raw kind-9735 events/tags remain available through
  `events`.
- No real cursor pagination yet; `hasNextPage` is always false.
