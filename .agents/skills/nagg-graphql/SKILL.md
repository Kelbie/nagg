---
name: nagg-graphql
description: Use when exploring a Nostr relay's data or asking plain-language questions about nostr events, notes, profiles, reactions, replies, follows, tags, relays, or aggregate relay activity served by the nagg GraphQL API.
---

# nagg GraphQL Explorer

Plain-language interface to the nagg Nostr-relay GraphQL API. The API is
generic: it exposes raw Nostr events, event references, reverse references, and
constrained event/tag/relay aggregations. Social concepts such as reactions,
replies, follows, profile metadata, and zaps are recipes over event kinds, tags,
and generic aggregations, not first-class schema fields.

## Setup

- Endpoint: `$NAGG_GRAPHQL_ENDPOINT` (default `http://127.0.0.1:8080/graphql`).
- Run every query with the helper: `scripts/gql.sh '<query>'`.
- Remote servers work by setting the endpoint:
  `NAGG_GRAPHQL_ENDPOINT=https://relay.example/graphql scripts/gql.sh '<query>'`.
- The API is a public, unauthenticated `POST /graphql` endpoint.
- If the API is unreachable, `gql.sh` prints a local start hint or remote
  connectivity checklist.

## Workflow

1. Normalize identifiers. If the user gives `npub`/`note`/`nevent`/`nprofile`,
   convert first: `scripts/to-hex.py <value>`. The API accepts lowercase 64-char
   hex for ids and pubkeys.
2. Pick one root query or recipe from the table below.
3. Run it with `gql.sh`.
4. Answer concisely with the result and the query shape used. Offer raw JSON on
   request.
5. If unsupported, say what is missing and offer the closest generic recipe.

## Query Roots

| Question is about... | Use | Key fields |
|---|---|---|
| one event by id | `event(id)` | `id`, `pubkey`, `kind`, `createdAt`, `content`, `tags`, `sig`, `updatedAt`, `pubkeyEvents`, `references`, `referencedBy`, `aggregateReferencedBy` |
| filtered raw events | `events(input)` | `nodes`, `pageInfo` |
| events ranked by references | `rankedEvents(input)` | `nodes`, `pageInfo` |
| recursive event context | `eventContext(id)` | `root`, `events` |
| aggregate stats | `aggregateEvents(input)` | `dataset`, `groupBy`, `metrics`, `rows` |

Minimal examples:

```graphql
query { event(id:"<hex>") { id pubkey kind createdAt content tags } }
query { events(input:{ kinds:[1], pubkeys:["<hex>"], since:1710000000, limit:10 }) { nodes { id content createdAt } } }
query { eventContext(id:"<hex>", limit:100) { root { id content } events { id kind pubkey tags } } }
query { aggregateEvents(input:{ dataset:"EVENTS", groupBy:["KIND"], metrics:["UNIQUE_EVENTS"], limit:10 }) { rows { dimensions metrics } } }
query { event(id:"<hex>") { references(input:{tags:[{key:"q"}]}) { nodes { id content } } } }
query { event(id:"<hex>") { aggregateReferencedBy(input:{via:{key:"e"}, events:{kinds:[7]}, metrics:[{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]}) { rows { metrics } } } }
query { rankedEvents(input:{references:{kinds:[7], since:1710000000}, via:{key:"e"}, target:{kinds:[1]}, metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}, limit:10}) { nodes { id content } } }
```

## Common Recipes

| Want | Query shape |
|---|---|
| Top event kinds | `aggregateEvents(dataset:"EVENTS", groupBy:["KIND"], metrics:["UNIQUE_EVENTS"])` |
| Busiest hours | `aggregateEvents(dataset:"EVENTS", groupBy:["HOUR"], metrics:["COUNT"])` |
| Most-active authors | `aggregateEvents(dataset:"EVENTS", groupBy:["AUTHOR"], metrics:["UNIQUE_EVENTS"], kinds:[1])` |
| Reaction breakdown for a target | `aggregateEvents(dataset:"TAGS", kinds:[7], tags:[{key:"e", value:"<event-id>"}], groupBy:["CONTENT"], metrics:["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"])` |
| Events reacting to a target | `events(input:{kinds:[7], tags:[{key:"e", value:"<event-id>"}]})` |
| Replies/comments for a target | `events(input:{kinds:[1,1111], tags:[{key:"e", value:"<event-id>"}]})` |
| Follower/contact-list events | `events(input:{kinds:[3], tags:[{key:"p", value:"<pubkey>"}]})` |
| Latest profile metadata for an event author | `event(id:"<event-id>") { pubkeyEvents(kinds:[0], limit:1) { content createdAt } }` |
| Quoted event for a note | `event(id:"<event-id>") { references(input:{tags:[{key:"q"}], limit:1}) { nodes { id content } } }` |
| Events referencing a target | `event(id:"<event-id>") { referencedBy(input:{via:{key:"e"}, events:{kinds:[1,7,9735]}}) { nodes { id kind pubkey } } }` |
| Distinct likers for a target | `event(id:"<event-id>") { aggregateReferencedBy(input:{via:{key:"e"}, events:{kinds:[7]}, metrics:[{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]}) { rows { metrics } } }` |
| Zap sats for a target | `event(id:"<event-id>") { aggregateReferencedBy(input:{via:{key:"e"}, events:{kinds:[9735]}, metrics:[{name:"sats", op:"SUM", derived:"nip57.amount_sats"}]}) { rows { metrics } } }` |
| Zap receipt events/tags | `events(input:{kinds:[9735], tags:[{key:"e", value:"<event-id>"}]})` |
| Top notes by recent likes | `rankedEvents(input:{references:{kinds:[7], since:<24h-ago>}, via:{key:"e"}, target:{kinds:[1]}, metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}})` |
| Relay distribution | `aggregateEvents(dataset:"RELAYS", groupBy:["RELAY"], metrics:["UNIQUE_EVENTS"])` |

## aggregateEvents Cheat Sheet

`dataset` is one of `EVENTS | TAGS | RELAYS`. `groupBy` needs at least one
dimension. `metrics` defaults to `["COUNT"]`. Rows are ordered by the first
metric descending. Result JSON keys are lower-cased, for example
`UNIQUE_EVENTS` becomes `unique_events`.

Common dimensions:

| Dimension | Datasets |
|---|---|
| `DAY`, `HOUR`, `EVENT_ID` | `EVENTS`, `TAGS`, `RELAYS` |
| `KIND`, `PUBKEY`, `AUTHOR`, `CONTENT` | `EVENTS`, `TAGS` |
| `TAG_KEY`, `TAG_VALUE` | `TAGS` |
| `RELAY` | `RELAYS` |

Common metrics:

| Metric | Datasets |
|---|---|
| `COUNT`, `UNIQUE_EVENTS` | `EVENTS`, `TAGS`, `RELAYS` |
| `UNIQUE_PUBKEYS`, `UNIQUE_AUTHORS` | `EVENTS`, `TAGS` |
| `UNIQUE_TAG_VALUES` | `TAGS` |
| `UNIQUE_RELAYS` | `RELAYS` |

## Don'ts

- Do not query typed app-level engagement or social graph fields unless the
  schema exposes them. Prefer recipes over raw events, `references`,
  `referencedBy`, `aggregateReferencedBy`, and `aggregateEvents`.
- Do not use removed roots for profiles or discussions. Query kind-0 events for
  profile metadata and kind-1/1111 `e`-tagged events for replies.
- Do not use non-existent `REACTIONS` or `REPLIES` datasets. Use `TAGS` with
  `kinds` and tag filters.
- Do not invent full-text search. The current GraphQL inputs do not expose text
  search.
- `events.first` does not exist. Use `events(input:{limit:N})`.
- Pagination is not complete: `pageInfo.hasNextPage` is always false.

Full schema vocabulary and dataset validity live in `reference.md`.
