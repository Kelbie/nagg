---
name: nagg-graphql
description: Use when exploring a Nostr relay's data or asking plain-language questions about nostr events, notes, profiles, or relay activity served by the nagg GraphQL API — e.g. how many likes/reposts/replies a note has, reaction/emoji breakdowns, a profile's followers/following, thread participants, or aggregate stats like top event kinds, most-active authors, busiest hours, or most-reacted events.
---

# nagg GraphQL Explorer

Plain-language interface to the nagg Nostr-relay GraphQL API. Translate the
user's question into ONE of the four GraphQL queries and run it. **Never invent
fields** — if the schema can't express it, say so and offer the closest query.

## Setup

- Endpoint: `$NAGG_GRAPHQL_ENDPOINT` (default `http://127.0.0.1:8080/graphql`).
- Run every query with the helper: `scripts/gql.sh '<query>'` — it builds the
  JSON body, pretty-prints the result, and surfaces GraphQL `errors`.
- **Works against any nagg server, local or remote** — just set the endpoint:
  `NAGG_GRAPHQL_ENDPOINT=https://relay.example/graphql scripts/gql.sh '<query>'`.
  HTTPS works automatically. The API is a public, unauthenticated `POST /graphql`
  (no token/header needed).
- If the API is unreachable, `gql.sh` prints a hint: the local start command for
  a localhost endpoint, or a connectivity checklist for a remote one. (Starting
  it locally needs ClickHouse env vars — see the repo README.)

## Workflow

1. **Normalize identifiers.** If the user gives an `npub`/`note`/`nevent`/`nprofile`,
   convert first: `scripts/to-hex.py <value>`. The API only accepts lowercase
   64-char hex and rejects bech32.
2. **Pick the query** (table below) or build an `aggregateEvents` input.
3. **Run it** with `gql.sh`.
4. **Answer concisely:** one sentence + a small table. Resolve pubkeys to names
   via `profile { metadata { name } }` when it aids readability. Offer raw JSON
   on request.
5. **If unsupported** (time windows, text search, zap amounts, arbitrary
   filters): say what's missing and offer the closest query. See "Limits" in
   `reference.md`.

## The four queries

| Question is about… | Use | Key fields |
|---|---|---|
| one note's engagement | `event(id)` | `likes`, `reposts`, `commentCount`, `reactionsByContent`, `likers`, `reposters` |
| a user | `profile(pubkey)` | `metadata`, `followers`, `following`, `followerList`, `followingList` |
| a discussion | `thread(rootEventId)` | `directReplies`, `participants`, `comments(sort)` |
| stats across the relay | `aggregateEvents(input)` | `dataset` + `groupBy` + `metrics` |

Minimal examples (pass to `gql.sh`):

```graphql
query { event(id:"<hex>") { likes reposts commentCount reactionsByContent { content count } } }
query { profile(pubkey:"<hex>") { metadata { name } followers following } }
query { thread(rootEventId:"<hex>") { directReplies participants comments(first:10){ edges{ node{ content author{ metadata{ name } } } } } } }
query { aggregateEvents(input:{ dataset:"EVENTS", groupBy:["KIND"], metrics:["UNIQUE_EVENTS"], limit:10 }) { rows{ dimensions metrics } } }
```

## aggregateEvents cheat-sheet

`dataset` ∈ `EVENTS | TAGS | REACTIONS | REPLIES`. `groupBy` needs ≥1 dim;
`metrics` defaults to `["COUNT"]`; rows are ordered by the first metric DESC.
**Validity differs per dataset — check the matrix in `reference.md` before
guessing dims/metrics, or the API returns an error.** Common recipes:

| Want | dataset | groupBy | metrics |
|---|---|---|---|
| Top event kinds | `EVENTS` | `[KIND]` | `[UNIQUE_EVENTS]` |
| Busiest hours | `EVENTS` | `[HOUR]` | `[COUNT]` |
| Most-active authors | `EVENTS` | `[AUTHOR]` | `[UNIQUE_EVENTS]` (add `kinds:[1]` for notes) |
| Most-reacted events | `REACTIONS` | `[TARGET_EVENT]` | `[COUNT]` |
| Emoji breakdown | `REACTIONS` | `[REACTION]` | `[COUNT]` |
| Most-replied events | `REPLIES` | `[TARGET_EVENT]` | `[UNIQUE_EVENTS]` |
| Popular tags | `TAGS` | `[TAG_KEY]` | `[COUNT]` |

## Don'ts (encoded gotchas)

- **Zaps are stubbed → 0.** Never report numbers from `zaps`/`zapSats`/`zappers`.
- **No time-range filter, no text search, no list-all-events root.** To enumerate
  events, group `aggregateEvents` by `EVENT_ID`.
- `first` is capped at 100; `hasNextPage` is always `false` (no real pagination).
- A dim/metric outside the dataset's column in the matrix makes the API error.
- A `TARGET_EVENT` (from REACTIONS/REPLIES) is usually a *referenced* event, not a
  stored one — `event(id)` on it returns mostly null. Use the aggregate counts.
- `KIND` in REACTIONS/REPLIES is the actor's kind (=7), not the target's, so
  "most-reacted **note**" isn't expressible — only "most-reacted target event".

**Full schema vocabulary, field semantics, and the validity matrices live in `reference.md`.**
