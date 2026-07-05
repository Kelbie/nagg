# Benchmark: REST app-view vs GraphQL (live v2 stack)

A repeatable latency comparison of nagg's precomputed REST app-view routes
against their closest GraphQL equivalents, run against the live v2 deployment.
The point is not "which transport is faster" in the abstract — the two sides
deliberately do **different amounts of work** — but to quantify what the
registry-backed app-view buys over prototyping the same read live in GraphQL.

- **Script:** `scripts/bench-appview-vs-graphql.ts` (bun, no deps)

  ```bash
  bun scripts/bench-appview-vs-graphql.ts                 # full run (5 warmup + 30 timed per target)
  bun scripts/bench-appview-vs-graphql.ts --runs 5 --warmup 2   # quick pass
  bun scripts/bench-appview-vs-graphql.ts --base <url>    # other deployment
  ```

- **Stack:** `https://nagg-production.up.railway.app` — `appViewVersion: v2`,
  GraphQL schema version `2026-06-18`, REST under `/nostr/*` per
  `docs/appview-api.md`, GraphQL at `POST /graphql`.
- **No Redis response cache** is deployed there, so every request is honest
  server + ClickHouse work (the REST `Server-Timing: app;dur=…` header confirms
  per-request compute; it is captured in the table).

## Method

- Request shapes are **discovered once** from live data (active kind-1 authors
  → a real feed page → its event ids → the most-replied event as thread root),
  then **frozen**: every timed request is byte-identical. No randomization.
- Per target: 5 unrecorded warmups, then 30 timed requests, strictly
  sequential, paced ~550 ms apart to stay under the app-view's 120 req/min
  client rate limit (429s are waited out and never recorded).
- Wall-clock ms measured client-side (`performance.now()` around `fetch`).
  `p50/p95/max` over the 30 samples; size is the median response body.
- A `GET /nostr/capabilities` baseline (near-zero server work) estimates the
  network floor, so wall numbers can be read as ≈ RTT + server time.

### Caveats

- **Small, fresh dataset.** This deployment indexes a small recent corpus.
  Absolute numbers are near the query-cost floor; the *shape* of the gaps
  (round-trips, expressible-vs-not, enrichment cost) is the durable finding,
  not the milliseconds. On a large corpus the live-aggregation side is the one
  that grows.
- Timings include ~85–90 ms of network round-trip (see baseline row); the
  client ran far from the Railway region, so server-side deltas smaller than
  RTT jitter show up compressed. REST rows carry the honest server-side `app`
  duration from `Server-Timing`; GraphQL does not emit it.
- GraphQL responses here select compact field sets; REST envelopes carry full
  hydration. Sizes are therefore not like-for-like either — that asymmetry is
  part of the semantic story below.

## Results

Run of record: 2026-07-05, `runs=30, warmup=5`, sequential, fixed requests,
~550 ms pacing, client → Railway `osl1` edge. `server app p50` is the REST
`Server-Timing` `app` component (GraphQL does not emit it; estimate its
server-side time as wall p50 − ~86 ms baseline).

Base: `https://nagg-production.up.railway.app`

| Target | Transport | p50 (ms) | p95 (ms) | max (ms) | ~size | server app p50 (ms) |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| baseline: GET /nostr/capabilities | REST | 86 | 90 | 90 | 1.6 KB | 0.0 |
| feed: POST /nostr/feed (5 authors, limit 20) | REST | 190 | 206 | 225 | 28.1 KB | 78.5 |
| feed floor: GraphQL events (same authors/kinds/limit) | GraphQL | 113 | 120 | 135 | 13.1 KB | — |
| aggregates: POST /nostr/events/aggregates (20 ids) | REST | 107 | 121 | 138 | 725 B | 19.1 |
| aggregates: GraphQL 4× aggregateEvents (same 20 ids) | GraphQL | 180 | 205 | 211 | 980 B | — |
| ranked: POST /nostr/feed/ranked (limit 20) | REST | 255 | 303 | 307 | 39.6 KB | 145.3 |
| ranked: GraphQL rankedEvents (identical input) | GraphQL | 176 | 217 | 220 | 23.0 KB | — |
| thread: GET /nostr/thread (limit 50) | REST | 207 | 217 | 229 | 74.9 KB | 91.7 |
| thread: GraphQL event + referencedBy (limit 50) | GraphQL | 144 | 174 | 216 | 61.9 KB | — |

## Semantic parity per pair — what each side is actually doing

The pairs are intentionally *not* equal-work. Per pair:

**1. Feed — `POST /nostr/feed` vs `events(input:{pubkeys, kinds, limit})`.**
The REST route runs the same base query (`kind IN (1,6,16)` for the author
set) **plus** hydration (repost originals, resolved roots, quoted events, each
author's kind-0 profile) **plus** every registry aggregate for the page. The
GraphQL side is the **raw-query floor**: bare event rows, nothing a client
could render a feed from without N follow-up queries. Read the delta as the
price of a render-ready page, not a transport tax.

**2. Aggregates — `POST /nostr/events/aggregates {ids}` vs 4× aliased
`aggregateEvents`.** The REST route evaluates the *entire declared rule
registry* for the ids (k7_e, k6_16_e, k1_q, k1_1111_e_reply, k9735_e
value_total + sources, and the vertex_* score-gated variants). The GraphQL
prototype approximates only **four rule families** (kind-7 actors, kind-6/16
actors, kind-1 `q` quotes, kind-9735 receipt count) and **cannot express** the
rest in one request: zap `value_total` needs bolt11/amount parsing, reply
counts need the reply-edge table, vertex-gated variants need the score rollup.
This is the purest pair: GraphQL does *less* and still pays per-rule-family
aggregation cost.

**3. Ranked — `POST /nostr/feed/ranked` vs `rankedEvents(input)`.** The one
pair with a **shared engine**: the REST route decodes its body into the exact
input map the GraphQL `rankedEvents` field accepts and runs the same ranking
pipeline (this benchmark sends the identical map to both). The difference is
that REST then enriches the ranked ids into a full feed envelope (profiles,
aggregates, hydration). The REST−GraphQL delta ≈ the cost of enrichment; the
GraphQL number ≈ the ranking pipeline alone.

**4. Thread — `GET /nostr/thread?id=` vs `event(id){ referencedBy }`.** REST
returns the root plus a **server-ranked** reply order (`order[0]` = root),
reply aggregates, and profile hydration. The closest GraphQL composition —
`event(id)` + `referencedBy` over the `e` tag — returns a flat reverse-
reference page in reverse-`created_at` order: no ranking, no aggregates, no
profiles. Reproducing the REST semantics in GraphQL would take additional
aggregation queries plus client-side sorting.

## Conclusions

Subtracting the ~86 ms network baseline gives approximate server-side costs:

| Pair | REST server (measured) | GraphQL server (est.) | What the numbers mean |
| --- | ---: | ---: | --- |
| feed | ~79 ms | ~27 ms | REST spends ~3× the floor to return a render-ready page (hydration + all aggregates) in **one** round trip; the GraphQL floor still needs profiles + aggregates + repost/quote resolution — several more round trips — before anything can render. |
| aggregates | ~19 ms | ~94 ms | The headline: the registry-backed lookup computes the **full** rule set ~5× faster than a GraphQL prototype that expresses only 4 of the rule families and cannot express zap totals, reply counts, or vertex-gated variants at all. |
| ranked | ~145 ms | ~90 ms | Same engine, same input map. The ~55 ms delta is exactly the envelope-enrichment cost (profiles + aggregates + hydration of 20 ranked events) — the ranking pipeline itself dominates both sides. |
| thread | ~92 ms | ~58 ms | ~34 ms buys server-ranked reply order, 37 aggregate targets, and profile hydration; the GraphQL composition would need extra queries plus client-side sorting to match it. |

The expected story holds, quantified:

- **Registry-backed app-view reads are trusted lookups.** `/nostr/events/aggregates`
  answers the entire declared rule vocabulary for 20 events in ~19 ms of server
  time because the registry's shape (and its precomputed edges/rollups) is the
  query plan. The GraphQL path re-derives a *subset* of that live for ~5× the
  cost — and this is on a **tiny dataset**; live aggregation scales with corpus
  size, precomputed lookups largely don't.
- **GraphQL is the prototype path, and it's good at that.** For a brand-new
  read there is real value in getting *something* in ~30–90 ms of server time
  with zero server changes — `rankedEvents` proves the whole ranking pipeline
  is reachable from a query string. The cost only appears when a prototype
  tries to reach **product parity**: each registry rule becomes another aliased
  aggregation (or an inexpressible one), each hydration becomes another
  round trip, and ranking/grouping semantics leak into the client.
- **The cheap-looking GraphQL rows are cheap because they do less.** Every
  GraphQL win in the wall-clock column (feed 113 vs 190, thread 144 vs 207)
  disappears once the client issues the follow-ups REST already folded in —
  and each follow-up costs a full ~86 ms RTT before any server work. One
  render-ready REST response beats 3–5 prototype queries on every axis that
  matters to the app.

Bottom line: prototype in GraphQL, ship as a declared rule + app-view route.
The measured gap between the two is the price (and the value) of promoting a
query into the registry.
