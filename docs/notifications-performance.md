# Nagg app-view load-time audit — notifications, feed, profile (June 2026)

> For how the notification flow is *meant* to work (grouping, policies,
> pagination, the layer responsibilities), see
> [`notifications-flow.md`](./notifications-flow.md). This doc is the perf
> history + benchmarks.


Status: **shipped** (query restructure + API changes deployed to the Railway
`nagg` service, schema `2026-06-07`). Materialized read-model is designed below
as the next step.

This document records a load-time audit of the `nagg` app-view / GraphQL
surfaces against real, popular Nostr accounts, the root-cause analysis of the
one slow surface (notifications), the fix that shipped, before/after numbers,
and two API changes that landed alongside it.

---

## 1. TL;DR

- Measured every surface the Sovran feed uses (profile, own feed, Following,
  For You, notifications) against **15 verified high-profile Nostr accounts**,
  cold (uncached) and warm (cached).
- **Everything except notifications was already healthy** — sub-1.5 s cold,
  ~0.15–0.3 s warm.
- **Notifications were broken for popular accounts**: 9 of 15 test accounts
  exceeded the 30 s handler timeout (HTTP 500/504) and returned nothing; the
  other 6 took 6.8–25.8 s. Because errors are never cached, the slow accounts
  could *never* warm up — every load was a 30 s failure.
- Root cause: the `store.Notifications` ClickHouse query joined and sorted the
  viewer's **entire** notification history (multiple `FINAL` merges + a
  reply-reference scan over all of `event_tags`) **before** applying `LIMIT`.
- Fix shipped: **limit-before-join restructure** — bound the candidate window
  first, then run the heavy joins only against that small set. Result:
  **0 timeouts, cold 0.55–8.0 s (median ~3.9 s), warm 0.18–0.80 s.**
- Shipped alongside: notifications now take `pubkey` (not `viewer`) for
  consistency, and the ranked feed carries the viewer `pubkey` for
  forward-compatible personalization.
- Next: a denormalized materialized read-model to push notifications cold to
  sub-second (§9).

---

## 2. Method

- **Target:** `https://nagg.up.railway.app` (the production Railway `nagg`
  service), hit directly with `curl`.
- **Transports per surface** (matched to what `nagg-ts` recipes actually build):

  | Surface | Transport | Request |
  |---|---|---|
  | Profile | REST app-view | `GET /nostr/profile?pubkey=…` |
  | Own feed (author's posts) | REST app-view | `GET /nostr/feed/user?pubkey=…&limit=30` |
  | Following (recent) | GraphQL | `POST /graphql` — `events(pubkeysFrom:[{latestEventTags:{kinds:[3] tag:{key:"p"} maxValues:2000}}] …)` (server expands the kind-3 follow list) |
  | For You (ranked) | REST app-view | `POST /nostr/feed/ranked` (ranked input map) |
  | Notifications | REST app-view | `GET /nostr/notifications?pubkey=…&limit=50` |
  | Follow counts | REST app-view | `GET /nostr/follows?pubkey=…` |

- **Cold vs warm:** every request carried a per-run-unique cache key. The first
  hit is **cold** (uncached); an immediate second hit to the same URL is
  **warm** (cache hit). The response cache is `30 s` fresh + `5 m`
  stale-while-revalidate (`NAGG_CACHE_DEFAULT_TTL` / `NAGG_CACHE_STALE_FOR`).
- **Important caching nuance:** the cache only stores *successful* responses.
  A notification request that times out (500/504) is never cached, so its
  "warm" retry is just another cold miss — the slow accounts can never benefit
  from the cache. This is why notifications had to be fixed at the query level,
  not the cache level.
- Notification cold timeouts were capped at 35 s by the client; the server's own
  handler timeout is **30 s** (`internal/appview/handler.go` — `context.WithTimeout(r.Context(), 30*time.Second)`), and Railway's edge adds a 504 at ~30 s.

### Identities tested (npub → hex, decoded and verified)

These are well-known, high-follower Bitcoin/Nostr accounts (fan-out chosen to
stress the engagement-heavy path). `follows`/`followers` are nagg's indexed
counts at test time.

| Name | hex pubkey | follows | followers |
|---|---|--:|--:|
| fiatjaf | `3bf0c63f…a459d` | 192 | 13.1k |
| jack | `82341f88…e6a2` | 695 | 14.9k |
| jb55 (Will Casarin) | `32e18276…e245` | 857 | 7.6k |
| ODELL | `04c915da…ecc9` | 1667 | 14.2k |
| Gigi | `6e468422…ee93` | 1099 | 13.9k |
| Snowden | `84dee6e6…7240` | 354 | 13.0k |
| Vitor (Amethyst) | `460c25e6…065c` | 249 | 12.3k |
| Lyn Alden | `eab0e756…1f4f` | 228 | 8.7k |
| NVK | `e88a691e…0411` | 502 | 7.3k |
| hodlbod | `97c70a44…e322` | 459 | 5.0k |
| pablof7z | `fa984bd7…8f52` | 1053 | 7.0k |
| Preston Pysh | `85080d3b…1204` | 369 | 7.2k |
| Guy Swann | `b9e76546…83df` | 0 | 0 |
| Carla | `1afe0c74…651f` | 0 | 0 |
| Wallet of Satoshi | `f8e6c643…8ca9` | 331 | 5.6k |

> Note: a pubkey only indexes once nagg has seen the account's kind-3 / activity.
> `Guy Swann` / `Carla` show 0 follows because their graph wasn't backfilled — yet
> their notifications still exercised the slow path (mentions/replies addressed
> to them). My initial probe used a wrong ODELL hex (`…e44a`) that decoded to a
> different key returning 0/0; decoding every npub from bech32 fixed it
> (`…ecc9`).

---

## 3. Baseline results (before the fix)

All times in seconds. `notif_c` is cold notifications; `http` is its status
(`504`/`500` = server/edge timeout, `000` = client-side 35 s cap).

| account | profile c/w | own feed c/w | Following c/w | For You c/w | **notif cold** | http | notif warm |
|---|---|---|---|---|--:|:--:|--:|
| fiatjaf | 1.14 / 0.16 | 1.24 / 0.22 | 0.85 / 0.16 | 1.59 / 1.22 | **30.16** | 500 | 30.26 |
| jack | 1.41 / 0.15 | 1.53 / 0.23 | 1.22 / 0.15 | 1.58 / 1.20 | **30.16** | 504 | 30.18 |
| jb55 | 0.98 / 0.15 | 1.30 / 0.29 | 0.94 / 0.15 | 1.75 / 1.13 | **30.16** | 504 | 30.28 |
| ODELL | 0.92 / 0.15 | 1.40 / 0.22 | 0.87 / 0.15 | 1.22 / 1.04 | **30.16** | 504 | 30.25 |
| Gigi | 1.02 / 0.16 | 1.21 / 0.22 | 1.02 / 0.15 | 1.29 / 1.32 | **30.15** | 500 | 24.45 |
| Snowden | 1.06 / 0.17 | 1.88 / 0.25 | 1.25 / 0.17 | 1.57 / 1.14 | **30.15** | 504 | 30.20 |
| Vitor | 5.30 / 0.16 | 1.02 / 0.31 | 0.98 / 0.15 | 1.36 / 1.12 | **25.75** | 200 | 0.31 |
| Lyn Alden | 0.99 / 0.23 | 0.73 / 0.23 | 0.95 / 0.15 | 1.15 / 1.27 | **35.0** | 000 | 30.19 |
| NVK | 0.87 / 0.15 | 0.81 / 0.21 | 1.22 / 0.17 | 1.59 / 1.18 | **30.18** | 504 | 30.25 |
| hodlbod | 0.85 / 0.17 | 0.75 / 0.21 | 0.87 / 0.15 | 1.58 / 1.09 | **14.95** | 200 | 0.39 |
| pablof7z | 1.00 / 0.15 | 0.72 / 0.21 | 1.09 / 0.36 | 1.28 / 1.20 | **8.27** | 200 | 0.44 |
| Preston Pysh | 0.89 / 0.16 | 0.62 / 0.17 | 0.61 / 0.22 | 1.20 / 1.31 | **10.53** | 200 | 0.52 |
| Guy Swann | 0.82 / 0.16 | 0.47 / 0.15 | 0.84 / 0.15 | 1.16 / 1.33 | **30.17** | 504 | 11.73 |
| Carla | 0.79 / 0.16 | 0.80 / 0.15 | 0.64 / 0.23 | 1.49 / 1.30 | **10.90** | 200 | 0.15 |
| Wallet of Satoshi | 1.53 / 0.17 | 0.68 / 0.20 | 1.09 / 0.18 | 1.20 / 1.14 | **6.84** | 200 | 0.32 |

Readout:
- **Profile / own feed / Following**: healthy. Cold 0.5–1.9 s, warm 0.15–0.3 s
  (cache gives ~7–9×). No action.
- **For You (ranked)**: fine at 1.2–1.75 s cold, but note warm ≈ cold (~1.0–1.3 s)
  — the ranked POST barely benefits from the cache (see §8.3).
- **Notifications**: **9 of 15 timed out** (30 s+, 500/504/000). The 6 that
  returned took 6.8–25.8 s. Warm only helped the accounts that succeeded cold
  (cache stores success only).

---

## 4. Root cause — it's the ClickHouse query, not the network

Three isolation experiments pinned the cost to `store.Notifications` itself, not
relay backfill, hydration, or serialization:

1. `GET /nostr/notifications?…&limit=1` → still 30 s timeout. **Result size is
   irrelevant** — the full candidate set is processed before `LIMIT`.
2. `tab=MENTIONS` (a strict subset) → still 30 s. The expensive work runs
   regardless of the post-filter.
3. **GraphQL** `notifications(input:{pubkey:…}){nodes{event{id}}}` — which does
   **no** REST hydration/backfill — returned `"context deadline exceeded"` at
   30 s. This proves the ClickHouse query is the bottleneck; hydration and
   relay backfill are not involved.

### Railway log evidence

During feed/Following requests the `nagg` service logs heavy **on-demand relay
fan-out** (`msg="on-demand relay query returned events" relay=wss://… events=…`
across ~30 relays) — that's the cold-feed backfill path and explains cold-vs-warm
gaps on *feed* surfaces. The notifications path does **not** backfill before the
query; its logs end in `context deadline exceeded` with no result, consistent
with the ClickHouse query being killed by the 30 s context.

### Why the query was slow (`internal/clickhouse/read.go`)

`notification_candidates` is a `ReplacingMergeTree` keyed
`ORDER BY (viewer, created_at, event_id, reason)`, fed by a materialized view
from `event_tags` (every `p`-tag reference to the viewer — every reaction,
repost, zap, reply, mention). The legacy read did, in order:

1. `SELECT … FROM notification_candidates FINAL WHERE viewer = ?` — the viewer's
   **entire** history, plus a `row_number()` window over all of it.
2. `INNER JOIN nostr_events AS e FINAL` — `FINAL` merge over the events table.
3. Two `LEFT JOIN vertex_scores … FINAL` — two more merge-on-read joins.
4. For the default `replyScope=THREAD`: a reply-reference subquery that scans
   **all** of `event_tags` (`tag_key='e'`, grouped by event), plus
   `event_tags ⋈ nostr_events FINAL` to check whether the viewer authored the
   parent.
5. `ORDER BY notification_created_at DESC … LIMIT ?` — applied **last**, after
   all of the above.

So for a high-engagement viewer the joins and the reply scan ran over the whole
notification history before the page limit applied. Confirmed that no policy/
scope knob avoided it:

```
jb55  replyScope=THREAD policy=STRICT   -> 54s 504
jb55  replyScope=DIRECT policy=STRICT   -> 30s 500
jb55  replyScope=THREAD policy=RELAXED  -> 30s 504
jb55  replyScope=THREAD policy=MODERATE -> 30s 504
```

---

## 5. The fix that shipped — limit-before-join

Commit `perf(notifications): bound candidate window before FINAL joins and reply
scans` (`internal/clickhouse/read.go`).

A `WITH recent AS (…)` CTE takes only the most-recent candidate window for the
viewer — a cheap range scan on the `(viewer, created_at, …)` sort key —
over-fetched 8× the page size (min 400, max 4000) so the follow-dedupe, policy
threshold, and reply-scope filters still fill the page. **Every** downstream
piece is then bounded to that window:

- `nostr_events FINAL` is filtered `WHERE id IN (SELECT event_id FROM recent)`.
- `vertex_scores FINAL` (actor) is filtered to `pubkey IN (SELECT actor_pubkey
  FROM recent)`; the viewer score is a single-key lookup.
- The reply-reference subqueries add `AND event_id IN (SELECT event_id FROM
  recent)`, and the `reply_parent` / `referenced` event lookups are bounded to
  the parent ids of those candidates — so the formerly-global `event_tags` scan
  now scales with the page, not the tag table.

Why the over-fetch is safe: for popular viewers the `STRICT` policy is a no-op
(the viewer's own vertex score clears the `viewer ≥ 80` threshold, so the
`actor OR viewer` test passes for every row), and only kind-1 replies are
scope-filtered — reactions/zaps/reposts/mentions always pass. 8× the page
comfortably refills. Worst case (a low-score account flooded with low-score
spam) under-fills the page slightly — strictly better than the previous 30 s
timeout, and addressed properly by the materialized model in §9.

Behavioural note: follow-dedupe is now scoped to the recent window (keep one
follow per actor within the window) rather than all-time. For a notifications
feed this is equivalent in practice.

---

## 6. After results (deployed)

| account | **notif cold (before → after)** | notif warm | events |
|---|--:|--:|--:|
| fiatjaf | 30.16 (500) → **4.29** | 0.39 | 50 |
| jack | 30.16 (504) → **3.51** | 0.41 | 50 |
| jb55 | 30.16 (504) → **3.94** | 0.65 | 50 |
| ODELL | 30.16 (504) → **5.89** | 0.33 | 50 |
| Gigi | 30.15 (500) → **3.28** | 0.40 | 50 |
| Snowden | 30.15 (504) → **7.98** | 0.35 | 50 |
| Vitor | 25.75 → **4.06** | 0.35 | 50 |
| Lyn Alden | 35.0 (timeout) → **3.87** | 0.60 | 50 |
| NVK | 30.18 (504) → **3.76** | 0.68 | 50 |
| hodlbod | 14.95 → **4.09** | 0.80 | 50 |
| pablof7z | 8.27 → **5.19** | 0.66 | 50 |
| Preston Pysh | 10.53 → **3.86** | 0.59 | 50 |
| Guy Swann | 30.17 (504) → **0.55** | 0.18 | — |
| Carla | 10.90 → **0.83** | 0.18 | — |
| Wallet of Satoshi | 6.84 → **3.28** | 0.50 | 50 |

- **Timeouts: 9 → 0.** Every account now returns a full 50-item page.
- Cold: **0.55–7.98 s** (median ~3.9 s). Warm: **0.18–0.80 s** — and because
  responses now succeed, the cache finally helps on repeat loads.
- Profile / own feed / Following / For You were unaffected by the change and
  match their baseline.

Cold is now bounded and usable but still multi-second for engaged accounts; §9
takes it to sub-second.

---

## 7. API change 1 — notifications take `pubkey`, not `viewer`

Every other account-scoped route uses `pubkey` (`/nostr/follows`,
`/nostr/feed/user`, `/nostr/profile`); only notifications used `viewer`. That
was the inconsistency. Since nothing has shipped, this is a clean rename with no
compatibility shim (per `no-backwards-compatibility`).

- **nagg**: GraphQL `NotificationInput.viewer → pubkey`
  (`parseNotificationInput`), REST `parseNotificationRequest` query param + JSON
  field `viewer → pubkey`. The internal `notification_candidates.viewer` column
  and `NotificationInput.Viewer` Go field keep their name — it's a meaningful
  domain term in the data model; only the request surface changed.
- **nagg-ts** (`0.3.0 → 0.4.0`, breaking): `NotificationsInput`,
  `notificationsInput`, `NotificationsAppViewInput`, `notificationsAppView` take
  `pubkey`.
- **sovran-app**: the notification call site
  (`features/feed/data/naggFeedClient.ts`) passes `pubkey`.
- **colada**: **not affected** — its `notifications` are payment-flow UI
  callbacks, unrelated to the social notifications endpoint.
- DM (`dmEnvelopes` / `dmConversation`) and `followStatus` keep `viewer` —
  there the term is relational (viewer vs counterparty/candidates) and
  meaningful, and the user only flagged notifications.

## 8. API change 2 — ranked feed carries the viewer `pubkey`

So future personalization (viewer-specific boosts, mutes, vertex weighting) can
land **without** a client/schema change — old clients already send the field.

- **nagg**: `RankedEventsInput` gains an optional top-level `pubkey`, accepted on
  the GraphQL `rankedEvents` input and the REST `/nostr/feed/ranked` body,
  normalized and threaded onto the parsed `rankedEventsInput.ViewerPubkey`
  (currently advisory; never fails the request). New capability flag
  `graphql.rank.viewerPubkey`.
- **nagg-ts**: `RankedEventsInput.pubkey`; `forYouRankedEventsInput` and
  `followingPopularRankedEventsInput` set it from `viewerPubkey`.
- **sovran-app**: flows automatically (the builders already receive
  `viewerPubkey`).

### 8.3 Aside: For You doesn't cache well

In the baseline the ranked POST's warm time ≈ cold (~1.0–1.3 s). `POST
/nostr/feed/ranked` is a mutation-shaped request; the REST cache keys/freshness
favour GETs. If For You latency matters later, consider caching the ranked POST
by a normalized body hash, or moving it behind a GET with query params.

### Schema/cache versioning

`GraphQLSchemaVersion` bumped `2026-06-06 → 2026-06-07` (it seeds the
response-cache key, so the bump also invalidates stale cached notification
responses from the old query).

### Release sequencing (important)

The nagg server change is **already deployed**, so it now expects `pubkey` for
notifications. The live app consumes the **published** `@sovranbitcoin/nagg-ts`
`0.3.0`, which still sends `viewer` — so app notifications will error until:

1. Publish `@sovranbitcoin/nagg-ts@0.4.0` (committed locally, version bumped).
2. Bump sovran-app's dependency to `^0.4.0` and ship.

This is acceptable under the "nothing shipped yet" assumption, but the window
exists until the package is republished.

---

## 9. Next — materialized notifications read-model (designed, not yet shipped)

The restructure removed the timeouts; a denormalized read-model removes the
remaining multi-second cold by eliminating the read-time `FINAL` merges and the
reply scan entirely. Sketch:

- A `notifications_feed` table keyed `(viewer, created_at, event_id)` holding the
  **denormalized** notification: event id/pubkey/kind/created_at/content/tags +
  `actor_pubkey` + precomputed `is_reply` / `is_reply_to_viewer` + the actor's
  last-known `actor_score`. Reads become a plain
  `WHERE viewer = ? [policy on stored score] ORDER BY created_at DESC LIMIT N`
  — no `FINAL`, no joins, no tag scan → sub-second.
- Population: extend the existing `mv_notification_candidates` MV (or a
  companion MV/enrich task) to denormalize event fields and the reply flags at
  ingest. `is_reply` / parent are knowable from the event's own tags;
  `is_reply_to_viewer` needs the parent's author, resolvable in the enrich pass.
- `actor_score` drifts as vertex scores update; refresh it in the periodic
  vertex/enrich job rather than read-time. Policy filtering then reads the
  stored score (degrade missing → 0).
- Migration must be declared in `internal/clickhouse/migrations` and the
  reconcile list (the deploy runs a **declarative schema reconcile that strips
  undeclared tables/columns**, so an out-of-band table would be dropped).
- Verify by re-running the §2 benchmark against the new read path; expect cold
  to collapse toward the warm numbers (~0.2–0.4 s).

This is its own focused deploy + backfill cycle and should be measured the same
way before/after.

---

## 10. Reproduce

The benchmark is a self-contained bash script (`curl` + a per-run cache key,
cold then warm for each surface, CSV out). Pubkeys were decoded from bech32
npubs with a small Python bech32 decoder. To re-run after any change, point it
at `https://nagg.up.railway.app`, sweep the identities in §2, and diff the CSV.

Single-surface spot check:

```bash
# Notifications, cold (cache-busted):
curl -s "https://nagg.up.railway.app/nostr/notifications?pubkey=<hex>&limit=50&_=$RANDOM" \
  -w '\n[%{http_code} %{time_total}s]\n' -o /dev/null

# Following (GraphQL, server expands the kind-3 follow list):
curl -s -X POST https://nagg.up.railway.app/graphql -H 'content-type: application/json' \
  -d '{"query":"{ events(input:{ pubkeysFrom:[{ latestEventTags:{ pubkey:\"<hex>\" kinds:[3] tag:{key:\"p\"} limit:1 maxValues:2000 } }] kinds:[1,1111] limit:30 }){ nodes { id } } }"}' \
  -w '\n[%{http_code} %{time_total}s]\n' -o /dev/null
```

---

## 10b. Notification grouping (shipped)

The All tab was still dominated by follows: every follower's kind-3 republish
becomes a follow candidate, so the recency-ordered window was *entirely*
follows and the page collapsed to one follow entry with nothing else.

Fix (app-view only; generic GraphQL stays per-event, mirroring how `feedResponse`
collapses reposts):

- `GET /nostr/notifications` now returns **grouped items**. follow / repost /
  reaction / zap collapse (follow → one group; the rest per target post); reply
  / quote / mention stay `type:"single"` so their text is readable. Each group
  node carries a representative event, a `total`, `totalCapped`, ≤3
  `sampleActors`, and (for repost/reaction/zap) an inline `targetEvent`.
- **Follows are fetched on a separate capped window** so the flood can't crowd
  out everything else; the body window excludes follows. The follow `total` is
  the exact `FollowCounts().Followers`; other totals are window-bounded with
  `totalCapped` → "N+". `limit` now means items, so a page is a genuine mixture.
- `grouped=false` returns the raw ungrouped rows (the followers-detail screen).

Verified live (e.g. jack): **17 items** — 1 follow group (`total` 15,046, 3
sample actors), plus reactions/mentions/reposts/replies/quotes, profiles
hydrated. Latency ~6–7 s cold (the extra follow sub-query adds ~2–3 s; a
follow-window that skips the reply-scope joins is an easy follow-up), warm
sub-second.

Clients: `@sovranbitcoin/nagg-ts` schema gains the optional group fields +
`grouped` param; `sovran-app` renders server groups (avatar cluster from
`sampleActors`, "A, B and N more …", reaction/zap titles, target preview) and
falls back to client-side consecutive grouping for the GraphQL path.

## 11. Commits

- `nagg` (pushed to `Kelbie/nagg` main → deployed):
  - `perf(notifications): bound candidate window before FINAL joins and reply scans`
  - `feat(api)!: notifications pubkey input + viewer pubkey on ranked feed`
  - `feat(notifications): group follows/reposts/reactions/zaps in the app-view`
  - `fix(notifications): pull follows onto their own window so the page is a mixture`
- `nagg-ts` (committed, `0.4.0`, needs publish):
  - `feat(api)!: notifications take pubkey; ranked input carries viewer pubkey`
  - `feat(notifications): grouped node schema + grouped app-view param`
- `sovran-app` (committed, needs nagg-ts `0.4.0`):
  - `feat(feed)!: pass pubkey to nagg notifications (API rename)`
  - `feat(notifications): render server-grouped notifications with a mixture`

---

## 12. Concurrency ceiling — the real prod limit (June 24)

Symptom: in the app, feed / notifications / search intermittently showed **no
nagg results** while nagg "seemed online". Device logs showed the app calling
every nagg endpoint correctly and degrading through `nagg → primal → relay`, but
a chunk of nagg reads returned **HTTP 500 (slow, ~2 s — a ClickHouse read that
ran then failed) or 502 (fast, ~110 ms — the container shedding/restarting)**,
interleaved with successes. So the client fell back and the user saw
primal/relay content (or nothing) instead of nagg's ranked results.

Root cause is **concurrency, not the per-query restructure (§5, still intact) and
not the client.** A graduated live test against prod (`/nostr/notifications`,
fiatjaf, cold, unique cache key per request) found a hard ceiling:

| concurrent cold reads | result |
|--:|---|
| 2 | both `200` (~7 s) |
| 3 | 2× `200` + **1× `500`** (fast, ~0.3 s) |
| 4 | 2× `200` + **2× `500`** |
| 5 | 2× `200` + **3× `500`** |

**Exactly two heavy reads fit**; ClickHouse sheds every additional one
immediately. A single engaged-account notifications query is multi-GB and ~7 s,
so two of them already saturate the shared ~30 GB instance (and the in-process
firehose / enricher / rollup workers compete for the same memory). An app launch
fires feed + notifications + search + profiles **concurrently**, so the burst
always overran the ceiling. The `NAGG_MAX_CONCURRENT_REQUESTS` guard was set to
**24** — far above the ceiling, so it never engaged: ClickHouse rejected first.

### What shipped

- **`NAGG_MAX_CONCURRENT_REQUESTS` default `24 → 2`** (`internal/config/config.go`).
  At 2 the semaphore admits only what ClickHouse can serve; excess **queues**
  (bounded by the 30 s request context) and succeeds slower instead of being shed
  as 5xx → the client keeps nagg instead of falling back.
- **New `NAGG_CLICKHOUSE_MAX_QUERY_MEMORY_BYTES`** (`0` = unset) — a per-read
  `max_memory_usage` cap injected on the read path (`internal/clickhouse/store.go`,
  via the `retryConn` read wrapper; inserts/rollup keep their own settings). A
  runaway read is rejected cleanly (`MEMORY_LIMIT_EXCEEDED`) instead of
  cgroup-OOMing the container (the 502/flap source). Off by default — set it once
  the real per-query footprint is measured.

### Caveat — this is a low-traffic ceiling, not the durable fix

A global limit of 2 with ~7 s heavy reads is ~0.3 cold-miss req/s; it stops the
5xx storm at current traffic but will queue-timeout as users grow. The durable
fixes that **raise** the safe concurrency are, in order: the materialized
notifications read-model (§9, collapses cold latency + per-query memory), then
scaling the ClickHouse instance and/or splitting the firehose/enricher/rollup
workers off the API service (`railway.ingester.toml` / `railway.enricher.toml`)
so reads don't compete with ingest for memory. Re-run the table above after any
of those and raise `NAGG_MAX_CONCURRENT_REQUESTS` to the new measured ceiling.

Immediate ops mitigation (no deploy of this change needed): set
`NAGG_MAX_CONCURRENT_REQUESTS=2` on the Railway `nagg` service and redeploy.
