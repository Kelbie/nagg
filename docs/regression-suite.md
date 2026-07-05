# v2 app-view live regression suite

`scripts/regression-check.ts` runs pinned-fixture regression checks against a
**live** nagg deployment (default: Railway prod), covering **every route the
app-view advertises** in `/nostr/capabilities`. It exists so a deploy can be
checked for contract regressions in one command, without a client in the loop.

```bash
bun scripts/regression-check.ts                          # against prod
bun scripts/regression-check.ts --base https://other-nagg.example
```

Exit code is non-zero on any `FAIL`. The run is read-only, strictly
sequential, paced ~600 ms per request (the app-view rate-limits 120 req/min),
and a 429 is **never** recorded as a failure — the runner waits out the window
and retries.

## Pin vs. drift: what the suite asserts

A live Nostr dataset moves constantly. The suite therefore distinguishes three
classes, and the fixtures file records which class every value belongs to:

**Pinned (must never change).** Nostr events are immutable, so a pinned
event's `id`, `kind`, `pubkey`, `created_at`, and `sha256(content)` are
asserted exactly. Structural contract is pinned too: the generic envelope
shape (`order` / `orderBy` / `events` / `aggregates` / `cursor`), aggregate
keys drawn only from the **registry rule vocabulary** (`k7_e`, `k9735_e`,
`k3_p_latest`, … — see `docs/rules-registry.md`), **zero values omitted
entirely**, DM routes served **bare** (no aggregates, no kind-0 hydration),
notifications speaking **kind vocabulary only** (no `reason` strings anywhere
in the JSON), `appViewVersion: "v2"`.

**Presence classes (value volatile, existence stable).** Aggregate counts
only accumulate for old events and the aggregate ledger outlives the
referencing events, so "`k7_e` **exists** with `actors >= 1` for a
long-referenced event" is stable while the exact count is not. Same shape:
"at least one kind-1/1111 event e-tagging the pinned thread root" (events are
never deleted there), "entries non-empty for an active viewer", "a kind-0 for
the pinned author".

**Never asserted.** Exact aggregate counts, rankings and ranked membership,
the full reply set, follow-edge truth values (people unfollow), kind-0
contents (replaceable), result sets of search/recommended.

Two deliberate softenings, learned from live runs (both are documented server
behavior, not regressions):

- **Hydration is best-effort.** Feed-family hydration (repost originals,
  authors' kind-0s) rides on a *non-blocking* relay backfill, so an order
  anchor can legitimately arrive without its embedded original, and an obscure
  author can lack an indexed kind-0. Strict "every order id resolves & every
  author has a kind-0" is asserted only where the index guarantees it (the
  pinned thread, `/nostr/events` of pinned ids); feed routes assert it for the
  pinned event/author only.
- **`/nostr/events/query`** (non-1059) hydrates kind-0 profiles alongside the
  matches, so the filter assertions apply to the **ordered** items, not to
  every element of `events[]`.

## Fixtures

All pinned values live in `scripts/regression-fixtures.json`; every entry has
a `_note` saying why it is pinned and what may drift. Fixture choices follow
one rule: **pick events retention can never take.** Kinds 7/9735 have no
lifetime rule (kept forever); kinds 1/1111 expire after 1y **only if nothing
referenced them**, so every pinned kind-1 is heavily referenced (the reference
ledger protects it). Pinned authors are high-fanout accounts whose kind-0 is
always indexed.

| Endpoint | Fixture / pinned what |
| --- | --- |
| `/nostr/capabilities` | `appViewVersion: "v2"`; every advertised route must be covered by a check (the "did we forget an endpoint" gate — a new route fails the run until it gets a fixture) |
| `/nostr/feed` | 2 pinned authors (jack, gigi) × 1 pinned kind-1 each: exact content/author/kind/created_at, and the id must appear in `order` when `until` windows over it |
| `/nostr/feed/user` | pinned author: resolved order ids are the author's events or repost anchors; author kind-0 present |
| `/nostr/feed/ranked` | structure only (`orderBy: "rank"`, non-empty over a 48 h window) — ranking is volatile |
| `/nostr/events` | 3 pinned ids — one kind-1, one kind-7, one kind-9735 — exact content equality; `order` = the requested ids that resolved |
| `/nostr/events/aggregates` | long-referenced event: `k7_e` and `k9735_e` keys **exist** with metrics ≥ 1; aggregates-only envelope (`order`/`events` empty) |
| `/nostr/events/query` | (a) kind-7 e-tag query: every ordered event matches the filter; (b) kinds `[1059]`: **bare** envelope (privacy) |
| `/nostr/thread` | pinned root (exact) at `order[0]`; ≥ 1 kind-1/1111 reply e-tagging it; kind-0 for root + every reply author; aggregates present |
| `/nostr/notifications` | active viewer (+ pinned `policy=RELAXED`): entries non-empty, kinds ⊆ {1,3,6,7,16,9735}, entry `id`/`target` embedded in `events[]`, **no reason strings** |
| `/nostr/notifications/seen` | envelope; any event is kind-30078 (empty envelope = never marked, acceptable) |
| `/nostr/follows` | kind-0 for the pubkey + pubkey-keyed aggregates from the registry vocabulary |
| `/nostr/profiles` | kind-0 per requested pubkey, content parses as JSON |
| `/nostr/profile` | kind-0 + `pubkeys[]` contains the pubkey + provider namespaces ⊆ plugin registry (`vertex`/`nip05`/`nagg`) |
| `/nostr/search` | structure only (envelope + `pubkeys` + providers); vertex-credit-dependent → SKIP on DVM 5xx |
| `/nostr/recommended` | structure only; vertex-credit-dependent → SKIP on DVM 5xx |
| `/nostr/follow-status` | `edges` = exactly the candidate keys, each `{out,in}` booleans (truth values never pinned) |
| `/nostr/social-graph` | latest kind-3 (with p tags) present for an active pubkey |
| `/nostr/own/profiles` | kind-0 + pubkey-keyed aggregates |
| `/nostr/own/{type}` (`authored`) | every ordered event authored by the viewer, kinds ⊆ {1,1111}; list cursor format `<RFC3339Nano>\|<id>` |
| `/nostr/dm/envelopes`, `/nostr/dm/conversation` | structure + **privacy invariants**: aggregates empty, zero kind-0 events, kinds ⊆ {4,1059} |
| `/nostr/mint/reviews`, `/nostr/mint/discover` | not envelopes: 200 + parseable + top-level shape (`summary`/`reviews`/`profiles`; `mints[]` with `mintUrl`) |
| `/app/latest-version` | POST-only; `version` string present |

## Result classes

- `PASS` / `FAIL` — self-explanatory; any `FAIL` exits non-zero.
- `SKIP` — a vertex-credit-dependent route (`search`, `recommended`) answered
  with a DVM/vertex 5xx: the external dependency is down, not nagg.
- `XFAIL` — the failure matches a documented signature in
  `fixtures.knownIssues`: a **real, already-reported server bug** being
  tracked. Non-gating so the suite still guards everything else. When an XFAIL
  flips to PASS the runner tells you to delete the entry so the route gates
  again. At the time of writing there is one: `pubkey-stats-column-rename` —
  `BatchPubkeyStats` (`internal/clickhouse/read.go`) still selects `followers`
  from `pubkey_stats`, but migration `019_terminology.sql` renamed the columns
  to `k3_out`/`k3_in`/`k1_1111_authored`, so `/nostr/follows`,
  `/nostr/profile`, and `/nostr/own/profiles` 500 in prod.

## Adding an endpoint

1. Add a fixture entry (with a `_note`: why pinned, what may drift) to
   `scripts/regression-fixtures.json`.
2. Add one small check to the `checks` array in
   `scripts/regression-check.ts`, listing the advertised route path(s) in its
   `covers` — that registers it with the capabilities coverage gate.
3. Run twice against prod; both runs must agree before you trust it.

The shared helpers (`checkEnvelope`, `checkPinnedEvent`, `checkBareEnvelope`,
`checkPinnedK0`, `checkNoReasonStrings`) cover the generic contract; a new
check is usually the route call plus 2–5 route-specific assertions.

## When to re-pin

Re-pin **only when a contract change is intentional** — a renamed aggregate
rule, a new envelope field semantics, a deliberate route change — and land the
fixture update in the same PR as the server change, saying so. Never re-pin to
make a red run green: a pinned event's content/author/kind/created_at can only
"drift" if the server is corrupting or losing data, which is exactly the
regression this suite exists to catch. Two legitimate maintenance cases that
are *fixture* drift, not contract drift, both flagged by the runner's failure
message: the notifications viewer going dormant (the read model keeps 14 days
— re-pin to another active viewer), and a knownIssues entry whose bug got
fixed (delete the entry).
