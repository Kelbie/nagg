# Retention & ingest caps

How nagg keeps `nostr_events` (and its `event_tags` shadow) from growing without
bound, in two layers: **don't store what nobody will read** (ingest cap) and
**delete what stopped mattering** (lifetime rules). Both are declarative rules
in the registry (`internal/rules`, see `docs/rules-registry.md`): the whole
policy is two rule lists, readable in a minute.

Everything capped or pruned is recoverable — the on-demand relay backfills
(user feed, threads, DMs, profiles) fetch anything a user actually asks for and
insert it back.

## Layer 0 — the kind allowlist

Before either layer, `PruneRemovedEventKinds` deletes every stored event whose
kind is outside **`NAGG_KINDS`**. That is the *retained* set, and it is
deliberately separate from `NAGG_FIREHOSE_KINDS`, the *subscribed* set, which
defaults to it. A kind can be kept without being subscribed — that is how the
mint deployment holds the kind-0 profiles it fetches on demand while
subscribing only to kind 38000. Conflate them and every restart deletes those
profiles; see [`modules.md`](modules.md).

Because this prune is unconditional, a process must never be pointed at a
database belonging to a differently-configured deployment.

## Who is exempt: the relevance model

The recurring problem with any anti-spam cap is misclassifying real users —
especially brand-new ones — and per-author reputation lookups (Vertex) are too
expensive to run for everyone. nagg sidesteps both with a signal it already has:

1. **Known viewers** (`known_viewers` table): every pubkey that uses a Sovran
   client shows up as the *viewer* on its own notifications / DM / thread
   requests. The appview records those sightings (`WithViewerTouch`, throttled
   to 1 insert/viewer/hour). A new profile is known from its **first app
   request** — it can never be treated like a spammer. Rows expire after a year
   without a sighting.
2. **Their follows**: everyone on a known viewer's latest contact list
   (`latest_k3`).

`relevance.Tracker` refreshes this exemption set from ClickHouse every 15
minutes and **fails open**: until the first successful refresh, nobody is
capped. Subject pubkeys (a profile someone browses, a feed they look at) are
deliberately NOT touched — otherwise viewing a bot's profile would exempt the
bot and its whole follow list.

## Layer 1 — ingest post cap

The registry's Cap rules (`rules.Default`): the default rule
`k1_1111_6_16_daily` gives a non-exempt author at most `NAGG_POST_CAP_PER_DAY`
(default **20**, `0` removes the rule) events of kinds 1, 1111, 6, 16 per 24h
window; the rest are dropped at the firehose. A Cap has `{Kinds, Max, Window,
ExemptKnownViewers}`; `Window == 0` declares a lifetime cap (in-process
approximation until a durable counter lands).

Why 20: measured over 30 days of prod traffic (2026-07), 11.87M posts came from
102k authors, and **90% of the volume was beyond a 20/day cap** — produced by
~1–2k firehose bridge/bot accounts. Real humans a Sovran user might later care
about rarely exceed it, and if one does, the exemption list and the on-demand
backfills cover them.

Mechanics worth knowing:
- The window buckets on the ingestion wall clock (a 24h window is the UTC
  day), not the event's author-claimed `created_at`, so backdating can't
  dodge the cap.
- Each rule's counter map is bounded (`capCounterMaxEntries`); overflowing it
  fails **open**, never dropping untracked authors.
- On-demand relay backfills bypass the cap by construction (they insert via
  `Store.InsertEvents`, not the firehose pipeline) — demand-driven fetches are
  definitionally relevant.
- Drops are summarized in the `ingest.capped` log line (per flush), never
  logged per event.

### Addressee gates

The registry's AddresseeGate rules drop recipient-only kinds at the firehose
unless a `p` tag addresses the exemption universe. The default rule
`k1059_known_addressee` gates NIP-59 gift wraps: a wrap is readable ONLY by
its p-tagged recipient, so one addressed to a pubkey no Sovran viewer maps to
can never be served to anyone. Measured 2026-07: **99% of 2.5M stored wraps
(3.8 GB, the single largest kind) were addressed to strangers.** This reverses
the earlier "keep the whole DM firehose" decision — the relisten/merge churn
of network-wide DMs dominated the memory bill, and the on-demand DM backfill
(`#p`-filtered relay query) serves a new viewer's history anyway; the round
trip they pay once at signup is the trade. With no exemption source the gate
fails **open**, like the caps; drops summarize in `ingest.unaddressed`.

## Layer 2 — declarative lifetime rules

The registry's Lifetime rules (`rules.Default` in `internal/rules/defaults.go`)
— the declarations are the policy. Absence of a rule for a kind means events
of that kind live forever. A `NAGG_MODULES=mint` deployment runs `rules.Mint`
instead, which declares no Lifetime rules at all (there is no kind-1 corpus to
age out) and keeps only the supersession rules for kinds 0 and 38000:

| Rule | Kinds | Keeps | Why |
| --- | --- | --- | --- |
| `replaceable_latest` | 0, 3, 10050, 10051 | latest event per author | Relays and every nagg reader only ever use the newest version; the rest is dead weight. Measured 2026-07: 80–92% of stored rows superseded (~13 GiB, mostly kind-3 contact lists). |
| `param_replaceable_latest` | 30078, 38000 | latest per (author, d-tag) | Same, keyed per d-tag. Measured: 98.7% of kind-30078 superseded (~4 GiB). |
| `k1_1111_unreferenced_1y` | 1, 1111 | events younger than 1 year OR ever referenced by any of the declared relationships (k7_e, k6_16_e, k1_q, k9735_e, k1_1111_e_reply) | Nobody reads year-old events nothing ever referenced. `MaxAgeUnlessReferenced` builds its protection ledger from the relationships' aggregate tables. Small today (index is young), load-bearing as the index ages. |
| `k1059_known_addressee` | 1059 | wraps with a `p` tag in the exemption universe | Erodes the stored stranger-wrap backlog the matching addressee gate now stops at the firehose (99% of wraps when measured). Guarded to be a no-op while `known_viewers` is empty, so a wiped registry can never mass-delete. |

Rules delete from `nostr_events` only. `event_tags` is deliberately left
alone: its superseded-event rows measured a mere ~0.5 GiB (46.7M of 2.05B
rows), and an event_tags mutation — even running alone — saturates the
instance and takes user reads down (observed twice on 2026-07-04). Orphaned
tag rows are inert; every reader is either materialized-aggregate-based or
bounded to recent time windows. References are read from the rules' aggregate tables (`agg_k7_e` etc.),
which are never pruned — so deleting a referencing event later never
"un-references" its target.

Execution (`Store.RunRetention`, scheduled by the rollup runner):
- **Lightweight `DELETE FROM`**, submitted async (`lightweight_deletes_sync=0`):
  rows mask immediately as the mutation runs; disk space reclaims through
  background merges. No part rewrite at submit time — safe on a near-full disk.
- **At most ONE mutation per pass**, and never while another is still
  executing (`ErrRetentionBusy`). Field lesson 2026-07-04: two table-wide
  mutations running concurrently fanned out across the whole background pool
  and starved user reads (error 439) until the fat one was `KILL MUTATION`ed;
  a single serialized mutation ran with reads healthy throughout. (Partition
  scoping is NOT used — `DELETE ... IN PARTITION` did not restrict the rewrite
  on this ClickHouse version.)
- **Disk-headroom guard** (`ErrRetentionNoHeadroom`): a mutation reserves up
  to the table's largest part size while rewriting; submitting without
  headroom wedges it in a reserve-fail retry loop (observed: "Cannot reserve
  17.89 GiB" retrying forever). Retention waits for merges to free space.
- **Minimum batch** (`retentionMinMatchedRows` = 50k): a full-table mutation
  costs the same for 5k rows as for 2M, so rules wait until enough garbage
  accrues instead of churning parts all day.
- Pacing: re-ticks every 5 minutes while there is work in flight or queued;
  `NAGG_RETENTION_INTERVAL` (default 24h) is the idle cadence once everything
  has converged. First pass 10 minutes after boot; one `chgate` slot per pass.
- Every pass logs a `retention rule` line (rule, table, matched rows) for what
  it acted on.

## Levers

| Env | Default | Meaning |
| --- | --- | --- |
| `NAGG_POST_CAP_PER_DAY` | `20` | Daily post/repost cap for non-exempt authors; `0` disables. |
| `NAGG_RETENTION_INTERVAL` | `24h` | Idle retention cadence (busy re-tick is fixed at 5m); `0` disables retention. |
| `NAGG_RETENTION_DRY_RUN` | `false` | Log per-rule matched counts without deleting. |

## Adding a rule

Append a `Lifetime` to the registry defaults with an existing policy, or
implement `rules.LifetimePolicy.DeletePredicate` for a new shape (it must
render for any id column the targets use). Add a predicate test in
`internal/rules`, and record the measurement that justified the rule in the
rule's comment — every current rule cites one.
