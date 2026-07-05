# devnagg — read-only staging app-view

`devnagg` is a Railway service for benchmarking ClickHouse query rewrites against
**real production data** before they ship to `main`. It runs the `staging` branch
and points at the **same** ClickHouse as prod, as a strict reader.

- URL: https://devnagg-production.up.railway.app
- Project: `Sovran` · Environment: `production` · Service: `devnagg`
- Branch: `staging` (Kelbie/nagg — never `callebtc/nagg`)

## Why it is safe against prod data

devnagg shares the prod ClickHouse (`clickhouse.railway.internal:9000`). It can
only read because:

| Guard | Effect |
|---|---|
| `NAGG_SKIP_MIGRATE=true` | `cmd/migrate` returns immediately — the pre-deploy never runs CREATE/reconcile. |
| `NAGG_SCHEMA_RECONCILE=off` | Even if migrate ran, the destructive table-stripping reconcile is disabled. |
| `NAGG_RUN_INGESTER=false` / `NAGG_RUN_ENRICHER=false` | The only writers are off; the app-view only issues SELECTs. |
| `NAGG_CLICKHOUSE_MAX_OPEN_CONNS=8` / `_IDLE=4` | Polite tenant: small pool so it doesn't starve prod of CH connections. |

**Never** run `./nagg-migrate` or schema-reconcile against the prod ClickHouse
from devnagg. Schema changes (projections, materialized views) ship through
`main` (`railway.toml`, which *does* run migrate), reviewed.

Recommended hardening (not yet applied): give devnagg a dedicated `GRANT SELECT`
ClickHouse user so read-only is enforced by the database, not just config.

## Provisioned via Railway CLI

```bash
railway add --service devnagg \
  --variables NAGG_PROCESS=api \
  --variables NAGG_SKIP_MIGRATE=true \
  --variables NAGG_SCHEMA_RECONCILE=off \
  --variables NAGG_RUN_INGESTER=false \
  --variables NAGG_RUN_ENRICHER=false \
  --variables NAGG_ON_DEMAND_USER_FEED=false \
  --variables NAGG_ON_DEMAND_GRAPHQL_HYDRATION=false \
  --variables NAGG_CLICKHOUSE_ADDR=clickhouse.railway.internal:9000 \
  --variables NAGG_CLICKHOUSE_DATABASE=default \
  --variables NAGG_CLICKHOUSE_USERNAME=admin \
  --variables NAGG_CLICKHOUSE_PASSWORD=<prod CH password>
railway variables --service devnagg --set NAGG_CLICKHOUSE_MAX_OPEN_CONNS=8 --set NAGG_CLICKHOUSE_MAX_IDLE_CONNS=4
railway domain --service devnagg
```

Redis is intentionally **unset** so every request computes — clean A/B numbers.

## Deploying an iteration

```bash
git switch staging && git commit ...        # your query change
railway down --service devnagg -y            # IMPORTANT: take old container offline first
railway up   --service devnagg --ci          # deploy with NO overlap
```

**Why `down` first:** Railway's zero-downtime deploy keeps the old container
running until the new one passes its healthcheck. The new container then can't
open its ClickHouse pool (the shared CH refuses new connections from the `admin`
user while the old container + prod services hold theirs), so it boots-loops with
`connection reset by peer` and the deploy fails. Taking devnagg offline first
frees its connections so the new container boots cleanly. Staging downtime is
acceptable.

To watch the deploy:
```bash
railway logs --build | grep "exporting to docker image format"   # build done
railway status --json | jq -r '.services.edges[].node|select(.name=="devnagg")|.serviceInstances.edges[].node.latestDeployment.status'
```

### Auto-deploy on push to `staging` (one-time dashboard step)

The CLI can't bind a service to a GitHub branch. In the Railway dashboard:
devnagg → Settings → connect repo `Kelbie/nagg`, set the deploy branch to
`staging`, and (optionally) set the config-as-code path to `railway.staging.toml`
(which omits the migrate pre-deploy). After that, `git push origin staging`
redeploys automatically. Until then, deploy with the `down`/`up` flow above.

## Audit harness — `scripts/parity-check.ts`

Full prod-vs-staging audit of every REST app-view sovran-app uses (feed,
feed/user, notifications, dm/envelopes, profile, events/aggregates, thread). For each
it checks three things:

1. **Data parity** — compares the set of event ids (or stat keys) prod vs devnagg
   return and reports overlap. Both read the same ClickHouse, so overlap is ~100%;
   the only legitimate gap is prod's on-demand relay hydration (off on devnagg),
   which shows as a few `prod-only` events.
2. **Contract** — validates each devnagg response against the nagg-ts client Zod
   schema (the real "won't break clients" gate; non-zero exit on failure).
3. **Speed** — devnagg `Server-Timing` (`app`/`db`, clean compute, Redis off) vs
   prod wall (cache-warmed, context only).

Pubkeys are discovered from the live feed and bucketed by follower count (via
`/nostr/follows`) so big accounts are compared against small ones. Also runs the
notification tab/policy/replyScope matrix and thread lookups (root ids derived
from each account's most-replied note).

```bash
bun scripts/parity-check.ts                  # full audit
bun scripts/parity-check.ts --big 3 --small 3
bun scripts/parity-check.ts --big 1 --small 1   # quick
```

## Load testing & ClickHouse capacity

`--load` sweeps notification-request concurrency (1/3/5/10/all pubkeys), one request
per distinct viewer (worst case — distinct viewers don't share cache), single attempt
(no retry) so failures and latency are real:

```bash
bun scripts/parity-check.ts --load             # concurrency sweep on devnagg
bun scripts/parity-check.ts --load --reps 3
```

**Measured scaling (devnagg, Redis off, limiter=2):** 0 failures across the sweep, but
throughput plateaus at ~0.9 req/s and p50 latency grows with concurrency (2.1s @1 →
9–10s @10–13) — the limiter converts overload into queueing, not errors. **Prod
reference (old queries, no limiter): conc=3 → 0/5 succeeded, all hit the 30s timeout** —
prod currently falls over at ~3 concurrent uncached notifications (fixed by PR #9).

### Why it caps (`railway connect ClickHouse` / HTTP `system` tables)

The instance is **not** small: **32 cores, ~29.8 GiB RAM**, `max_concurrent_queries=1000`,
`max_server_memory_usage≈28.8 GB`. The problem is **memory**: `max_memory_usage=0` (no
per-query cap) and each notification query peaks **~0.8–1 GB**. A notification *request*
fans out to ~8 ClickHouse queries, so ~4 concurrent requests ≈ 4×8×0.85 GB ≈ **26 GB →
near the 28.8 GB ceiling → cgroup OOM → `connection reset by peer`**. That matches the
observed conc=2 OK / conc=4 fail.

### What was applied (and the result)

1. **Cut per-query memory (done):** removed `notification_candidates FINAL` (→ windowed
   `LIMIT 1 BY (event_id, reason)`) and both `vertex_scores FINAL` joins (→
   `argMax(score, fetched_at) GROUP BY pubkey`) in `internal/clickhouse/read.go`. Modest:
   notifications `db` p50 ≈ 550ms. Same data (ungrouped id sets match prod 100%).
2. **Bounded read-only ClickHouse user (done, server-side):** `admin` is config-defined and
   can't be `ALTER`ed, so created a SQL-managed `nagg_ro` (SELECT-only + memory/time limits);
   devnagg now connects as it. Overload now degrades **gracefully** (a per-user memory-limit
   rejection) instead of a cgroup-OOM `connection reset` that destabilises the whole server.
   ```sql
   CREATE USER nagg_ro IDENTIFIED BY '<pw>'
     SETTINGS max_memory_usage = 4000000000,        -- 4 GB per query (runaway guard)
              max_memory_usage_for_user = 22000000000, -- 22 GB total (headroom under 29.8 GB cgroup)
              max_execution_time = 28;               -- under nagg's 30s request ctx
   GRANT SELECT ON default.* TO nagg_ro;
   ```
3. **Backpressure limiter (done):** `NAGG_MAX_CONCURRENT_REQUESTS=2` on devnagg.

### Measured ceiling (the honest result)

Even after 1–3, the shared CH caps at **~2 concurrent heavy requests**: limiter=2 → 100% across
the whole sweep (~0.96 req/s, latency grows with queueing); limiter=4 → failures return when the
CH is busy (conc=3 dropped to 50% in one run, 100% when idle). The ceiling is **variable**
because devnagg shares the CH with prod + ingestion + merges, so available memory headroom
fluctuates. A fixed limiter can't adapt — **2 is the safe floor on the shared instance**.

### To actually serve hundreds of users (remaining levers)

- **Redis** (prod has it) — the primary lever: collapses repeat reads so most requests never
  touch CH. Distinct cold viewers still cost, so the limiter + headroom still matter.
- **More RAM on ClickHouse** — the only way to raise the ~2-concurrent ceiling materially;
  ~0.85 GB/query × ~8 queries/request means 30 GB only fits ~2–3 requests. Doubling RAM ≈
  doubles safe concurrency.
- **Apply to prod:** point prod `nagg` at `nagg_ro` (read-only + graceful limits) and set
  `NAGG_MAX_CONCURRENT_REQUESTS` (+ merge PR #9 so prod gets the faster queries — prod currently
  times out at conc=3).
- **Dedicated dev ClickHouse** — removes the prod-shared variance; needed for clean, repeatable
  load testing and so load tests stop taxing prod.

Note: `--load` exercises the **shared** prod ClickHouse, so keep runs bounded.
