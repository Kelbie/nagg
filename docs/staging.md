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
feed/user, notifications, dm/envelopes, profile, notes/stats, thread). For each
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

### Recommendation (priority order)

1. **Cut per-query memory (app-side, our control):** remove the remaining `FINAL` in the
   notifications query (`notification_candidates FINAL`, `vertex_scores FINAL` in
   `internal/clickhouse/read.go`) — same `LIMIT 1 BY`/`argMax` technique. Lower memory/query
   raises safe concurrency on the *same* instance. Validate with `--load`.
2. **Set a per-query memory guardrail (CH config, affects prod — recommend, don't apply blind):**
   `max_memory_usage` ≈ 4 GB. Over-budget queries then fail with a clean "memory limit
   exceeded" instead of OOM-resetting the whole server (which takes prod down too).
3. **Backpressure limiter** (`NAGG_MAX_CONCURRENT_REQUESTS`, shipped) sized to the memory
   budget: with ~0.85 GB/query × ~8/request, ≈3 concurrent requests fit under 28.8 GB →
   keep devnagg at 2–3. Set it on prod too.
4. **Redis** (prod) collapses repeat reads — the main lever for many *returning* users, but
   not for many *distinct* cold viewers (so 1–3 still matter).
5. **More RAM** only if 1–4 don't reach the target concurrency — likely unnecessary first.

Note: `--load` exercises the **shared** prod ClickHouse, so keep runs bounded; a dedicated
dev ClickHouse is worth it if sustained load testing becomes routine.
