#!/usr/bin/env bun
/**
 * bench-appview-vs-graphql.ts — repeatable latency benchmark of nagg's REST
 * app-view routes against their closest GraphQL equivalents, run against a
 * live deployment (default: Railway prod). No Redis response cache is
 * deployed there, so timings are honest server + ClickHouse work.
 *
 * Pairs (see docs/bench-appview-vs-graphql.md for the semantic-parity notes —
 * the two sides are NOT doing identical work; that asymmetry is the point):
 *
 *   1. POST /nostr/feed (pubkeys spec)      vs  GraphQL events(input:{pubkeys, kinds:[1,6,16], limit})
 *      REST hydrates reposts/roots/quotes/profiles + aggregates; the GraphQL
 *      side is the raw-query floor (event rows only).
 *   2. POST /nostr/events/aggregates {ids}  vs  GraphQL 4× aliased aggregateEvents
 *      (k7 / k6+16 / k1-q / k9735 counts over the same ~20 real ids).
 *   3. POST /nostr/feed/ranked              vs  GraphQL rankedEvents(input) — SAME input
 *      map on both sides (the REST route feeds it to the shared ranker), but
 *      REST then enriches into a full feed envelope.
 *   4. GET /nostr/thread?id=                vs  GraphQL event(id){ referencedBy(e-tag) }
 *      REST adds server ranking + aggregates + profile hydration; GraphQL is
 *      a flat reverse-reference page.
 *
 * Method: request shapes are DISCOVERED up front from live data (active
 * pubkeys → feed ids → most-replied thread root), then frozen. Per target:
 * WARMUP unrecorded requests + RUNS timed requests, strictly sequential, the
 * exact same request every time (no randomization). Client wall-clock ms via
 * performance.now(); REST Server-Timing headers captured as supplementary
 * server-side (`app`) time. Reports p50/p95/max + response size as a
 * markdown table.
 *
 * Usage:
 *   bun scripts/bench-appview-vs-graphql.ts                # 5 warmup + 30 runs (default)
 *   bun scripts/bench-appview-vs-graphql.ts --runs 10 --warmup 2   # quick pass
 *   bun scripts/bench-appview-vs-graphql.ts --base https://other-nagg.example
 *
 * Read-only; keeps total load polite (~280 sequential requests at defaults).
 */

const args = process.argv.slice(2);
const flag = (name: string, fallback: string): string => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 && args[i + 1] ? args[i + 1] : fallback;
};

const BASE = flag("base", "https://nagg-production.up.railway.app").replace(/\/$/, "");
const RUNS = Number(flag("runs", "30"));
const WARMUP = Number(flag("warmup", "5"));
// The app-view handler rate-limits clients to 120 req/min (internal/appview/
// handler.go), so pace every request to stay safely under it. 429s are waited
// out and retried without being recorded.
const PACE_MS = Number(flag("pace", "550"));
const RATE_LIMIT_BACKOFF_MS = 20_000;
const FEED_LIMIT = 20;
const THREAD_LIMIT = 50;
const AGGREGATE_IDS = 20;

// ---------------------------------------------------------------------------
// HTTP helpers
// ---------------------------------------------------------------------------

interface Timed {
  ms: number;
  bytes: number;
  status: number;
  serverTiming: string | null;
  body: any;
}

async function timedFetch(path: string, init?: RequestInit): Promise<Timed> {
  const start = performance.now();
  const res = await fetch(`${BASE}${path}`, init);
  const text = await res.text();
  const ms = performance.now() - start;
  let body: any = null;
  try {
    body = JSON.parse(text);
  } catch {
    /* leave null; status/bytes still recorded */
  }
  return {
    ms,
    bytes: new TextEncoder().encode(text).length,
    status: res.status,
    serverTiming: res.headers.get("server-timing"),
    body,
  };
}

const postJSON = (path: string, payload: unknown) =>
  timedFetch(path, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify(payload),
  });

const graphql = (query: string) => postJSON("/graphql", { query });

function assertOk(label: string, t: Timed): void {
  if (t.status !== 200) {
    throw new Error(`${label}: HTTP ${t.status}`);
  }
  if (t.body?.errors?.length) {
    throw new Error(`${label}: GraphQL errors: ${JSON.stringify(t.body.errors[0]?.message)}`);
  }
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** Paced request: waits PACE_MS after each call and retries (unrecorded) on 429. */
async function pacedRequest(label: string, request: () => Promise<Timed>): Promise<Timed> {
  for (let attempt = 0; ; attempt++) {
    const t = await request();
    await sleep(PACE_MS);
    if (t.status === 429) {
      if (attempt >= 5) throw new Error(`${label}: still rate-limited after ${attempt} retries`);
      console.error(`  ${label}: 429, backing off ${RATE_LIMIT_BACKOFF_MS / 1000}s`);
      await sleep(RATE_LIMIT_BACKOFF_MS);
      continue;
    }
    assertOk(label, t);
    return t;
  }
}

// ---------------------------------------------------------------------------
// Stats
// ---------------------------------------------------------------------------

function percentile(sorted: number[], p: number): number {
  if (sorted.length === 0) return NaN;
  const idx = Math.min(sorted.length - 1, Math.ceil((p / 100) * sorted.length) - 1);
  return sorted[Math.max(0, idx)];
}

/** Parse the `app` component out of a Server-Timing header ("app;dur=68.5, db;dur=16.4"). */
function serverAppMs(header: string | null): number | null {
  if (!header) return null;
  const m = header.match(/(?:^|,)\s*app;dur=([0-9.]+)/);
  return m ? Number(m[1]) : null;
}

interface BenchResult {
  name: string;
  transport: "REST" | "GraphQL";
  runs: number;
  p50: number;
  p95: number;
  max: number;
  bytes: number; // median
  serverP50: number | null; // median Server-Timing app dur (REST only)
}

async function bench(
  name: string,
  transport: "REST" | "GraphQL",
  request: () => Promise<Timed>,
): Promise<BenchResult> {
  for (let i = 0; i < WARMUP; i++) {
    await pacedRequest(`${name} warmup`, request);
  }
  const samples: number[] = [];
  const sizes: number[] = [];
  const serverMs: number[] = [];
  for (let i = 0; i < RUNS; i++) {
    const t = await pacedRequest(`${name} run ${i}`, request);
    samples.push(t.ms);
    sizes.push(t.bytes);
    const app = serverAppMs(t.serverTiming);
    if (app !== null) serverMs.push(app);
  }
  samples.sort((a, b) => a - b);
  sizes.sort((a, b) => a - b);
  serverMs.sort((a, b) => a - b);
  const result: BenchResult = {
    name,
    transport,
    runs: RUNS,
    p50: percentile(samples, 50),
    p95: percentile(samples, 95),
    max: samples[samples.length - 1],
    bytes: percentile(sizes, 50),
    serverP50: serverMs.length ? percentile(serverMs, 50) : null,
  };
  console.error(
    `  done ${name}: p50 ${result.p50.toFixed(0)}ms p95 ${result.p95.toFixed(0)}ms ` +
      `max ${result.max.toFixed(0)}ms ~${(result.bytes / 1024).toFixed(1)}KB` +
      (result.serverP50 !== null ? ` (server app p50 ${result.serverP50.toFixed(1)}ms)` : ""),
  );
  return result;
}

// ---------------------------------------------------------------------------
// Discovery — derive one frozen request shape per target from live data
// ---------------------------------------------------------------------------

async function discover() {
  console.error(`Discovering request shapes from ${BASE} …`);

  // Active authors: most recent kind-1 events.
  const recent = await pacedRequest("discovery events", () =>
    graphql(`query { events(input:{kinds:[1], limit:30}) { nodes { id pubkey } } }`),
  );
  const counts = new Map<string, number>();
  for (const n of recent.body.data.events.nodes) {
    counts.set(n.pubkey, (counts.get(n.pubkey) ?? 0) + 1);
  }
  const pubkeys = [...counts.entries()]
    .sort((a, b) => b[1] - a[1])
    .slice(0, 5)
    .map(([pk]) => pk);
  if (pubkeys.length === 0) throw new Error("no active pubkeys found");

  // Feed page for those authors → real event ids for the aggregates pair.
  const feed = await pacedRequest("discovery feed", () =>
    postJSON("/nostr/feed", { spec: JSON.stringify({ pubkeys }), limit: FEED_LIMIT }),
  );
  const ids: string[] = (feed.body.order ?? []).slice(0, AGGREGATE_IDS);
  if (ids.length === 0) throw new Error("feed returned no event ids");

  // Most-replied event → thread root.
  const replied = await pacedRequest("discovery thread root", () =>
    graphql(
      `query { aggregateEvents(input:{dataset:"TAGS", groupBy:["TAG_VALUE"], kinds:[1,1111], tags:[{key:"e"}], metrics:["UNIQUE_EVENTS"], limit:1}) { rows { dimensions } } }`,
    ),
  );
  const threadRoot: string | undefined =
    replied.body.data.aggregateEvents.rows[0]?.dimensions?.tag_value;
  if (!threadRoot) throw new Error("no thread root found");

  console.error(
    `  authors=${pubkeys.length} feedIds=${ids.length} threadRoot=${threadRoot.slice(0, 8)}…`,
  );
  return { pubkeys, ids, threadRoot };
}

// ---------------------------------------------------------------------------
// Targets
// ---------------------------------------------------------------------------

function targets(pubkeys: string[], ids: string[], threadRoot: string) {
  const quoted = ids.map((id) => `"${id}"`).join(",");

  // 3. Ranked: the SAME input map goes to both transports (the REST route
  // decodes its body into the map the GraphQL rankedEvents field accepts).
  const rankedInput = {
    references: { kinds: [7, 6, 16, 9735], since: 1704067200 },
    via: { key: "e" },
    target: { kinds: [1] },
    metric: { name: "engagers", op: "COUNT_DISTINCT", distinctField: "PUBKEY" },
    limit: FEED_LIMIT,
  };
  const rankedGQL = `query {
    rankedEvents(input:{
      references:{kinds:[7,6,16,9735], since:1704067200}
      via:{key:"e"}
      target:{kinds:[1]}
      metric:{name:"engagers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
      limit:${FEED_LIMIT}
    }) { nodes { id pubkey kind createdAt content tags } }
  }`;

  // 2. Aggregates: REST computes every registry rule for the ids in one call;
  // the GraphQL prototype needs one aliased aggregation per rule family.
  const aggregatesGQL = `query {
    likes: aggregateEvents(input:{dataset:"TAGS", groupBy:["TAG_VALUE"], kinds:[7], tags:[{key:"e", values:[${quoted}]}], metrics:["UNIQUE_PUBKEYS"], limit:100}) { rows { dimensions metrics } }
    reposts: aggregateEvents(input:{dataset:"TAGS", groupBy:["TAG_VALUE"], kinds:[6,16], tags:[{key:"e", values:[${quoted}]}], metrics:["UNIQUE_PUBKEYS"], limit:100}) { rows { dimensions metrics } }
    quotes: aggregateEvents(input:{dataset:"TAGS", groupBy:["TAG_VALUE"], kinds:[1], tags:[{key:"q", values:[${quoted}]}], metrics:["UNIQUE_EVENTS"], limit:100}) { rows { dimensions metrics } }
    zaps: aggregateEvents(input:{dataset:"TAGS", groupBy:["TAG_VALUE"], kinds:[9735], tags:[{key:"e", values:[${quoted}]}], metrics:["COUNT"], limit:100}) { rows { dimensions metrics } }
  }`;

  // 1. Feed floor: same authors + kinds as the REST FollowsFeed query.
  const feedGQL = `query {
    events(input:{pubkeys:[${pubkeys.map((p) => `"${p}"`).join(",")}], kinds:[1,6,16], limit:${FEED_LIMIT}}) {
      nodes { id pubkey kind createdAt content tags }
    }
  }`;

  // 4. Thread: flat reverse-reference page over the root's e-tag.
  const threadGQL = `query {
    event(id:"${threadRoot}") {
      id pubkey kind createdAt content tags
      referencedBy(input:{via:{key:"e"}, events:{kinds:[1,1111], limit:${THREAD_LIMIT}}}) {
        nodes { id pubkey kind createdAt content tags }
      }
    }
  }`;

  return [
    {
      // Network floor: a light route with near-zero server work, so wall-clock
      // numbers below can be read as RTT + server time.
      name: `baseline: GET /nostr/capabilities`,
      transport: "REST" as const,
      request: () => timedFetch("/nostr/capabilities"),
    },
    {
      name: `feed: POST /nostr/feed (${pubkeys.length} authors, limit ${FEED_LIMIT})`,
      transport: "REST" as const,
      request: () => postJSON("/nostr/feed", { spec: JSON.stringify({ pubkeys }), limit: FEED_LIMIT }),
    },
    {
      name: `feed floor: GraphQL events (same authors/kinds/limit)`,
      transport: "GraphQL" as const,
      request: () => graphql(feedGQL),
    },
    {
      name: `aggregates: POST /nostr/events/aggregates (${ids.length} ids)`,
      transport: "REST" as const,
      request: () => postJSON("/nostr/events/aggregates", { ids }),
    },
    {
      name: `aggregates: GraphQL 4× aggregateEvents (same ${ids.length} ids)`,
      transport: "GraphQL" as const,
      request: () => graphql(aggregatesGQL),
    },
    {
      name: `ranked: POST /nostr/feed/ranked (limit ${FEED_LIMIT})`,
      transport: "REST" as const,
      request: () => postJSON("/nostr/feed/ranked", rankedInput),
    },
    {
      name: `ranked: GraphQL rankedEvents (identical input)`,
      transport: "GraphQL" as const,
      request: () => graphql(rankedGQL),
    },
    {
      name: `thread: GET /nostr/thread (limit ${THREAD_LIMIT})`,
      transport: "REST" as const,
      request: () => timedFetch(`/nostr/thread?id=${threadRoot}&limit=${THREAD_LIMIT}`),
    },
    {
      name: `thread: GraphQL event + referencedBy (limit ${THREAD_LIMIT})`,
      transport: "GraphQL" as const,
      request: () => graphql(threadGQL),
    },
  ];
}

// ---------------------------------------------------------------------------
// Main
// ---------------------------------------------------------------------------

const { pubkeys, ids, threadRoot } = await discover();
const plan = targets(pubkeys, ids, threadRoot);

console.error(
  `Benchmarking ${plan.length} targets × (${WARMUP} warmup + ${RUNS} timed), sequential …`,
);
const results: BenchResult[] = [];
for (const t of plan) {
  results.push(await bench(t.name, t.transport, t.request));
}

// Markdown table on stdout (stderr carries progress), ready to paste into docs.
console.log(`\nBase: ${BASE}  (runs=${RUNS}, warmup=${WARMUP}, sequential, fixed requests)\n`);
console.log("| Target | Transport | p50 (ms) | p95 (ms) | max (ms) | ~size | server app p50 (ms) |");
console.log("| --- | --- | ---: | ---: | ---: | ---: | ---: |");
for (const r of results) {
  const size = r.bytes >= 1024 ? `${(r.bytes / 1024).toFixed(1)} KB` : `${r.bytes} B`;
  const server = r.serverP50 !== null ? r.serverP50.toFixed(1) : "—";
  console.log(
    `| ${r.name} | ${r.transport} | ${r.p50.toFixed(0)} | ${r.p95.toFixed(0)} | ${r.max.toFixed(0)} | ${size} | ${server} |`,
  );
}
