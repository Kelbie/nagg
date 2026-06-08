#!/usr/bin/env bun
/**
 * parity-check.ts — full prod-vs-staging audit of every nagg app-view endpoint
 * sovran-app uses. For each endpoint it checks three things:
 *
 *   1. DATA PARITY  — does devnagg return the same data as prod? We compare the
 *      set of event ids (or stat keys / connection nodes) returned by each and
 *      report overlap. Both read the SAME ClickHouse, so overlap should be ~100%;
 *      the only legitimate gap is prod's on-demand relay hydration (off on
 *      devnagg), which can add a few prod-only events.
 *   2. CONTRACT     — does devnagg still validate against the nagg-ts client Zod
 *      schema (won't break shipped clients).
 *   3. SPEED        — devnagg Server-Timing (app/db, clean compute, Redis off)
 *      vs prod wall (cache-warmed, shown only for context).
 *
 * Pubkeys are DISCOVERED from the live feed and BUCKETED by follower count
 * (via /nostr/follows) so we compare big accounts against small ones without
 * hardcoding. Plus a notification policy/tab/replyScope matrix and thread
 * lookups (root ids derived from each account's most-replied note).
 *
 * Endpoints covered (the REST app-views sovran-app/nagg-ts call): feed,
 * feed/user, notifications, dm/envelopes, profile, notes/stats, thread.
 *
 * Usage:
 *   bun scripts/parity-check.ts                 # full audit
 *   bun scripts/parity-check.ts --big 3 --small 3
 *   bun scripts/parity-check.ts --big 1 --small 1   # quick
 *
 * Exit code is non-zero if any devnagg response fails the client contract.
 */

import {
  NaggFeedPageSchema,
  NaggNotificationsPageSchema,
  NaggDmEnvelopesDataSchema,
  NaggThreadSchema,
  NaggNoteStatsSchema,
} from "../../nagg-ts/src/schemas";
import type { ZodTypeAny } from "../../nagg-ts/node_modules/zod";

const PROD = process.env.PROD_BASE ?? "https://nagg.up.railway.app";
const DEV = process.env.DEV_BASE ?? "https://devnagg-production.up.railway.app";

const arg = (name: string, def: number) => {
  const i = process.argv.indexOf(`--${name}`);
  return i >= 0 ? Number(process.argv[i + 1]) : def;
};
const N_BIG = arg("big", 3);
const N_SMALL = arg("small", 3);

// Seeds: verification npub + well-known accounts. Discovery expands this.
const VERIFY = "c673ff0b5f228feb0abb1001882178d4c588bc4e50f857173544b5543b454f81";
const SEEDS = [
  VERIFY,
  "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2", // jack
  "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d", // fiatjaf
  "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93", // gigi
  "32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245", // jb55
  "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9", // odell
  "97c70a44366a6535c145b333f973ea86dfdc2d7a99da618c40c64705ad98e322", // hodlbod
  // User-supplied accounts (mix of follower sizes) for broader coverage.
  "1e53e900c3bbc5ead295215efe27b2c8d5fbd15fb3dd810da3063674cb7213b2",
  "ddf03aca85ade039e6742d5bef3df352df199d0d31e22b9858e7eda85cb3bbbe",
  "1021c8921548fa89abb4cc7e8668a3a8dcebae0a4c323ffeaf570438832d6993",
  "805b34f708837dfb3e7f05815ac5760564628b58d5a0ce839ccbb6ef3620fac3",
  "1afe0c74e3d7784eba93a5e3fa554a6eeb01928d12739ae8ba4832786808e36d",
  "c48e29f04b482cc01ca1f9ef8c86ef8318c059e0e9353235162f080f26e14c11",
];

type Req = { method?: "GET" | "POST"; body?: unknown };
type Resp = { ms: number; status: number; timing: Record<string, number>; body: any };

async function call(base: string, path: string, req: Req = {}): Promise<Resp> {
  let last: Resp = { ms: 0, status: 0, timing: {}, body: null };
  for (let attempt = 0; attempt < 3; attempt++) {
    const t0 = performance.now();
    try {
      const res = await fetch(base + path, {
        method: req.method ?? "GET",
        headers: { accept: "application/json", "content-type": "application/json" },
        body: req.body ? JSON.stringify(req.body) : undefined,
      });
      const body = await res.json().catch(() => null);
      const timing: Record<string, number> = {};
      const h = res.headers.get("server-timing");
      if (h) for (const seg of h.split(",")) {
        const m = seg.trim().match(/^([a-z]+);dur=([0-9.]+)/i);
        if (m) timing[m[1]] = parseFloat(m[2]);
      }
      last = { ms: performance.now() - t0, status: res.status, timing, body };
      if (res.status >= 200 && res.status < 300 && body) return last;
    } catch {
      last = { ms: performance.now() - t0, status: 0, timing: {}, body: null };
    }
  }
  return last;
}

// ---- id / value extraction per endpoint ------------------------------------

const feedIds = (b: any): string[] =>
  (b?.items ?? [])
    .map((i: any) => i?.event?.id ?? i?.repostEvent?.id ?? i?.originalEvent?.id)
    .filter(Boolean);
const notifIds = (b: any): string[] =>
  (b?.notifications?.nodes ?? []).map((n: any) => n?.event?.id).filter(Boolean);
const dmIds = (b: any): string[] => (b?.dmEnvelopes?.nodes ?? []).map((n: any) => n?.id).filter(Boolean);
const threadIds = (b: any): string[] =>
  [b?.root?.id, ...((b?.events ?? []).map((e: any) => e?.id))].filter(Boolean);
const statKeys = (b: any): string[] => (b && typeof b === "object" ? Object.keys(b) : []);

function overlap(a: string[], b: string[]): { pct: number; inter: number; prodOnly: number; devOnly: number } {
  const A = new Set(a), B = new Set(b);
  const inter = [...A].filter((x) => B.has(x)).length;
  const union = new Set([...a, ...b]).size;
  return {
    pct: union === 0 ? 100 : Math.round((inter / union) * 100),
    inter,
    prodOnly: [...A].filter((x) => !B.has(x)).length,
    devOnly: [...B].filter((x) => !A.has(x)).length,
  };
}

function validate(schema: ZodTypeAny, body: unknown): string {
  const r = schema.safeParse(body);
  if (r.success) return "✓";
  const f = r.error.issues[0];
  return `✗ ${f.path.join(".") || "<root>"}: ${f.message}`;
}

const ms = (n?: number) => (n === undefined ? "   - " : `${n.toFixed(0)}`.padStart(5));
const failures: string[] = [];

// ---- discover + bucket pubkeys ---------------------------------------------

async function discoverPubkeys(): Promise<Map<string, number>> {
  const candidates = new Set<string>(SEEDS);
  // Pull authors from a few seeds' feeds to add real accounts of varied sizes.
  for (const seed of SEEDS.slice(1, 4)) {
    const r = await call(DEV, `/nostr/feed?pubkeys=${seed}&limit=60`);
    for (const item of r.body?.items ?? []) {
      const pk = item?.event?.pubkey ?? item?.repostEvent?.pubkey;
      if (pk) candidates.add(pk);
    }
    for (const pk of Object.keys(r.body?.profiles ?? {})) candidates.add(pk);
  }
  // Follower count per candidate (devnagg) → size.
  const sizes = new Map<string, number>();
  for (const pk of candidates) {
    const r = await call(DEV, `/nostr/follows?pubkey=${pk}`);
    sizes.set(pk, Number(r.body?.followers ?? 0));
  }
  return sizes;
}

// ---- per-endpoint audit row ------------------------------------------------

type Row = {
  ep: string;
  pk: string;
  devApp?: number;
  devDb?: number;
  prodMs: number;
  count: number;
  overlapPct: number;
  prodOnly: number;
  devOnly: number;
  contract: string;
};

async function auditEndpoint(
  ep: string,
  path: string,
  pk: string,
  extract: (b: any) => string[],
  schema?: ZodTypeAny,
  req: Req = {},
): Promise<Row> {
  const dev = await call(DEV, path, req);
  const prod = await call(PROD, path, req);
  const ov = overlap(extract(prod.body), extract(dev.body));
  const contract = schema
    ? dev.status === 200 ? validate(schema, dev.body) : `✗ HTTP ${dev.status}`
    : dev.status === 200 ? "✓" : `✗ HTTP ${dev.status}`;
  if (contract.startsWith("✗")) failures.push(`${ep} ${pk.slice(0, 8)}: ${contract}`);
  return {
    ep, pk,
    devApp: dev.timing.app, devDb: dev.timing.db,
    prodMs: prod.ms, count: extract(dev.body).length,
    overlapPct: ov.pct, prodOnly: ov.prodOnly, devOnly: ov.devOnly, contract,
  };
}

function printRows(title: string, rows: Row[], labels: Map<string, string>) {
  console.log(`\n━━ ${title} ━━`);
  console.log(`${"account".padEnd(20)} ${"devApp".padStart(6)} ${"devDb".padStart(6)} ${"prod".padStart(6)} ${"n".padStart(4)} ${"overlap".padStart(8)} ${"p-only".padStart(6)} contract`);
  for (const r of rows) {
    const lab = labels.get(r.pk) ?? r.pk.slice(0, 10);
    console.log(
      `${lab.padEnd(20)} ${ms(r.devApp)} ${ms(r.devDb)} ${ms(r.prodMs)} ${String(r.count).padStart(4)} ${(`${r.overlapPct}%`).padStart(8)} ${String(r.prodOnly).padStart(6)} ${r.contract}`,
    );
  }
}

// ---- main ------------------------------------------------------------------

console.log(`prod=${PROD}\ndev =${DEV}\nDiscovering + bucketing pubkeys…`);
const sizes = await discoverPubkeys();
const ranked = [...sizes.entries()].sort((a, b) => b[1] - a[1]);
const big = ranked.filter(([, n]) => n >= 1000).slice(0, N_BIG).map(([pk]) => pk);
const smallPool = ranked.filter(([, n]) => n > 0 && n < 1000).map(([pk]) => pk);
const small = smallPool.slice(-N_SMALL); // smallest few
const selected = [...new Set([VERIFY, ...big, ...small])];

const labels = new Map<string, string>();
const named: Record<string, string> = {
  [VERIFY]: "verify", [SEEDS[1]]: "jack", [SEEDS[2]]: "fiatjaf", [SEEDS[3]]: "gigi",
  [SEEDS[4]]: "jb55", [SEEDS[5]]: "odell", [SEEDS[6]]: "hodlbod",
};
for (const pk of selected) {
  const nm = named[pk] ?? pk.slice(0, 8);
  labels.set(pk, `${nm}(${sizes.get(pk) ?? 0}f)`);
}
const isBig = new Set(big);

console.log(`\nDiscovered ${sizes.size} candidates. Auditing ${selected.length}:`);
for (const pk of selected) console.log(`  ${labels.get(pk)}  ${isBig.has(pk) ? "BIG" : "small"}`);

// Endpoint matrix (the REST app-views sovran-app uses).
const feedRows: Row[] = [];
const userFeedRows: Row[] = [];
const notifRows: Row[] = [];
const dmRows: Row[] = [];
const profileRows: Row[] = [];
const statsRows: Row[] = [];
const threadRows: Row[] = [];

for (const pk of selected) {
  feedRows.push(await auditEndpoint("feed", `/nostr/feed?pubkeys=${pk}&limit=30`, pk, feedIds, NaggFeedPageSchema));
  userFeedRows.push(await auditEndpoint("feed/user", `/nostr/feed/user?pubkey=${pk}&limit=30`, pk, feedIds, NaggFeedPageSchema));
  notifRows.push(await auditEndpoint("notif", `/nostr/notifications?pubkey=${pk}&limit=50`, pk, notifIds, NaggNotificationsPageSchema));
  dmRows.push(await auditEndpoint("dm", `/nostr/dm/envelopes?viewer=${pk}&limit=50`, pk, dmIds, NaggDmEnvelopesDataSchema));
  profileRows.push(await auditEndpoint("profile", `/nostr/profile?pubkey=${pk}`, pk, () => [], undefined));

  // notes/stats + thread: derive ids from this account's own feed.
  const ownFeed = await call(DEV, `/nostr/feed/user?pubkey=${pk}&limit=40`);
  const noteIds = (ownFeed.body?.items ?? []).map((i: any) => i?.event?.id).filter(Boolean).slice(0, 25);
  if (noteIds.length > 0) {
    statsRows.push(await auditEndpoint("notes/stats", `/nostr/notes/stats`, pk, statKeys, NaggNoteStatsSchema, { method: "POST", body: { ids: noteIds } }));
    // thread root = the note with the most replies (best exercises thread depth).
    const stats = await call(DEV, `/nostr/notes/stats`, { method: "POST", body: { ids: noteIds } });
    let rootId = noteIds[0], best = -1;
    for (const id of noteIds) {
      const replies = Number(stats.body?.[id]?.replies ?? 0);
      if (replies > best) { best = replies; rootId = id; }
    }
    threadRows.push(await auditEndpoint("thread", `/nostr/thread?id=${rootId}&limit=100`, pk, threadIds, NaggThreadSchema));
  }
}

printRows("feed (GET /nostr/feed)", feedRows, labels);
printRows("feed/user (GET /nostr/feed/user)", userFeedRows, labels);
printRows("notifications (GET /nostr/notifications)", notifRows, labels);
printRows("dm/envelopes (GET /nostr/dm/envelopes)", dmRows, labels);
printRows("profile (GET /nostr/profile)", profileRows, labels);
printRows("notes/stats (POST /nostr/notes/stats)", statsRows, labels);
printRows("thread (GET /nostr/thread)", threadRows, labels);

// ---- notification policy / tab / replyScope matrix -------------------------
// The full filter matrix the app's notifications tab exposes, on one big + one
// small account, to confirm both servers agree and to time each filter.
const matrixPks = [big[0], small[small.length - 1]].filter(Boolean);
const combos = [
  { tab: "ALL", policy: "STRICT", replyScope: "THREAD" },
  { tab: "ALL", policy: "RELAXED", replyScope: "THREAD" },
  { tab: "ALL", policy: "MODERATE", replyScope: "THREAD" },
  { tab: "ALL", policy: "FOLLOWS", replyScope: "THREAD" },
  { tab: "ALL", policy: "STRICT", replyScope: "DIRECT" },
  { tab: "MENTIONS", policy: "STRICT", replyScope: "THREAD" },
];
console.log(`\n━━ notifications policy matrix ━━`);
console.log(`${"account".padEnd(20)} ${"tab".padEnd(9)} ${"policy".padEnd(9)} ${"reply".padEnd(7)} ${"devApp".padStart(6)} ${"devDb".padStart(6)} ${"n".padStart(4)} ${"overlap".padStart(8)} contract`);
for (const pk of matrixPks) {
  for (const c of combos) {
    const path = `/nostr/notifications?pubkey=${pk}&limit=50&tab=${c.tab}&policy=${c.policy}&replyScope=${c.replyScope}`;
    const r = await auditEndpoint("notif-matrix", path, pk, notifIds, NaggNotificationsPageSchema);
    console.log(
      `${(labels.get(pk) ?? "").padEnd(20)} ${c.tab.padEnd(9)} ${c.policy.padEnd(9)} ${c.replyScope.padEnd(7)} ${ms(r.devApp)} ${ms(r.devDb)} ${String(r.count).padStart(4)} ${(`${r.overlapPct}%`).padStart(8)} ${r.contract}`,
    );
  }
}

// ---- summaries -------------------------------------------------------------
function avg(rows: Row[], pick: (r: Row) => number | undefined, filt: (r: Row) => boolean) {
  const v = rows.filter(filt).map(pick).filter((x): x is number => x !== undefined);
  return v.length ? Math.round(v.reduce((a, b) => a + b, 0) / v.length) : 0;
}
const allRows = [...feedRows, ...userFeedRows, ...notifRows, ...dmRows, ...statsRows, ...threadRows];
const groups: [string, Row[]][] = [
  ["feed", feedRows], ["feed/user", userFeedRows], ["notif", notifRows],
  ["dm", dmRows], ["notes/stats", statsRows], ["thread", threadRows],
];
console.log(`\n━━ big vs small (avg devnagg app ms) ━━`);
console.log(`${"endpoint".padEnd(14)} ${"BIG".padStart(7)} ${"small".padStart(7)}`);
for (const [name, rows] of groups) {
  console.log(`${name.padEnd(14)} ${String(avg(rows, (r) => r.devApp, (r) => isBig.has(r.pk))).padStart(7)} ${String(avg(rows, (r) => r.devApp, (r) => !isBig.has(r.pk))).padStart(7)}`);
}

const lowOverlap = allRows.filter((r) => r.count > 0 && r.overlapPct < 70);
const populated = allRows.filter((r) => r.count > 0);
const avgOverlap = Math.round(populated.reduce((a, r) => a + r.overlapPct, 0) / Math.max(1, populated.length));
console.log(`\n━━ summary ━━`);
console.log(`data overlap (prod∩dev): avg ${avgOverlap}% across ${populated.length} populated responses`);
if (lowOverlap.length) {
  console.log(`⚠️  ${lowOverlap.length} responses <70% overlap (check on-demand hydration / pagination):`);
  for (const r of lowOverlap) console.log(`   ${r.ep} ${labels.get(r.pk)}: ${r.overlapPct}% (prod-only ${r.prodOnly}, dev-only ${r.devOnly})`);
}
console.log(failures.length ? `❌ contract: ${failures.length} failure(s):\n   ${failures.join("\n   ")}` : `✅ contract: every devnagg response validates against the nagg-ts client schema`);
process.exit(failures.length ? 1 : 0);
