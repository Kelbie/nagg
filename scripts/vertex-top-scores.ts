#!/usr/bin/env bun
/**
 * vertex-top-scores: rank the most-followed pubkeys through the self-hosted
 * Vertex instance.
 *
 * 1. Discovery — asks nagg's generic aggregation for the top N pubkeys by
 *    distinct kind-3 followers (tag_value of p tags, grouped, UNIQUE_PUBKEYS):
 *    the "biggest accounts" as our own ClickHouse sees them.
 * 2. Scoring — POSTs those pubkeys to the Vertex Open-Ranking HTTP API
 *    (ORE-03 /rank/pubkeys), which serves global pagerank from the crawler's
 *    walk store. Credit-free, IP-rate-limited only.
 *
 * Usage: bun scripts/vertex-top-scores.ts [--n 20]
 *        [--nagg https://nagg-production.up.railway.app]
 *        [--vertex https://vertex-production-3ea6.up.railway.app]
 */
const arg = (name: string, dflt: string) => {
  const i = process.argv.indexOf(`--${name}`);
  return i > 0 && process.argv[i + 1] ? process.argv[i + 1] : dflt;
};
const N = Number(arg("n", "20"));
const NAGG = arg("nagg", "https://nagg-production.up.railway.app");
const VERTEX = arg("vertex", "https://vertex-production-3ea6.up.railway.app");

// 1. Top-N most-followed pubkeys, from nagg's generic aggregation.
const gq = await fetch(`${NAGG}/graphql`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({
    query: `{ aggregateEvents(input:{ kinds:[3], dataset:"TAGS", tags:[{key:"p"}],
      groupBy:["tag_value"], metrics:["UNIQUE_PUBKEYS"], limit:${N} })
      { rows { dimensions metrics } } }`,
  }),
}).then((r) => r.json());
const rows: { pubkey: string; followers: number }[] =
  (gq as any).data.aggregateEvents.rows.map((r: any) => ({
    pubkey: r.dimensions.tag_value,
    followers: r.metrics.unique_pubkeys,
  }));

// 2. Score them through the self-hosted Vertex Open-Ranking API.
const ranked = await fetch(`${VERTEX}/rank/pubkeys`, {
  method: "POST",
  headers: { "Content-Type": "application/json" },
  body: JSON.stringify({ pubkeys: rows.map((r) => r.pubkey) }),
}).then((r) => r.json());
const rankBy = new Map<string, number>(
  ((ranked as any).results ?? []).map((x: any) => [x.pubkey, x.rank]),
);

// 3. Resolve display names from nagg (kind-0 events via the profiles route).
const prof = await fetch(
  `${NAGG}/nostr/profiles?pubkeys=${rows.map((r) => r.pubkey).join(",")}`,
).then((r) => r.json());
const nameBy = new Map<string, string>();
for (const ev of (prof as any).events ?? []) {
  if (ev.kind !== 0) continue;
  try {
    const meta = JSON.parse(ev.content);
    nameBy.set(ev.pubkey, meta.display_name || meta.name || "");
  } catch {}
}

console.log(`top ${rows.length} by distinct kind-3 followers (nagg) → vertex global pagerank\n`);
console.log("rank_score  followers  pubkey            name");
for (const r of rows) {
  const score = rankBy.get(r.pubkey);
  console.log(
    `${(score ?? 0).toFixed(6).padStart(10)}  ${String(r.followers).padStart(9)}  ${r.pubkey.slice(0, 16)}  ${nameBy.get(r.pubkey) ?? ""}`,
  );
}
