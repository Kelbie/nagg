#!/usr/bin/env bun
/**
 * parity-check.ts — compare the prod nagg app-view against the devnagg staging
 * service for (a) response-SHAPE parity (so query rewrites can't break clients)
 * and (b) server compute time.
 *
 * Metric notes:
 *  - devnagg runs the staging branch (Server-Timing instrumented, Redis OFF), so
 *    its `Server-Timing: app/db/hydrate` is clean per-request compute — this is
 *    the A/B metric for optimizations.
 *  - prod runs main (no Server-Timing) with Redis ON, so we only read its JSON
 *    body (for shape parity) and wall-clock time (cache-busted by rotating
 *    pubkeys) as a sanity check.
 *
 * Shape parity: we validate each response against the nagg-ts Zod schema that
 * the app actually parses with. This is the real "won't break clients" contract
 * and is immune to data volatility (prod's live notification set differs from
 * devnagg's run-to-run), unlike a strict prod-vs-dev shape diff.
 *
 * Usage:
 *   bun scripts/parity-check.ts                 # parity + bench, default pubkeys
 *   bun scripts/parity-check.ts --bench-only    # only devnagg Server-Timing
 *   bun scripts/parity-check.ts --pubkeys a,b   # custom hex pubkeys
 */

import {
  NaggFeedPageSchema,
  NaggNotificationsPageSchema,
  NaggDmEnvelopesDataSchema,
} from "../../nagg-ts/src/schemas";
import type { ZodTypeAny } from "../../nagg-ts/node_modules/zod";

const PROD = process.env.PROD_BASE ?? "https://nagg.up.railway.app";
const DEV = process.env.DEV_BASE ?? "https://devnagg-production.up.railway.app";

// Primary verification pubkey (npub1ceel7z6...) + well-known high-activity
// accounts to defeat prod's Redis cache and exercise realistic data volumes.
const DEFAULT_PUBKEYS: Record<string, string> = {
  verify: "c673ff0b5f228feb0abb1001882178d4c588bc4e50f857173544b5543b454f81",
  jack: "82341f882b6eabcd2ba7f1ef90aad961cf074af15b9ef44a09f9d2a8fbfbe6a2",
  fiatjaf: "3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d",
  gigi: "6e468422dfb74a5738702a8823b9b28168abab8655faacb6853cd0ee15deee93",
  jb55: "32e1827635450ebb3c5a7d12c1f8e7b2b514439ac10a67eef3d9fd9c5c68e245",
  odell: "04c915daefee38317fa734444acee390a8269fe5810b2241e5e6dd343dfbecc9",
};

type EndpointSpec = { name: string; path: (pk: string) => string; schema: ZodTypeAny };
const ENDPOINTS: EndpointSpec[] = [
  { name: "feed", path: (pk) => `/nostr/feed?pubkeys=${pk}&limit=30`, schema: NaggFeedPageSchema },
  { name: "feed/user", path: (pk) => `/nostr/feed/user?pubkey=${pk}&limit=30`, schema: NaggFeedPageSchema },
  { name: "notifications/all", path: (pk) => `/nostr/notifications?pubkey=${pk}&limit=50`, schema: NaggNotificationsPageSchema },
  { name: "notifications/mentions", path: (pk) => `/nostr/notifications?pubkey=${pk}&limit=50&tab=MENTIONS`, schema: NaggNotificationsPageSchema },
  { name: "dm/envelopes", path: (pk) => `/nostr/dm/envelopes?viewer=${pk}&limit=50`, schema: NaggDmEnvelopesDataSchema },
];

const args = process.argv.slice(2);
const benchOnly = args.includes("--bench-only");
const pkArg = args.find((a) => a.startsWith("--pubkeys"));
const pubkeys: Record<string, string> = pkArg
  ? Object.fromEntries(
      (pkArg.split("=")[1] ?? "").split(",").map((p, i) => [`pk${i}`, p.trim()]),
    )
  : DEFAULT_PUBKEYS;

async function fetchJSON(
  base: string,
  path: string,
): Promise<{ ms: number; status: number; timing: string | null; body: any }> {
  const t0 = performance.now();
  const res = await fetch(base + path, { headers: { accept: "application/json" } });
  const body = await res.json().catch(() => null);
  return {
    ms: performance.now() - t0,
    status: res.status,
    timing: res.headers.get("server-timing"),
    body,
  };
}

function parseTiming(h: string | null): Record<string, number> {
  const out: Record<string, number> = {};
  if (!h) return out;
  for (const seg of h.split(",")) {
    const m = seg.trim().match(/^([a-z]+);dur=([0-9.]+)/i);
    if (m) out[m[1]] = parseFloat(m[2]);
  }
  return out;
}

function fmt(n: number | undefined): string {
  return n === undefined ? "  -  " : `${n.toFixed(0)}ms`.padStart(7);
}

// ---- main ------------------------------------------------------------------

let parityFailures = 0;

function validate(schema: ZodTypeAny, body: unknown): string {
  const r = schema.safeParse(body);
  if (r.success) return "ok";
  const first = r.error.issues[0];
  return `${first.path.join(".") || "<root>"}: ${first.message}`;
}

for (const ep of ENDPOINTS) {
  console.log(`\n━━ ${ep.name} ━━`);
  console.log(
    `${"pubkey".padEnd(10)} ${"dev app".padStart(7)} ${"dev db".padStart(7)} ${"dev hyd".padStart(7)} ${"prod wall".padStart(9)}  contract (dev / prod)`,
  );
  for (const [label, pk] of Object.entries(pubkeys)) {
    const path = ep.path(pk);
    const dev = await fetchJSON(DEV, path);
    const devT = parseTiming(dev.timing);
    let contractCol = "(bench-only)";
    let prodWall = "   -   ";
    if (!benchOnly) {
      const prod = await fetchJSON(PROD, path);
      prodWall = `${prod.ms.toFixed(0)}ms`.padStart(9);
      const devValid = validate(ep.schema, dev.body);
      const prodValid = validate(ep.schema, prod.body);
      if (devValid === "ok" && prodValid === "ok") {
        contractCol = "✓ both valid";
      } else {
        // Only a devnagg failure breaks clients (that's the code under test). A
        // prod-only failure means the schema lags reality — surfaced, not fatal.
        if (devValid !== "ok") parityFailures++;
        contractCol = `dev:${devValid === "ok" ? "✓" : "✗ " + devValid} prod:${prodValid === "ok" ? "✓" : "✗ " + prodValid}`;
      }
    }
    console.log(
      `${label.padEnd(10)} ${fmt(devT.app)} ${fmt(devT.db)} ${fmt(devT.hydrate)} ${prodWall}  ${contractCol}`,
    );
  }
}

if (!benchOnly) {
  console.log(
    parityFailures === 0
      ? "\n✅ contract: every devnagg response validates against the nagg-ts client schema"
      : `\n❌ contract: ${parityFailures} devnagg response(s) FAILED the client schema — would break clients`,
  );
  process.exit(parityFailures === 0 ? 0 : 1);
}
