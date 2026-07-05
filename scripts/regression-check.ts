#!/usr/bin/env bun
/**
 * regression-check.ts — live regression suite for nagg's v2 app-view, run
 * against a real deployment (default: Railway prod). Every route advertised by
 * /nostr/capabilities is exercised with pinned real events + stable-invariant
 * assertions, so a deploy can be checked for contract regressions in one run.
 *
 * Philosophy (see docs/regression-suite.md):
 *   PIN   what can never legitimately change — a pinned event's id / kind /
 *         pubkey / created_at / sha256(content); envelope structure; registry
 *         rule names; DM privacy invariants (no aggregates, no kind-0
 *         hydration); zero-values-omitted.
 *   CLASS what fluctuates but must stay present — "at least one kind-1/1111
 *         reply e-tagging the root", "k7_e exists with actors >= 1 for a
 *         long-referenced event", "entries non-empty for an active viewer".
 *   NEVER assert exact aggregate counts, rankings, reply sets, edge truth
 *         values, or kind-0 contents (replaceable events).
 *
 * All pinned values live in scripts/regression-fixtures.json (each with a
 * `_note` explaining why it is pinned and what may drift). Adding an endpoint
 * = add a fixture entry + one small check function below (and list its route
 * in COVERED so the capabilities coverage gate knows about it).
 *
 * Result classes:
 *   PASS  — all assertions held.
 *   FAIL  — a stable invariant broke (exit code 1).
 *   SKIP  — vertex-credit-dependent route answered with a DVM/vertex 5xx; the
 *           external dependency, not nagg, is unavailable.
 *   XFAIL — failure matching a documented knownIssues signature in the
 *           fixtures (a real, already-reported server bug). Non-gating so the
 *           suite still guards everything else; flips to PASS when fixed, at
 *           which point the runner tells you to delete the entry.
 *
 * The app-view rate-limits 120 req/min: requests are strictly sequential and
 * paced (~600 ms), and a 429 is NEVER recorded as a failure — the runner
 * waits and retries.
 *
 * Usage:
 *   bun scripts/regression-check.ts                    # against prod
 *   bun scripts/regression-check.ts --base https://other-nagg.example
 *
 * Read-only; ~35 sequential requests per run.
 */

import { createHash } from "node:crypto";

const fixtures = await Bun.file(new URL("./regression-fixtures.json", import.meta.url)).json();

const args = process.argv.slice(2);
const flag = (name: string): string | undefined => {
  const i = args.indexOf(`--${name}`);
  return i >= 0 ? args[i + 1] : undefined;
};
const BASE = flag("base") ?? fixtures.base;
const PACE_MS = 600;

// ---- paced HTTP client (429 = wait + retry, never a failure) ----------------

type Resp = { status: number; body: any; text: string };

let lastRequestAt = 0;
async function call(path: string, body?: unknown): Promise<Resp> {
  for (let attempt = 0; attempt < 8; attempt++) {
    const wait = lastRequestAt + PACE_MS - Date.now();
    if (wait > 0) await Bun.sleep(wait);
    lastRequestAt = Date.now();
    let res: Response;
    try {
      res = await fetch(BASE + path, {
        method: body !== undefined ? "POST" : "GET",
        headers: { accept: "application/json", "content-type": "application/json" },
        body: body !== undefined ? JSON.stringify(body) : undefined,
        signal: AbortSignal.timeout(60_000),
      });
    } catch {
      await Bun.sleep(2_000); // transient network error — retry
      continue;
    }
    if (res.status === 429) {
      await Bun.sleep(15_000); // rate limited — wait out the window, retry
      continue;
    }
    const text = await res.text();
    let parsed: any = null;
    try { parsed = JSON.parse(text); } catch { /* non-JSON error body */ }
    return { status: res.status, body: parsed, text };
  }
  return { status: 0, body: null, text: "exhausted retries (429/network)" };
}

// ---- tiny assertion collector ------------------------------------------------

class Tap {
  failures: string[] = [];
  count = 0;
  ok(cond: boolean, msg: string) {
    this.count++;
    if (!cond) this.failures.push(msg);
    return cond;
  }
}

type Status = "PASS" | "FAIL" | "SKIP" | "XFAIL";
type Result = { name: string; status: Status; detail: string };

const sha256 = (s: string) => createHash("sha256").update(s, "utf8").digest("hex");
const HEX64 = /^[0-9a-f]{64}$/;
const now = () => Math.floor(Date.now() / 1000);

// ---- shared envelope invariants ----------------------------------------------

const EVENT_RULES = new Set<string>(fixtures.registry.eventRules);
const PUBKEY_RULES = new Set<string>(fixtures.registry.pubkeyRules);
const NOTIF_KINDS = new Set<number>(fixtures.registry.notificationKinds);

function eventsById(body: any): Map<string, any> {
  return new Map(((body?.events ?? []) as any[]).map((e) => [e?.id, e]));
}

/**
 * The generic envelope contract: order is 64-hex ids, orderBy is a known
 * value, events are raw Nostr shapes deduped by id, aggregates keys are
 * registry rule names with NO zero values (zero = omitted, per contract).
 * `resolveOrder: false` is for routes whose order may anchor ids without a
 * locally-embedded event (documented anchors).
 */
function checkEnvelope(
  t: Tap,
  body: any,
  opts: { resolveOrder?: boolean; pubkeyKeyedAggregates?: boolean } = {},
) {
  if (!t.ok(body !== null && typeof body === "object", "response is not a JSON object")) return;
  t.ok(Array.isArray(body.order), "order is not an array");
  for (const id of body.order ?? []) t.ok(typeof id === "string" && HEX64.test(id), `order entry not 64-hex: ${id}`);
  t.ok(body.orderBy === "created_at" || body.orderBy === "rank", `orderBy unexpected: ${body.orderBy}`);
  t.ok(Array.isArray(body.events), "events is not an array");
  const seen = new Set<string>();
  for (const e of body.events ?? []) {
    t.ok(
      e && HEX64.test(e.id ?? "") && Number.isInteger(e.kind) && HEX64.test(e.pubkey ?? "") &&
        typeof e.content === "string" && Array.isArray(e.tags) && Number.isInteger(e.created_at),
      `event not raw Nostr shape: ${JSON.stringify(e).slice(0, 120)}`,
    );
    t.ok(!seen.has(e?.id), `events not deduplicated by id: ${e?.id}`);
    seen.add(e?.id);
  }
  if (opts.resolveOrder !== false) {
    const byId = eventsById(body);
    for (const id of body.order ?? []) t.ok(byId.has(id), `order id does not resolve against events[]: ${id}`);
  }
  t.ok(typeof (body.aggregates ?? undefined) === "object" && body.aggregates !== null, "aggregates missing or not an object");
  const allowed = opts.pubkeyKeyedAggregates ? PUBKEY_RULES : EVENT_RULES;
  for (const [target, rules] of Object.entries(body.aggregates ?? {})) {
    t.ok(HEX64.test(target), `aggregates target not 64-hex: ${target}`);
    for (const [rule, metrics] of Object.entries(rules as any)) {
      t.ok(allowed.has(rule), `aggregate rule not in registry vocabulary: ${rule}`);
      for (const [metric, value] of Object.entries(metrics as any)) {
        t.ok(typeof value === "number" && value !== 0, `zero/non-numeric aggregate not omitted: ${target}.${rule}.${metric}=${value}`);
      }
    }
  }
  if (body.cursor !== undefined) t.ok(typeof body.cursor === "string" && body.cursor.length > 0, "cursor present but not a non-empty string");
}

/**
 * Every kind-1/1111 event's author must have a kind-0 embedded. STRICT — only
 * valid where the local index guarantees coverage (the pinned thread, whose
 * participants are indexed). Feed routes hydrate best-effort under
 * non-blocking relay backfill, so there use checkPinnedK0 instead.
 */
function checkK0Hydration(t: Tap, body: any, label: string) {
  const k0Authors = new Set((body?.events ?? []).filter((e: any) => e?.kind === 0).map((e: any) => e.pubkey));
  const authors = new Set<string>(
    (body?.events ?? []).filter((e: any) => e?.kind === 1 || e?.kind === 1111).map((e: any) => e.pubkey as string),
  );
  for (const a of authors) t.ok(k0Authors.has(a), `${label}: kind-1/1111 author ${a.slice(0, 8)}… has no kind-0 in events[]`);
}

/** Hydration works at all: a kind-0 for each PINNED (known-indexed) author. */
function checkPinnedK0(t: Tap, body: any, pubkeys: string[], label: string) {
  const k0Authors = new Set((body?.events ?? []).filter((e: any) => e?.kind === 0).map((e: any) => e.pubkey));
  for (const pk of pubkeys) t.ok(k0Authors.has(pk), `${label}: no kind-0 for pinned author ${pk.slice(0, 8)}…`);
}

/** DM privacy: bare envelope — no aggregates, zero kind-0 hydration. */
function checkBareEnvelope(t: Tap, body: any, label: string) {
  t.ok(Object.keys(body?.aggregates ?? {}).length === 0, `${label}: DM/bare route carries aggregates (privacy invariant)`);
  t.ok(!(body?.events ?? []).some((e: any) => e?.kind === 0), `${label}: DM/bare route hydrates kind-0 profiles (privacy invariant)`);
}

/** Deep-walk: no `reason` key anywhere (kinds carry the semantics, v2 contract). */
function checkNoReasonStrings(t: Tap, node: any, path = "$") {
  if (Array.isArray(node)) node.forEach((v, i) => checkNoReasonStrings(t, v, `${path}[${i}]`));
  else if (node && typeof node === "object") {
    for (const [k, v] of Object.entries(node)) {
      t.ok(k !== "reason" && k !== "reasons", `reason key found at ${path}.${k} — v2 is kind-vocabulary only`);
      checkNoReasonStrings(t, v, `${path}.${k}`);
    }
  }
}

function checkPinnedEvent(t: Tap, body: any, pin: { id: string; kind: number; pubkey: string; createdAt: number; contentSha256: string }, label: string) {
  const e = eventsById(body).get(pin.id);
  if (!t.ok(!!e, `${label}: pinned event ${pin.id.slice(0, 8)}… missing from events[]`)) return;
  t.ok(e.kind === pin.kind, `${label}: pinned kind drifted ${e.kind} != ${pin.kind}`);
  t.ok(e.pubkey === pin.pubkey, `${label}: pinned author drifted`);
  t.ok(e.created_at === pin.createdAt, `${label}: pinned created_at drifted ${e.created_at} != ${pin.createdAt}`);
  t.ok(sha256(e.content) === pin.contentSha256, `${label}: pinned content hash drifted`);
}

// ---- SKIP / XFAIL classification ----------------------------------------------

/** Vertex/DVM-dependent routes: an upstream-credit 5xx is a SKIP, not a FAIL. */
function vertexSkip(r: Resp): string | null {
  if (r.status >= 500 && /vertex|dvm|credit/i.test(r.text)) return `vertex/DVM unavailable (HTTP ${r.status})`;
  if (r.status === 502 || r.status === 503) return `upstream DVM unavailable (HTTP ${r.status})`;
  return null;
}

/** Documented known server bugs (fixtures.knownIssues) → XFAIL, not FAIL. */
function knownIssue(r: Resp): string | null {
  for (const issue of fixtures.knownIssues ?? []) {
    if (r.status === issue.httpStatus && r.text.includes(issue.bodyIncludes)) return issue.id;
  }
  return null;
}

// ---- checks -------------------------------------------------------------------
// Each check covers one or more advertised routes (COVERED drives the
// capabilities coverage gate). Adding an endpoint = fixture entry + one entry here.

type Check = { name: string; covers: string[]; run: () => Promise<Result> };

function result(name: string, t: Tap, extra = ""): Result {
  if (t.failures.length === 0) return { name, status: "PASS", detail: `${t.count} assertions${extra ? ` — ${extra}` : ""}` };
  return { name, status: "FAIL", detail: t.failures.join("; ") };
}

/** Wrap a check body with HTTP + SKIP/XFAIL classification. */
function httpCheck(
  name: string,
  covers: string[],
  request: () => Promise<Resp>,
  assertions: (t: Tap, r: Resp) => void,
  opts: { vertexDependent?: boolean } = {},
): Check {
  return {
    name,
    covers,
    run: async () => {
      const r = await request();
      if (opts.vertexDependent) {
        const skip = vertexSkip(r);
        if (skip) return { name, status: "SKIP", detail: skip };
      }
      const issue = knownIssue(r);
      if (issue) return { name, status: "XFAIL", detail: `known issue "${issue}" (HTTP ${r.status}): ${r.text.slice(0, 100).trim()}` };
      const t = new Tap();
      if (!t.ok(r.status === 200, `HTTP ${r.status}: ${r.text.slice(0, 160).trim()}`)) return result(name, t);
      assertions(t, r);
      return result(name, t);
    },
  };
}

const P = fixtures.pinned;
const V = fixtures.viewers;

const checks: Check[] = [
  // -- capabilities: version pin + the forget-an-endpoint gate (evaluated below,
  //    after all checks are declared, via COVERED).
  {
    name: "capabilities",
    covers: ["/nostr/capabilities"],
    run: async () => {
      const r = await call("/nostr/capabilities");
      const t = new Tap();
      if (!t.ok(r.status === 200, `HTTP ${r.status}`)) return result("capabilities", t);
      t.ok(r.body?.appViewVersion === fixtures.capabilities.appViewVersion, `appViewVersion ${r.body?.appViewVersion} != ${fixtures.capabilities.appViewVersion}`);
      t.ok((r.body?.capabilities ?? []).includes("appview.v2"), "capability token appview.v2 missing");
      const advertised: string[] = r.body?.appViews?.[0]?.routes ?? [];
      t.ok(advertised.length > 0, "no advertised app-view routes");
      const covered = new Set(checks.flatMap((c) => c.covers));
      for (const route of advertised) {
        t.ok(covered.has(route), `advertised route has NO regression coverage (add a fixture + check): ${route}`);
      }
      return result("capabilities", t, `${advertised.length} advertised routes all covered`);
    },
  },

  // -- feed: pinned author events must appear when `until` windows over them.
  //    Hydration (repost originals, kind-0s) is BEST-EFFORT under non-blocking
  //    relay backfill, so order-resolution and kind-0 coverage are asserted for
  //    the pinned (known-indexed) event/author only — never for every anchor.
  ...P.feedAuthors.map((a: any) =>
    httpCheck(
      `feed (${a.label})`,
      ["/nostr/feed"],
      () => call(`/nostr/feed?pubkeys=${a.pubkey}&until=${a.createdAt + 1}&limit=30`),
      (t, r) => {
        checkEnvelope(t, r.body, { resolveOrder: false });
        t.ok((r.body.order ?? []).includes(a.eventId), `pinned event ${a.eventId.slice(0, 8)}… not in order for until=${a.createdAt + 1}`);
        checkPinnedEvent(t, r.body, { id: a.eventId, kind: a.kind, pubkey: a.pubkey, createdAt: a.createdAt, contentSha256: a.contentSha256 }, "feed");
        checkPinnedK0(t, r.body, [a.pubkey], "feed");
      },
    ),
  ),

  // -- feed/user: single-author feed. Every RESOLVED order id must be the
  //    author's own event or a repost anchor (original id e-tagged by the
  //    author's kind-6/16); unresolved anchors are the documented best-effort
  //    hydration gap, not a regression.
  httpCheck(
    "feed/user",
    ["/nostr/feed/user"],
    () => call(`/nostr/feed/user?pubkey=${P.feedAuthors[0].pubkey}&limit=30`),
    (t, r) => {
      const a = P.feedAuthors[0];
      checkEnvelope(t, r.body, { resolveOrder: false });
      t.ok((r.body.order ?? []).length > 0, "user feed empty for a pinned active author");
      const byId = eventsById(r.body);
      const reposted = new Set<string>();
      for (const e of r.body.events ?? []) {
        if ((e.kind === 6 || e.kind === 16) && e.pubkey === a.pubkey) {
          for (const tag of e.tags) if (tag[0] === "e" && HEX64.test(tag[1] ?? "")) reposted.add(tag[1]);
        }
      }
      let ownResolved = 0;
      for (const id of r.body.order ?? []) {
        const e = byId.get(id);
        if (!e) continue; // unresolved anchor — best-effort hydration
        t.ok(e.pubkey === a.pubkey || reposted.has(id), `resolved order id ${id.slice(0, 8)}… is neither authored by ${a.label} nor a repost anchor`);
        if (e.pubkey === a.pubkey) ownResolved++;
      }
      t.ok(ownResolved > 0, "no order id resolved to an event authored by the pinned author");
      checkPinnedK0(t, r.body, [a.pubkey], "feed/user");
    },
  ),

  // -- feed/ranked: ranking is fully volatile — structure only.
  httpCheck(
    "feed/ranked",
    ["/nostr/feed/ranked"],
    () => {
      const input = structuredClone(fixtures.rankedFeedInput.input);
      input.references.since = now() - fixtures.rankedFeedInput.sinceHoursAgo * 3600;
      return call("/nostr/feed/ranked", input);
    },
    (t, r) => {
      checkEnvelope(t, r.body);
      t.ok(r.body.orderBy === "rank", `orderBy ${r.body.orderBy} != rank`);
      t.ok((r.body.order ?? []).length > 0, "ranked feed empty over a 48h window");
      // Hydration wired at all (authors are volatile, so presence class only).
      t.ok((r.body.events ?? []).some((e: any) => e.kind === 0), "ranked feed carries no kind-0 hydration");
    },
  ),

  // -- feed/ranked (gated): the app's For-You shape — vertex pubkeyScore gate
  // + weighted rule-name terms — which routes through the DB-first
  // rank_features scan. Structure-only, but 200 is the assertion that the
  // rank_features column vocabulary matches the read (broke 2026-07-05).
  httpCheck(
    "feed/ranked (gated)",
    [],
    () => {
      const input = structuredClone(fixtures.rankedFeedInput.gatedInput);
      input.references.since = now() - fixtures.rankedFeedInput.sinceHoursAgo * 3600;
      return call("/nostr/feed/ranked", input);
    },
    (t, r) => {
      checkEnvelope(t, r.body);
      t.ok(r.body.orderBy === "rank", `orderBy = ${r.body.orderBy}, want rank`);
    },
  ),

  // -- events: exact content equality for three pinned immutable events
  //    (kind-1, kind-7, kind-9735 — generic kinds, not "post/like/zap").
  httpCheck(
    "events",
    ["/nostr/events"],
    () => call(`/nostr/events?ids=${P.events.map((e: any) => e.id).join(",")}`),
    (t, r) => {
      checkEnvelope(t, r.body);
      t.ok(
        JSON.stringify(r.body.order) === JSON.stringify(P.events.map((e: any) => e.id)),
        `order != requested resolved ids: ${JSON.stringify(r.body.order)}`,
      );
      for (const pin of P.events) checkPinnedEvent(t, r.body, pin, `events kind-${pin.kind}`);
    },
  ),

  // -- events/aggregates: presence class — rules must EXIST with >= 1 for a
  //    long-referenced event (the aggregate ledger only accumulates); exact
  //    counts never asserted.
  httpCheck(
    "events/aggregates",
    ["/nostr/events/aggregates"],
    () => call("/nostr/events/aggregates", { ids: [P.aggregatesTarget.id] }),
    (t, r) => {
      checkEnvelope(t, r.body, { resolveOrder: false });
      t.ok((r.body.order ?? []).length === 0 && (r.body.events ?? []).length === 0, "aggregates-only route returned order/events");
      const agg = r.body.aggregates?.[P.aggregatesTarget.id];
      if (t.ok(!!agg, "no aggregates for a known-referenced event")) {
        for (const rule of P.aggregatesTarget.mustHaveRules) {
          const metrics = agg[rule];
          t.ok(!!metrics, `rule ${rule} missing for a long-referenced event`);
          if (metrics) for (const v of Object.values(metrics)) t.ok((v as number) >= 1, `${rule} metric < 1`);
        }
      }
    },
  ),

  // -- events/query: constrained filter — every ORDERED event matches it.
  //    (events[] may additionally carry kind-0 hydration for non-1059 queries;
  //    the filter applies to the ordered items, not the hydration.)
  httpCheck(
    "events/query (kind-7 refs)",
    ["/nostr/events/query"],
    () => call("/nostr/events/query", { kinds: [7], tags: [{ key: "e", values: [P.aggregatesTarget.id] }], limit: 5 }),
    (t, r) => {
      checkEnvelope(t, r.body);
      t.ok((r.body.order ?? []).length > 0, "no kind-7 events for a long-referenced target");
      const byId = eventsById(r.body);
      for (const id of r.body.order ?? []) {
        const e = byId.get(id);
        if (!t.ok(!!e, `ordered id ${id.slice(0, 8)}… missing from events[]`)) continue;
        t.ok(e.kind === 7, `filter leak: ordered kind ${e.kind}`);
        t.ok(e.tags.some((tag: string[]) => tag[0] === "e" && tag[1] === P.aggregatesTarget.id), `filter leak: ordered event ${id.slice(0, 8)}… lacks the e-tag`);
      }
    },
  ),
  httpCheck(
    "events/query (kind-1059 bare)",
    ["/nostr/events/query"],
    () => call("/nostr/events/query", { kinds: [1059], tags: [{ key: "p", values: [V.dmViewer] }], limit: 5 }),
    (t, r) => {
      checkEnvelope(t, r.body);
      checkBareEnvelope(t, r.body, "events/query kinds=[1059]");
      for (const e of r.body.events ?? []) t.ok(e.kind === 1059, `filter leak: kind ${e.kind}`);
    },
  ),

  // -- thread: the suite's anchor fixture. Root pinned exactly; replies as a
  //    presence class (events are never deleted here, so at least one
  //    kind-1/1111 e-tagging the root must persist); full kind-0 hydration.
  httpCheck(
    "thread",
    ["/nostr/thread"],
    () => call(`/nostr/thread?id=${P.thread.rootId}&limit=100`),
    (t, r) => {
      checkEnvelope(t, r.body);
      t.ok(r.body.order?.[0] === P.thread.rootId, `order[0] ${String(r.body.order?.[0]).slice(0, 8)}… != root`);
      checkPinnedEvent(t, r.body, { id: P.thread.rootId, kind: P.thread.rootKind, pubkey: P.thread.rootPubkey, createdAt: P.thread.rootCreatedAt, contentSha256: P.thread.rootContentSha256 }, "thread root");
      const replies = (r.body.events ?? []).filter(
        (e: any) => (e.kind === 1 || e.kind === 1111) && e.id !== P.thread.rootId && e.tags.some((tag: string[]) => tag[0] === "e" && tag[1] === P.thread.rootId),
      );
      t.ok(replies.length >= 1, "no kind-1/1111 event referencing the root — replies lost");
      checkK0Hydration(t, r.body, "thread");
      t.ok(Object.keys(r.body.aggregates ?? {}).length >= 1, "thread with known references carries no aggregates");
    },
  ),

  // -- notifications: presence + vocabulary. Entry kinds come from the closed
  //    set; NO reason strings anywhere; entries resolve against events[].
  httpCheck(
    "notifications",
    ["/nostr/notifications"],
    () => call(`/nostr/notifications?pubkey=${V.notificationsViewer}&limit=20&policy=${V.notificationsPolicy}`),
    (t, r) => {
      checkEnvelope(t, r.body);
      t.ok(Array.isArray(r.body.entries), "entries missing");
      t.ok(typeof r.body.hasNext === "boolean", "hasNext missing");
      t.ok((r.body.entries ?? []).length > 0, "no entries for the pinned ACTIVE viewer (14d read model — re-pin viewer if dormant)");
      // Follow-only output is the read-model's signature failure (kind-3 keeps
      // flowing because contact lists republish with fresh timestamps while
      // late-ingested engagement history gets skipped) — an active viewer must
      // show at least one non-follow entry, or the pipeline upstream of the
      // page read is broken even though entries[] is non-empty.
      t.ok(
        (r.body.entries ?? []).some((e: any) => e.kind !== 3),
        "entries are follow-only — engagement rows lost upstream (viewer_refs → viewer_feed rollup)",
      );
      const byId = eventsById(r.body);
      for (const e of r.body.entries ?? []) {
        t.ok(NOTIF_KINDS.has(e.kind), `entry kind outside closed set: ${e.kind}`);
        t.ok(byId.has(e.id), `entry id ${String(e.id).slice(0, 8)}… not embedded in events[]`);
        if (e.target) t.ok(byId.has(e.target), `entry target ${String(e.target).slice(0, 8)}… not embedded in events[]`);
        for (const actor of e.actors ?? []) t.ok(HEX64.test(actor.pubkey ?? ""), "actor without pubkey");
      }
      checkNoReasonStrings(t, r.body);
    },
  ),

  // -- notifications/seen: the viewer's kind-30078 marker, or an empty envelope.
  httpCheck(
    "notifications/seen",
    ["/nostr/notifications/seen"],
    () => call(`/nostr/notifications/seen?pubkey=${V.notificationsViewer}`),
    (t, r) => {
      checkEnvelope(t, r.body);
      for (const e of r.body.events ?? []) t.ok(e.kind === 30078, `seen marker kind ${e.kind} != 30078`);
    },
  ),

  // -- follows: pubkey-keyed aggregates from the registry vocabulary.
  //    (XFAIL while the pubkey_stats column-rename bug is live.)
  httpCheck(
    "follows",
    ["/nostr/follows"],
    () => call(`/nostr/follows?pubkey=${V.profilePubkey}`),
    (t, r) => {
      checkEnvelope(t, r.body, { pubkeyKeyedAggregates: true });
      t.ok((r.body.events ?? []).some((e: any) => e.kind === 0 && e.pubkey === V.profilePubkey), "no kind-0 for the requested pubkey");
    },
  ),

  // -- profiles (plural): kind-0s for the requested pubkeys, content is JSON.
  httpCheck(
    "profiles",
    ["/nostr/profiles"],
    () => call(`/nostr/profiles?pubkeys=${V.profilesPubkeys.join(",")}`),
    (t, r) => {
      checkEnvelope(t, r.body, { pubkeyKeyedAggregates: true });
      for (const pk of V.profilesPubkeys) {
        const k0 = (r.body.events ?? []).find((e: any) => e.kind === 0 && e.pubkey === pk);
        if (t.ok(!!k0, `no kind-0 for ${pk.slice(0, 8)}…`)) {
          t.ok((() => { try { JSON.parse(k0.content); return true; } catch { return false; } })(), `kind-0 content not JSON for ${pk.slice(0, 8)}…`);
        }
      }
    },
  ),

  // -- profile (singular): kind-0 + pubkeys list + provider namespaces from the
  //    plugin registry. (XFAIL while the pubkey_stats bug is live.)
  httpCheck(
    "profile",
    ["/nostr/profile"],
    () => call(`/nostr/profile?pubkey=${V.profilePubkey}`),
    (t, r) => {
      checkEnvelope(t, r.body, { resolveOrder: false, pubkeyKeyedAggregates: true });
      t.ok((r.body.pubkeys ?? []).includes(V.profilePubkey), "pubkeys[] missing the requested pubkey");
      t.ok((r.body.events ?? []).some((e: any) => e.kind === 0 && e.pubkey === V.profilePubkey), "no kind-0 for the requested pubkey");
      const allowedNs = new Set(fixtures.registry.providerNamespaces);
      for (const provs of Object.values(r.body.providers ?? {})) {
        for (const ns of Object.keys(provs as any)) t.ok(allowedNs.has(ns), `provider namespace not in plugin registry: ${ns}`);
      }
    },
  ),

  // -- search: structure only; membership/ranking volatile; DVM 5xx = SKIP.
  httpCheck(
    "search",
    ["/nostr/search"],
    () => call(`/nostr/search?query=${fixtures.search.query}&limit=${fixtures.search.limit}`),
    (t, r) => {
      // Search is a pubkey-centric route: its aggregates are keyed by pubkey
      // (k3_p_latest / k3_author_latest / k1_1111_author), not by event id.
      checkEnvelope(t, r.body, { pubkeyKeyedAggregates: true });
      t.ok(Array.isArray(r.body.pubkeys), "pubkeys missing");
      t.ok((r.body.pubkeys ?? []).length > 0, "no results for a well-indexed query");
      for (const pk of r.body.pubkeys ?? []) t.ok(HEX64.test(pk), `pubkeys entry not 64-hex: ${pk}`);
      const allowedNs = new Set(fixtures.registry.providerNamespaces);
      for (const provs of Object.values(r.body.providers ?? {})) {
        for (const ns of Object.keys(provs as any)) t.ok(allowedNs.has(ns), `provider namespace not in plugin registry: ${ns}`);
      }
    },
    { vertexDependent: true },
  ),

  // -- recommended: vertex-credit dependent, no local fallback — SKIP on 5xx.
  httpCheck(
    "recommended",
    ["/nostr/recommended"],
    () => call(`/nostr/recommended?source=${fixtures.recommended.source}&limit=${fixtures.recommended.limit}`),
    (t, r) => {
      // Pubkey-centric route: aggregates are keyed by pubkey rules.
      checkEnvelope(t, r.body, { resolveOrder: false, pubkeyKeyedAggregates: true });
      t.ok(Array.isArray(r.body.pubkeys), "pubkeys missing");
    },
    { vertexDependent: true },
  ),

  // -- follow-status: edges structure only — {out,in} booleans per candidate;
  //    the truth values are volatile (people unfollow).
  httpCheck(
    "follow-status",
    ["/nostr/follow-status"],
    () => call(`/nostr/follow-status?viewer=${V.followStatusViewer}&candidates=${V.followStatusCandidates.join(",")}`),
    (t, r) => {
      checkEnvelope(t, r.body, { pubkeyKeyedAggregates: true });
      const edges = r.body.edges ?? {};
      t.ok(Object.keys(edges).length === V.followStatusCandidates.length, `edges has ${Object.keys(edges).length} keys, want ${V.followStatusCandidates.length}`);
      for (const pk of V.followStatusCandidates) {
        const edge = edges[pk];
        t.ok(!!edge && typeof edge.out === "boolean" && typeof edge.in === "boolean", `edge for ${pk.slice(0, 8)}… missing or not {out,in} booleans`);
      }
    },
  ),

  // -- social-graph: the viewer's latest kind-3 must be present (an active
  //    pubkey always has one; kind-3 is replaceable-latest, never pruned to zero).
  httpCheck(
    "social-graph",
    ["/nostr/social-graph"],
    () => call(`/nostr/social-graph?pubkey=${V.socialGraphPubkey}`),
    (t, r) => {
      checkEnvelope(t, r.body, { resolveOrder: false });
      const k3 = (r.body.events ?? []).find((e: any) => e.kind === 3 && e.pubkey === V.socialGraphPubkey);
      t.ok(!!k3, "no kind-3 for an active pubkey");
      if (k3) t.ok(k3.tags.some((tag: string[]) => tag[0] === "p"), "kind-3 carries no p tags");
    },
  ),

  // -- own/profiles: kind-0 + pubkey-keyed aggregates.
  //    (XFAIL while the pubkey_stats bug is live.)
  httpCheck(
    "own/profiles",
    ["/nostr/own/profiles"],
    () => call(`/nostr/own/profiles?pubkeys=${V.ownHistoryPubkey}`),
    (t, r) => {
      checkEnvelope(t, r.body, { pubkeyKeyedAggregates: true });
      t.ok((r.body.events ?? []).some((e: any) => e.kind === 0 && e.pubkey === V.ownHistoryPubkey), "no kind-0 for the requested pubkey");
    },
  ),

  // -- own/{type}: every ordered event is the viewer's own, of the type's kinds.
  httpCheck(
    "own/authored",
    ["/nostr/own/"],
    () => call(`/nostr/own/authored?pubkey=${V.ownHistoryPubkey}&limit=10`),
    (t, r) => {
      checkEnvelope(t, r.body);
      t.ok((r.body.order ?? []).length > 0, "no authored history for an active pubkey");
      const byId = eventsById(r.body);
      for (const id of r.body.order ?? []) {
        const e = byId.get(id);
        t.ok(!!e && e.pubkey === V.ownHistoryPubkey, `order id ${id.slice(0, 8)}… not authored by the viewer`);
        t.ok(!!e && (e.kind === 1 || e.kind === 1111), `authored entry kind ${e?.kind} outside {1,1111}`);
      }
      if (r.body.cursor) t.ok(/^\d{4}-\d{2}-\d{2}T.+\|[0-9a-f]{64}$/.test(r.body.cursor), `list cursor not "<RFC3339Nano>|<id>": ${r.body.cursor}`);
    },
  ),

  // -- DM routes: STRUCTURE + the privacy invariants (bare envelopes §5).
  httpCheck(
    "dm/envelopes",
    ["/nostr/dm/envelopes"],
    () => call(`/nostr/dm/envelopes?viewer=${V.dmViewer}&limit=20`),
    (t, r) => {
      checkEnvelope(t, r.body);
      checkBareEnvelope(t, r.body, "dm/envelopes");
      for (const e of r.body.events ?? []) t.ok(e.kind === 4 || e.kind === 1059, `DM envelope kind outside {4,1059}: ${e.kind}`);
    },
  ),
  httpCheck(
    "dm/conversation",
    ["/nostr/dm/conversation"],
    () => call(`/nostr/dm/conversation?viewer=${V.dmViewer}&limit=20`),
    (t, r) => {
      checkEnvelope(t, r.body);
      checkBareEnvelope(t, r.body, "dm/conversation");
      for (const e of r.body.events ?? []) t.ok(e.kind === 4 || e.kind === 1059, `DM conversation kind outside {4,1059}: ${e.kind}`);
    },
  ),

  // -- mint routes: NOT envelopes — 200 + parseable + top-level shape only.
  httpCheck(
    "mint/reviews",
    ["/nostr/mint/reviews"],
    () => call(`/nostr/mint/reviews?u=${encodeURIComponent(fixtures.mint.reviewsUrl)}`),
    (t, r) => {
      t.ok(r.body && typeof r.body === "object", "not a JSON object");
      t.ok(typeof r.body?.summary === "object" && r.body.summary !== null, "summary missing");
      t.ok(Array.isArray(r.body?.reviews), "reviews missing");
      t.ok(typeof r.body?.profiles === "object" && r.body.profiles !== null, "profiles missing");
      t.ok(typeof r.body?.summary?.mintUrl === "string", "summary.mintUrl missing");
    },
  ),
  httpCheck(
    "mint/discover",
    ["/nostr/mint/discover"],
    () => call(`/nostr/mint/discover?limit=${fixtures.mint.discoverLimit}`),
    (t, r) => {
      t.ok(Array.isArray(r.body?.mints), "mints missing");
      t.ok((r.body?.mints ?? []).length > 0, "no mints discovered");
      for (const m of r.body?.mints ?? []) t.ok(typeof m.mintUrl === "string" && m.mintUrl.length > 0, "mint without mintUrl");
    },
  ),

  // -- app/latest-version: static payload; POST-only by contract.
  httpCheck(
    "app/latest-version",
    ["/app/latest-version"],
    () => call("/app/latest-version", { storage: { version: "0.0.0" } }),
    (t, r) => {
      t.ok(typeof r.body?.version === "string", "version missing");
    },
  ),
];

// ---- main ----------------------------------------------------------------------

console.log(`nagg v2 app-view regression suite`);
console.log(`base = ${BASE}`);
console.log(`pace = ${PACE_MS}ms (rate limit 120/min; 429 → wait + retry, never a failure)\n`);

const results: Result[] = [];
for (const check of checks) {
  const r = await check.run();
  results.push(r);
  const pad = r.name.padEnd(28);
  console.log(`${r.status.padEnd(5)} ${pad} ${r.detail}`);
}

const counts = { PASS: 0, FAIL: 0, SKIP: 0, XFAIL: 0 } as Record<Status, number>;
for (const r of results) counts[r.status]++;

console.log(`\n━━ summary ━━`);
console.log(`PASS ${counts.PASS}  FAIL ${counts.FAIL}  SKIP ${counts.SKIP}  XFAIL ${counts.XFAIL}`);
if (counts.XFAIL > 0) {
  console.log(`\n⚠️  XFAIL = documented known server bugs (fixtures.knownIssues) — tracked, non-gating:`);
  for (const r of results.filter((x) => x.status === "XFAIL")) console.log(`   ${r.name}: ${r.detail}`);
  console.log(`   When one flips to PASS, DELETE its knownIssues entry so it gates again.`);
}
if (counts.SKIP > 0) {
  console.log(`\nSKIP = vertex-credit-dependent routes with the DVM unavailable:`);
  for (const r of results.filter((x) => x.status === "SKIP")) console.log(`   ${r.name}: ${r.detail}`);
}
if (counts.FAIL > 0) {
  console.log(`\n❌ regressions detected:`);
  for (const r of results.filter((x) => x.status === "FAIL")) console.log(`   ${r.name}: ${r.detail}`);
}
process.exit(counts.FAIL > 0 ? 1 : 0);
