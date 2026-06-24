# ADR 0001 — REST `/nostr/search` shares the cache-backed `SearchProvider` with GraphQL

- Status: accepted
- Date: 2026-06-24

## Context

`GET /nostr/search` (REST app-view) and the GraphQL `profileSearch` resolver are
served by the same nagg process but historically diverged in result quality. With
the live Vertex pagerank DVM out of credits, GraphQL kept returning good results
while REST returned junk (exact/prefix local text matches, `calle@cashu.me` buried
behind random accounts).

Root cause: the two transports used different Vertex seams.

- GraphQL routed through `vertex.SearchProvider` — a cache-backed wrapper that
  serves previously-computed pagerank results from ClickHouse (`CachedVertexSearch`,
  `fromCache:true`), refreshes the live DVM asynchronously, and swallows DVM errors.
- REST called the raw `*vertex.Client` directly (`h.vertex.Search`, no cache read),
  so any DVM failure dropped it straight to the local text index.

PR #28 had already given REST a *local* fallback (no more 502 on DVM failure), but
not the *cached-Vertex* path, so the divergence persisted whenever the live DVM was
unavailable. Git history shows REST-search-as-local-only was an incomplete port (the
REST endpoint predated the GraphQL resolver), not a deliberate decision.

## Decision

1. **One shared `SearchProvider`.** `cmd/api` constructs a single
   `vertex.NewSearchProvider(...)` and injects it into both the GraphQL schema
   (`graphqlapi.WithProfileSearch`) and the REST handler
   (`appview.WithProfileSearch`). REST's `search()` calls
   `h.profileSearcher.Search(...)` instead of `h.vertex.Search(...)`; the existing
   dedup + local-fallback + enrichment stay unchanged. Both transports now read the
   same cache and dedup live refreshes through the provider's singleflight.

2. **Typed-nil safety.** `vertexClient` is a `*vertex.Client`, which is a *typed nil*
   when no Vertex key is configured. Passing it into the `SearchRefreshClient`
   interface produces a non-nil interface, so the provider's `p.client == nil` guard
   is skipped and a cache-miss refresh panics on a nil-pointer method call. `cmd/api`
   now assigns through the interface only when non-nil, so the provider sees a true
   nil and returns `ErrUnavailable` (callers fall back to local). This also fixes the
   same latent panic on the GraphQL path.

3. **Accepted asymmetry.** With no Vertex key, `search()` degrades gracefully
   (cached → local index) while `recommended()` still returns `503`. This is
   intentional: search has a local floor, recommended has no local equivalent, so a
   `503` is the honest response.

4. **Scope.** The deliverable is result *ordering* + continuous pagerank
   `rank`/`score`. Adding `followers`/`searchRank`/`searchScore` JSON fields to the
   REST response (GraphQL sources these from a separate `CachedVertexProfiles`
   lookup) is deferred — the app renders in server order and treats those fields as
   optional.

## Consequences

- REST search matches GraphQL whenever the cache is warm (verified live for
  `Cal`/`calle`); both share the same cold-query limitation when the DVM is down.
- Shared-provider cache-miss refreshes bypass the handler's `querySlots` ClickHouse
  concurrency cap; load is bounded in practice (singleflight-deduped, miss-only) and
  is observable via the `appview.search` debug log (`vertex_count`/`local_count`/
  `from_cache`, `query_len` only — never the raw term).
- Reversible: revert the two-file change; `WithVertex` remains for `recommended()`.
