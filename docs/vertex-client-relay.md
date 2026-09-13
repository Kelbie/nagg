# Vertex client relay

Enable `NAGG_MODULES=mint,app,vertex` to serve Vertex lookups on the mint-sized
ClickHouse. `nostr` also includes the Vertex routes. No social graph tables,
new migrations, or new dependencies are required.

The app signs a Nostr DVM request locally with the querying user's key and
sends only the signed event to nagg. nagg validates it, subscribes for the
matching response, and forwards the event unchanged to the configured Vertex
relay. The nsec/private key never leaves the device. nagg does not re-sign it.

Public Vertex gives reputable keys 100 free credits per day. Refill depends on
age and sufficient walks in Vertex's graph; unknown, young, or low-reputation
keys do not refill. `followerCount` and the default `globalPagerank` cost one
credit; `personalizedPagerank` costs ten. The nagg bot key is not a reliable
source of credits. Client-signed requests can populate the shared cache, and
users without credits can read that cache without signing a request.

## Requests

`POST /nostr/vertex/relay` takes the signed event as its entire JSON body:

```jsonc
{
  "id": "<event hash>", "pubkey": "<querying user's hex pubkey>",
  "created_at": 1789257600, "kind": 5312,
  "tags": [["param", "target", "<profile pubkey>"], ["param", "limit", "7"]],
  "content": "", "sig": "<signature>"
}
```

Generate `created_at` at signing time; the example timestamp is illustrative.
Kinds are 5312 (profile), 5315 (search), and 5313 (recommend). Search uses
`["param","search","alice"]`; `limit` defaults to 5 and must be 1–100;
`sort` defaults to `globalPagerank`; `source`, when present, must be a lowercase
hex pubkey. Personalized requests require `source`. Profile `target` must be
lowercase hex. Duplicate/unknown params and ambiguous argument combinations
are rejected. Request content is limited to 1024 bytes, tags to 32, each tag
to eight strings of at most 1024 bytes, and the JSON body to 64 KiB.
Signatures and event IDs are checked; timestamps must be within ±300 seconds.

A successful response is `{ok:true, kind:"profile"|"search"|"recommend",
result:..., fetchedAt:<unix>, cached:<bool>}`. Profiles use `ProfileResult`
(lowercase `pubkey`, `rank`, optional `score`, `nodes`, `topFollowers`,
`vertexFetchedAt`, and the signed `response`). Search/recommend lists use the
existing `SearchResult` field names `PubKey`, `Npub`, `Rank`, `Score`, `Nodes`,
plus `vertexFetchedAt`. Only global Pagerank ranks are converted to scores.

Profile results write through `SaveVertexProfile`, including `vertex_scores`
when a score exists. Profile payload JSON retains the verified signed response.
The profile table is keyed only by target, so source-scoped/non-global profile
results are returned with `cached:false` rather than replacing global scores.
Search results use the existing query/sort/source/limit cache key. That table
has no signed-event payload column: it retains parsed data from the verified
response, not the original signature. No schema is added to archive search
responses. Empty search results have no cache row in the existing schema.
Recommendations have no suitable table and return `cached:false`.

The existing profile writer uses buffered ClickHouse inserts without waiting
for flush. The requesting client receives the freshly fetched result directly;
other readers may see the prior cache entry until flush (normally up to 15s).
An acknowledged buffer can be lost on database restart. This retains the
existing small-batch policy; see `insert-async-small-batches` in the ClickHouse
best-practices guidance for the durability tradeoff.

Insufficient credits and other terminal kind-7000 notices return HTTP 200:
`{ok:false, reason:"insufficient_credits"|"rejected", message:"..."}`. Messages
are fixed, sanitized text; upstream content is not reflected. Processing/success
notices wait for the result. Deadline expiry returns 504 with
`{ok:false,reason:"timeout"}`; other relay/cache errors return 502 with
`{ok:false,reason:"unavailable"}`. Disabled/unconfigured relay returns 503.
Invalid requests return 400; the shared per-signer limit returns 429.
These responses are never stored in the Redis/LRU response cache.

## Refresh while reading

Use the optional `svr` query parameter on `GET /nostr/profile` and
`GET|POST /nostr/search`: it is the **unpadded base64url encoding of the signed
event JSON**, called `signedVertexRequest` by the app. The same query parameter
is used with POST; there is no signed-event body field. POST search takes
`{query, limit?, sort?, source?}` as its JSON body.

```text
GET /nostr/profile?pubkey=<target>&svr=<base64url event>
GET /nostr/search?query=alice&limit=5&sort=globalPagerank&svr=<base64url event>
POST /nostr/search?svr=<base64url event>  body: {"query":"alice","limit":5}
```

The signed kind and target/search arguments must match the read request exactly
(after the standard default limit/sort normalization). Validation, personalized
policy, and the 10/min per-pubkey allowance are shared with the relay endpoint.
The DVM call runs synchronously, bounded by the existing 15-second timeout.
The read returns the fresh result directly, without waiting for cache visibility.
Failures use the relay error response above; omit `svr` to read the cache.
Without `svr`, existing search cache/local fallback behavior remains unchanged;
full Nostr profiles retain their server-key refresh behavior. Mint-mode profiles
use cached Vertex data regardless of the local follower gate and skip social
stats; missing kind-0 rows leave pubkeys and ranks usable.

Every Vertex provider object in these read envelopes includes
`vertexFetchedAt` (Unix seconds or null). Search includes `vertexFresh`, true
when the returned Vertex search data is younger than the declared seven-day
`Policy.CacheTTL`; cache misses/local-only results are false. A successful
signed search is fresh even when empty. `fromCache` remains independent: cached
data can still be fresh. Ordinary GET reads retain their response-cache TTL;
`svr` reads bypass it entirely, preventing cached or background credit spending.
All routes also have `/v1` aliases. Capabilities are
`appview.vertex.clientRelay` and `appview.vertex.fetchedAt`.

## Operator settings for nagg-mint

```text
NAGG_MODULES=mint,app,vertex
NAGG_VERTEX_RELAY_ENABLED=true
NAGG_VERTEX_RELAY=wss://relay.vertexlab.io
NAGG_VERTEX_CLIENT_MAX_PER_MIN=10
NAGG_VERTEX_ALLOW_PERSONALIZED=false
NAGG_VERTEX_SYNC_BATCH=20
NAGG_VERTEX_SYNC_THROTTLE=2s
```

Keep `NAGG_KINDS=0,38000` and `NAGG_FIREHOSE_KINDS=38000` unchanged.
`NAGG_VERTEX_PRIVATE_KEY` is optional; leave it unset for client relay/cache-only
operation. It enables the residual server-signed syncer (and legacy live read
fallbacks) only when configured. Never copy a user's private key into this env.
The syncer retains its 30-minute interval, refreshes eligible stale profiles,
and stops a tick after one `vertex.sync.credits_exhausted` warning. Without
Nostr tables it selects previously scored targets from `vertex_scores`; it does
not query the social graph. Eligibility remains the declared 500 inbound refs
and seven-day TTL. Nostr deployments retain batch 200 / throttle 0s defaults.

Adding these modules does not change mint DDL: core + mint migrations, the mint
rule registry, and the three plugin cache tables are identical before and after.
`schema_reconcile.go` computes desired columns from these declarations and
bounds table/view drops by `allModuleDDL`. The pure schema test verifies an
empty reconcile plan against the declared existing mint schema. No live
ClickHouse inspection is required; unrelated pre-existing schema drift would
still be subject to the existing reconciler.

## Privacy and trust

Vertex and nagg see which signing pubkey looked up which target or search term.
Signed requests are public Nostr events, not encrypted queries. Shared cache
results are readable by other clients. GET URLs containing `svr` may also be
visible to browser history or infrastructure access logs. The handler does not
log signed request tags or upstream notice text. Use the POST relay endpoint
when a URL-carried signed event is undesirable.

Response event IDs, signatures, response kinds, and request-ID correlation are
verified. As with the existing client, the configured relay is the upstream
trust boundary; verifying a signature alone does not establish a particular
Vertex operator identity. Point it only at the intended provider relay.
