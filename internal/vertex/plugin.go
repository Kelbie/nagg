package vertex

import (
	"time"

	"github.com/vertex-lab/nagg/internal/dvm"
)

// PluginName is the provider namespace Vertex data appears under (envelope
// `providers` keys, rank-term score sources, the vertex_scores source column).
const PluginName = "vertex"

// Plugin adapts the Vertex DVM to the generic dvm.Plugin seam. Identity
// (name, kinds, cache DDL) is static and available from construction — the
// store needs it at migrate time, before any client exists. Runtime providers
// are attached afterwards by the process wiring (cmd/api) once the store and
// relay client are up; a provider that was never attached simply reports the
// capability as unsupported.
type Plugin struct {
	search    *SearchProvider
	recommend *Client
}

// NewPlugin returns the Vertex plugin with its static identity. Attach
// runtime capabilities with WithSearch/WithRecommend.
func NewPlugin() *Plugin { return &Plugin{} }

// WithSearch attaches the cache-backed profile-search provider.
func (p *Plugin) WithSearch(search *SearchProvider) *Plugin {
	p.search = search
	return p
}

// WithRecommend attaches the live DVM client used for recommendations.
func (p *Plugin) WithRecommend(client *Client) *Plugin {
	p.recommend = client
	return p
}

// Policy: cached values are trusted for 7 days before the sync refetches
// them (best-effort scores that sharpen as the provider's graph matures),
// and only pubkeys with at least MinInboundRefs latest-list inbound refs
// are worth consulting the provider for — the declarative form of the
// historical >500-followers requirement.
//
// BOOTSTRAP SETTINGS while the self-hosted provider's graph is young:
//   - CacheTTL 1 minute (target: 7 days): effectively always-refetch — every
//     sync tick re-scores its whole batch, so improving graph values land in
//     nagg as fast as the sync can carry them instead of being trusted for a
//     week. Raise to 7 * 24h once the graph has converged.
//   - MinInboundRefs 100 (target: 500): in-graph follower counts lag the
//     real network; at 500 the For-You rank gate matched almost nobody.
func (p *Plugin) Policy() dvm.Policy {
	return dvm.Policy{
		CacheTTL:       time.Minute,
		MinInboundRefs: 100,
	}
}

func (p *Plugin) Name() string { return PluginName }

func (p *Plugin) Kinds() []dvm.KindPair {
	return []dvm.KindPair{
		{Request: ProfileRequestKind, Response: ProfileResponseKind},
		{Request: RecommendRequestKind, Response: RecommendResponseKind},
		{Request: SearchRequestKind, Response: SearchResponseKind},
	}
}

// CacheDDL declares the plugin's ClickHouse cache tables: verified profile
// payloads, the synced pubkey scores rank gating reads, and the search-result
// cache. These were previously static migration SQL (002/003); they now live
// with the plugin so dropping the plugin retires them via the reconciler.
func (p *Plugin) CacheDDL() []string {
	return []string{
		`CREATE TABLE IF NOT EXISTS vertex_scores
(
    source LowCardinality(String) DEFAULT 'vertex',
    pubkey FixedString(64),
    score Float64,
    rank Float64,
    followers UInt64,
    nodes UInt64,
    fetched_at DateTime
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (source, pubkey);`,
		`CREATE TABLE IF NOT EXISTS vertex_profile_cache
(
    pubkey FixedString(64),
    fetched_at DateTime,
    payload String
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY pubkey;`,
		`CREATE TABLE IF NOT EXISTS vertex_search_cache
(
    query_norm String,
    sort LowCardinality(String) DEFAULT 'globalPagerank',
    source String DEFAULT '',
    requested_limit UInt64,
    position UInt64,
    pubkey FixedString(64),
    rank Nullable(Float64),
    score Nullable(Float64),
    nodes UInt64,
    fetched_at DateTime
)
ENGINE = ReplacingMergeTree(fetched_at)
ORDER BY (query_norm, sort, source, requested_limit, position, pubkey);`,
	}
}

// ScoreProvider: Vertex scores flow through the vertex_scores cache table
// (written by the Syncer, read by the store's PubkeyScores) rather than a
// per-call provider object, so there is no handle to expose here — the
// capability shows up as the plugin's Name() being a valid score source.
func (p *Plugin) ScoreProvider() any { return nil }

func (p *Plugin) SearchProvider() any {
	if p.search == nil {
		return nil
	}
	return p.search
}

func (p *Plugin) RecommendProvider() any {
	if p.recommend == nil {
		return nil
	}
	return p.recommend
}
