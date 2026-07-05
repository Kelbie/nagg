// Package dvm is the generic seam for Nostr Data Vending Machine
// integrations. A DVM plugin declares its identity the same way the rules
// registry declares aggregations: as data the rest of the stack derives
// behavior from — the request/response kinds it speaks, the ClickHouse cache
// tables it owns (applied at Migrate time and reconciled exactly like
// rule-generated tables), and the named capabilities it offers. Consumers
// (app views, GraphQL, rank rules) reference a plugin by Name(), never by a
// hardcoded vendor string, so adding a second DVM is registration, not
// surgery.
//
// Deliberately outside this seam: plain HTTP upstreams (the cashu mint
// auditor) — they are clients, not DVMs — and the enricher's derived
// metrics, which are computed locally.
package dvm

import (
	"fmt"
	"strings"
	"time"
)

// KindPair is one request/response kind pair a plugin speaks (e.g. a 5312
// request answered by 6312 events, per the DVM convention of response kind =
// request kind + 1000).
type KindPair struct {
	Request  int
	Response int
}

// Policy declares when and how the rest of the stack consults a plugin —
// the usage rules, declared next to the identity, in the same
// declare-don't-hand-roll spirit as the rules registry.
type Policy struct {
	// CacheTTL is how long the plugin's cached values are trusted before a
	// refetch: the score sync refreshes pubkeys whose stored values are older
	// than this. Readers always serve whatever is cached (best-effort — a
	// young provider dataset improves in place on each TTL cycle).
	CacheTTL time.Duration
	// MinInboundRefs gates which pubkeys are worth the provider's time: only
	// pubkeys whose latest-list inbound reference fan-in (kind-3 p refs, the
	// latest_k3 projection) reaches this count are synced, rank-gated, or
	// refreshed through the plugin. The declarative form of the historical
	// "more than 500 followers" requirement. Zero disables the gate.
	MinInboundRefs uint64
}

// Plugin is one DVM integration. Name is the provider namespace clients see
// (envelope `providers` keys, rank-term score sources). The capability
// getters return the plugin's provider implementations, or nil when the
// capability is unsupported; callers type-assert to the interface they
// require — the concrete provider shapes stay owned by the plugin package,
// keeping this seam dependency-free.
type Plugin interface {
	Name() string
	Kinds() []KindPair
	// CacheDDL returns the plugin's ClickHouse cache-table CREATE statements
	// (IF NOT EXISTS, reapplied every Migrate and included in the schema
	// reconciler's desired set — dropping a plugin retires its tables).
	CacheDDL() []string
	// Policy declares when the stack consults this plugin (cache TTL,
	// inbound-ref gate). Zero values disable the respective behavior.
	Policy() Policy

	// ScoreProvider is the pubkey-score source used for rank gating; nil when
	// the plugin provides no scores.
	ScoreProvider() any
	// SearchProvider serves profile search; nil when unsupported.
	SearchProvider() any
	// RecommendProvider serves pubkey recommendations; nil when unsupported.
	RecommendProvider() any
}

// Registry holds the configured plugins. Construct with NewRegistry, which
// validates the set at startup.
type Registry struct {
	plugins []Plugin
	byName  map[string]Plugin
}

// NewRegistry validates and indexes the plugin set. A malformed plugin is a
// programming error, so callers should treat an error as fatal.
func NewRegistry(plugins ...Plugin) (*Registry, error) {
	r := &Registry{
		plugins: plugins,
		byName:  make(map[string]Plugin, len(plugins)),
	}
	for _, p := range plugins {
		name := p.Name()
		if name == "" || strings.ToLower(name) != name {
			return nil, fmt.Errorf("dvm plugin name %q must be a non-empty lowercase identifier", name)
		}
		if _, dup := r.byName[name]; dup {
			return nil, fmt.Errorf("dvm plugin %q registered twice", name)
		}
		if len(p.Kinds()) == 0 {
			return nil, fmt.Errorf("dvm plugin %q declares no kinds", name)
		}
		for _, ddl := range p.CacheDDL() {
			if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS") {
				return nil, fmt.Errorf("dvm plugin %q cache DDL must be CREATE TABLE IF NOT EXISTS statements", name)
			}
		}
		r.byName[name] = p
	}
	return r, nil
}

// MustRegistry is NewRegistry for wiring paths without an error channel.
func MustRegistry(plugins ...Plugin) *Registry {
	r, err := NewRegistry(plugins...)
	if err != nil {
		panic(fmt.Sprintf("dvm: invalid plugin set: %v", err))
	}
	return r
}

// Empty returns a registry with no plugins.
func Empty() *Registry { return &Registry{byName: map[string]Plugin{}} }

// Plugins returns the registered plugins in registration order.
func (r *Registry) Plugins() []Plugin { return r.plugins }

// Plugin returns the named plugin, or nil.
func (r *Registry) Plugin(name string) Plugin { return r.byName[name] }

// Has reports whether a plugin with this name is registered.
func (r *Registry) Has(name string) bool { return r.byName[name] != nil }

// Names returns the registered plugin names in registration order.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.plugins))
	for _, p := range r.plugins {
		out = append(out, p.Name())
	}
	return out
}

// CacheDDL returns every plugin's cache-table statements, in registration
// order — the store applies these alongside the rule-generated DDL.
func (r *Registry) CacheDDL() []string {
	var out []string
	for _, p := range r.plugins {
		out = append(out, p.CacheDDL()...)
	}
	return out
}
