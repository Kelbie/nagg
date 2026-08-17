// Package modules declares nagg's independently deployable slices.
//
// A deployment names the slices it wants in NAGG_MODULES and everything else
// follows from that single declaration: which migrations run, which rule
// registry drives the generated schema, which relay kinds are subscribed and
// stored, which HTTP routes mount, and which background workers start.
//
// This exists because the full app-view — a network-wide Nostr archive with
// ranking, enrichment, notifications and rollups — is far more machine than the
// cashu mint observatory needs. Splitting them by declaration lets one binary
// serve either a full deployment or a mint-only one whose ClickHouse holds a
// handful of tables.
//
// The zero value of Set means "every module", so any caller that never mentions
// modules behaves exactly as it did before this package existed.
package modules

import (
	"fmt"
	"sort"
	"strings"
)

// Module is one deployable slice.
type Module string

const (
	// Nostr is the social app-view: feed, thread, notifications, DMs, profiles,
	// search, follows, ranking, enrichment, rollup and retention.
	Nostr Module = "nostr"
	// Mint is the cashu mint observatory: NUT-06 snapshots over time, the
	// ecosystem changelog, the auditor merge, NIP-87 reviews and discovery.
	Mint Module = "mint"
	// App is the client-config surface: /app/latest-version, /app/ai-lineup.
	App Module = "app"
)

// Core is the pseudo-module every deployment carries: the raw ingestion tables,
// the migration ledger, the relay-backfill checkpoints, the system-log bounds.
// It is never named in NAGG_MODULES — it is what is left when you subtract
// every optional slice.
const Core Module = "core"

// known is the closed set NAGG_MODULES may name, in canonical order.
var known = []Module{Nostr, Mint, App}

// Set is an enabled-module set. The nil/zero Set means ALL modules, so a caller
// that never configures modules keeps the pre-modules behavior.
type Set map[Module]struct{}

// All returns the every-module set — the default, and today's behavior verbatim.
func All() Set {
	out := make(Set, len(known))
	for _, m := range known {
		out[m] = struct{}{}
	}
	return out
}

// Parse reads a comma-separated module list. Empty (or whitespace) yields All();
// an unknown name is an error rather than a silent drop, because a typo'd
// NAGG_MODULES that quietly fell back to "everything" would deploy the full
// archive on a machine sized for the mint observatory.
func Parse(csv string) (Set, error) {
	out := Set{}
	for _, part := range strings.Split(csv, ",") {
		name := Module(strings.ToLower(strings.TrimSpace(part)))
		if name == "" {
			continue
		}
		if !valid(name) {
			return nil, fmt.Errorf("unknown module %q (known: %s)", name, joinModules(known))
		}
		out[name] = struct{}{}
	}
	if len(out) == 0 {
		return All(), nil
	}
	return out, nil
}

// Has reports whether m is enabled. Core is always enabled, and the nil Set
// enables everything.
func (s Set) Has(m Module) bool {
	if m == Core {
		return true
	}
	if s == nil {
		return true
	}
	_, ok := s[m]
	return ok
}

// IsAll reports whether every known module is enabled — the signal that a
// deployment is a full one, used to keep whole-schema operations (the
// reconciler's drop pass) from running on a deliberately partial deployment.
func (s Set) IsAll() bool {
	if s == nil {
		return true
	}
	for _, m := range known {
		if _, ok := s[m]; !ok {
			return false
		}
	}
	return true
}

// String renders the enabled modules in canonical order, for startup logging.
func (s Set) String() string {
	if s == nil {
		return joinModules(known)
	}
	out := make([]Module, 0, len(s))
	for m := range s {
		out = append(out, m)
	}
	sort.Slice(out, func(i, j int) bool { return out[i] < out[j] })
	return joinModules(out)
}

// ParseTag validates a module name used as a schema/route ownership tag. Unlike
// Parse it also accepts "core", the always-on pseudo-module.
func ParseTag(name string) (Module, error) {
	m := Module(strings.ToLower(strings.TrimSpace(name)))
	if m == Core || valid(m) {
		return m, nil
	}
	return "", fmt.Errorf("unknown module tag %q (known: core,%s)", name, joinModules(known))
}

func valid(m Module) bool {
	for _, k := range known {
		if k == m {
			return true
		}
	}
	return false
}

func joinModules(mods []Module) string {
	parts := make([]string, 0, len(mods))
	for _, m := range mods {
		parts = append(parts, string(m))
	}
	return strings.Join(parts, ",")
}
