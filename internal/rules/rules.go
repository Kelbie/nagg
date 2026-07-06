// Package rules is the declarative registry at the heart of nagg's generic
// data layer. Every kind-to-kind aggregation, event lifetime, and per-kind
// ingest cap is declared here as data; the ClickHouse schema (aggregate
// tables + materialized views), the ingest fan-out, the retention predicates,
// and the read-side lookup specs are all derived from these declarations.
//
// The vocabulary is deliberately unopinionated: rules speak in event kinds,
// tag keys, and targets — never in app-level concepts (posts, likes, reposts).
// A relationship is "(source kinds) --tag--> (target)"; what an app calls a
// "like count" is just the unique-actor metric of the rule counting kind-7
// events that e-reference an event.
//
// Two refresh tiers exist because not every aggregate can be maintained by a
// ClickHouse materialized view at insert time:
//
//   - RefreshIngest: derived entirely from the inserted rows. Plain tag
//     references become an AggregatingMergeTree fed by a generated MV over
//     event_tags; extractor-based references (e.g. zap receipts, whose target
//     and amount hide in nested JSON / bolt11) become rows in the generic
//     event_refs table written during InsertEvents, with a generated MV
//     aggregating them.
//   - RefreshPeriodic: inputs change outside ingest (vertex scores, reply-
//     graph semantics), so a rollup pass recomputes them on an interval.
//
// Rule names are the identifiers clients see in app-view `aggregates` maps
// and that rank formulas reference. The canonical convention is kind-derived
// and neutral: "k7_e" (kind-7 events referencing via e tag), "k6_16_e",
// "k9735_e" — see CanonicalName.
package rules

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// TargetType says what a reference points at, which determines target
// validation in generated SQL and how readers key their lookups.
type TargetType string

const (
	// TargetEventID targets a 64-hex event id (e/q tags, zap targets).
	TargetEventID TargetType = "event"
	// TargetPubkey targets a 64-hex pubkey (p tags).
	TargetPubkey TargetType = "pubkey"
	// TargetAddress targets a NIP-01 kind:pubkey:d address (a tags).
	TargetAddress TargetType = "address"
)

// Ref describes where a relationship's references come from. Exactly one of
// TagKey, Extractor, or Author must be set. TagKey references are fully
// declarative and maintained by a generated materialized view over
// event_tags; Extractor references name an entry in the extractor registry
// (extractors.go) and are materialized as event_refs rows during ingest;
// Author aggregates events against their own author's pubkey (no reference
// at all — "how many events of these kinds has each pubkey created").
type Ref struct {
	// TagKey is the tag whose value is the reference target ("e", "p", "q",
	// "a"). Empty when Extractor or Author is set.
	TagKey string
	// Marker optionally requires the tag's NIP-10 marker (tag position 3,
	// i.e. tag_extra[2] in event_tags) to equal this value, e.g. "reply".
	// Empty matches any marker including none.
	Marker string
	// Extractor names a registered extractor that derives references a plain
	// tag match cannot express (nested zap-request JSON, bolt11 amounts).
	Extractor string
	// Author aggregates by the source event's own pubkey. Target must be
	// TargetPubkey and the only valid metric is uniq_sources.
	Author bool
	// Target is what the reference points at.
	Target TargetType
}

// Agg is the aggregate function applied per target for one metric.
type Agg string

const (
	// AggUniqActors counts distinct referencing pubkeys — "how many people".
	AggUniqActors Agg = "uniq_actors"
	// AggUniqSources counts distinct referencing events — "how many events".
	AggUniqSources Agg = "uniq_sources"
	// AggSumValue sums the extractor-provided value — "how much".
	// Only valid on extractor-based refs (tag refs carry no value).
	AggSumValue Agg = "sum_value"
)

// Metric is one aggregated column of a relationship's table.
type Metric struct {
	// Name is the column name and the key clients see next to the rule name.
	Name string
	Agg  Agg
}

// Refresh selects the maintenance tier for a relationship.
type Refresh string

const (
	RefreshIngest   Refresh = "ingest"
	RefreshPeriodic Refresh = "periodic"
)

// Relationship declares a kind-to-kind reference aggregation: "aggregate
// events of Kinds that reference a Target via Ref, maintaining Metrics".
type Relationship struct {
	// Name is the client-visible aggregation identifier and the basis of the
	// generated table name (agg_<Name>). Use CanonicalName unless a rule
	// genuinely needs a bespoke identifier.
	Name    string
	Kinds   []int
	Ref     Ref
	Metrics []Metric
	Refresh Refresh
}

// LifetimePolicy renders the WHERE predicate selecting expired rows of the
// rule's kinds for deletion. idColumn abstracts the event-id column name so
// the same predicate can cascade across tables.
type LifetimePolicy interface {
	DeletePredicate(kinds []int, idColumn string) string
	// Describe returns a short neutral description for logs.
	Describe() string
}

// Lifetime declares when events of some kinds stop being stored. Absence of
// a Lifetime rule for a kind means the event lives forever.
type Lifetime struct {
	Name   string
	Kinds  []int
	Policy LifetimePolicy
}

// Supersession declares that once an author publishes a newer event of one of
// Kinds, the older versions are pruned (NIP-01 replaceable semantics; PerDTag
// versions per (author, d-tag) instead of per author). THE DEFAULT IS KEEP:
// kinds with no Supersession rule retain every version forever — declaring
// one is an explicit, per-kind opt into deleting replaced events, exactly
// like any other lifetime decision.
type Supersession struct {
	Name    string
	Kinds   []int
	PerDTag bool
}

// lifetime compiles the supersession into the retention machinery's rule
// shape; supersessions ARE lifetimes, just declared by intent.
func (s Supersession) lifetime() Lifetime {
	var policy LifetimePolicy = KeepLatestPerAuthor{}
	if s.PerDTag {
		policy = KeepLatestPerAuthorDTag{}
	}
	return Lifetime{Name: s.Name, Kinds: s.Kinds, Policy: policy}
}

// Cap limits how many events of Kinds a single pubkey may ingest within
// Window. Window == 0 means a lifetime cap (no window). Exempt authors
// (known viewers and their follows) bypass the cap when ExemptKnownViewers.
type Cap struct {
	Name               string
	Kinds              []int
	Max                int
	Window             time.Duration
	ExemptKnownViewers bool
}

// Backfill declares that events of Kinds are pulled from relay history
// systematically instead of only observed live. A live subscription only ever
// sees NEW publications, so long-lived, rarely-republished kinds
// (parameterized-replaceable app data being the canonical case) would take
// months to accumulate from the firehose alone. A Backfill rule makes the
// ingester walk each relay's stored history to exhaustion (NIP-01 until/limit
// pagination) and then re-walk the recent window every Resync, catching events
// published while nagg was offline or beyond the firehose's since window.
// Absence of a rule means a kind is live-plus-on-demand only — the right
// default for high-volume social kinds, whose history is deliberately bounded
// by the cap and lifetime rules.
type Backfill struct {
	Name  string
	Kinds []int
	// Resync is the interval between top-up walks once the initial history
	// walk has completed. 0 disables re-syncing (initial walk only).
	Resync time.Duration
}

// Registry holds the full declared rule set. Construct with New, which
// validates cross-references and uniqueness.
type Registry struct {
	relationships []Relationship
	projections   []Projection
	lifetimes     []Lifetime
	caps          []Cap
	backfills     []Backfill

	byName map[string]*Relationship
}

// New validates the rule set and returns a Registry. It is intended to run
// at startup: a malformed rule set is a programming error, so callers should
// treat an error as fatal. Supersessions compile into lifetime rules (listed
// first, so retention considers cheap supersession prunes before age rules).
func New(relationships []Relationship, projections []Projection, supersessions []Supersession, lifetimes []Lifetime, caps []Cap, backfills []Backfill) (*Registry, error) {
	compiled := make([]Lifetime, 0, len(supersessions)+len(lifetimes))
	for _, s := range supersessions {
		if s.Name == "" {
			return nil, fmt.Errorf("supersession: empty name")
		}
		if len(s.Kinds) == 0 {
			return nil, fmt.Errorf("supersession %q: no kinds", s.Name)
		}
		compiled = append(compiled, s.lifetime())
	}
	compiled = append(compiled, lifetimes...)
	lifetimes = compiled

	r := &Registry{
		relationships: relationships,
		projections:   projections,
		lifetimes:     lifetimes,
		caps:          caps,
		backfills:     backfills,
		byName:        make(map[string]*Relationship, len(relationships)),
	}
	seenProj := map[string]bool{}
	for _, proj := range projections {
		if err := validateProjection(proj); err != nil {
			return nil, fmt.Errorf("projection %q: %w", proj.Name, err)
		}
		if seenProj[proj.Name] {
			return nil, fmt.Errorf("projection %q: duplicate name", proj.Name)
		}
		seenProj[proj.Name] = true
	}
	for i := range relationships {
		rel := &relationships[i]
		if err := validateRelationship(rel); err != nil {
			return nil, fmt.Errorf("relationship %q: %w", rel.Name, err)
		}
		if _, dup := r.byName[rel.Name]; dup {
			return nil, fmt.Errorf("relationship %q: duplicate name", rel.Name)
		}
		r.byName[rel.Name] = rel
	}
	for _, lt := range lifetimes {
		if err := validateLifetime(r, lt); err != nil {
			return nil, fmt.Errorf("lifetime %q: %w", lt.Name, err)
		}
	}
	for _, c := range caps {
		if err := validateCap(c); err != nil {
			return nil, fmt.Errorf("cap %q: %w", c.Name, err)
		}
	}
	seenBackfill := map[string]bool{}
	for _, b := range backfills {
		if err := validateBackfill(b); err != nil {
			return nil, fmt.Errorf("backfill %q: %w", b.Name, err)
		}
		if seenBackfill[b.Name] {
			return nil, fmt.Errorf("backfill %q: duplicate name", b.Name)
		}
		seenBackfill[b.Name] = true
	}
	return r, nil
}

// Relationships returns the declared relationships in declaration order.
func (r *Registry) Relationships() []Relationship { return r.relationships }

// Lifetimes returns the declared lifetime rules in declaration order.
func (r *Registry) Lifetimes() []Lifetime { return r.lifetimes }

// Caps returns the declared cap rules in declaration order.
func (r *Registry) Caps() []Cap { return r.caps }

// Backfills returns the declared relay-history backfill rules in declaration
// order.
func (r *Registry) Backfills() []Backfill { return r.backfills }

// Relationship returns the named relationship, or nil.
func (r *Registry) Relationship(name string) *Relationship { return r.byName[name] }

// IngestExtractorRules returns the ingest-tier relationships that use a
// registered extractor: the set InsertEvents must fan out to event_refs.
func (r *Registry) IngestExtractorRules() []Relationship {
	var out []Relationship
	for _, rel := range r.relationships {
		if rel.Refresh == RefreshIngest && rel.Ref.Extractor != "" {
			out = append(out, rel)
		}
	}
	return out
}

func validateRelationship(rel *Relationship) error {
	if rel.Name == "" {
		return fmt.Errorf("empty name")
	}
	if !validIdent(rel.Name) {
		return fmt.Errorf("name must be a lowercase identifier ([a-z0-9_])")
	}
	if len(rel.Kinds) == 0 {
		return fmt.Errorf("no kinds")
	}
	sources := 0
	if rel.Ref.TagKey != "" {
		sources++
	}
	if rel.Ref.Extractor != "" {
		sources++
	}
	if rel.Ref.Author {
		sources++
	}
	if sources != 1 {
		return fmt.Errorf("exactly one of Ref.TagKey, Ref.Extractor, or Ref.Author must be set")
	}
	if rel.Ref.Extractor != "" {
		if _, ok := extractors[rel.Ref.Extractor]; !ok {
			return fmt.Errorf("unknown extractor %q", rel.Ref.Extractor)
		}
	}
	if rel.Ref.Marker != "" && rel.Ref.TagKey == "" {
		return fmt.Errorf("marker filters apply to tag refs only")
	}
	if rel.Ref.Author {
		if rel.Ref.Target != TargetPubkey {
			return fmt.Errorf("author refs must target pubkeys")
		}
		for _, m := range rel.Metrics {
			if m.Agg != AggUniqSources {
				return fmt.Errorf("author refs support uniq_sources metrics only")
			}
		}
	}
	switch rel.Ref.Target {
	case TargetEventID, TargetPubkey, TargetAddress:
	default:
		return fmt.Errorf("invalid target %q", rel.Ref.Target)
	}
	if len(rel.Metrics) == 0 {
		return fmt.Errorf("no metrics")
	}
	seen := map[string]bool{}
	for _, m := range rel.Metrics {
		if !validIdent(m.Name) {
			return fmt.Errorf("metric %q: name must be a lowercase identifier", m.Name)
		}
		if seen[m.Name] {
			return fmt.Errorf("metric %q: duplicate", m.Name)
		}
		seen[m.Name] = true
		switch m.Agg {
		case AggUniqActors, AggUniqSources:
		case AggSumValue:
			if rel.Ref.Extractor == "" {
				return fmt.Errorf("metric %q: sum_value requires an extractor ref", m.Name)
			}
		default:
			return fmt.Errorf("metric %q: invalid agg %q", m.Name, m.Agg)
		}
	}
	switch rel.Refresh {
	case RefreshIngest, RefreshPeriodic:
	default:
		return fmt.Errorf("invalid refresh %q", rel.Refresh)
	}
	return nil
}

func validateLifetime(r *Registry, lt Lifetime) error {
	if lt.Name == "" {
		return fmt.Errorf("empty name")
	}
	if len(lt.Kinds) == 0 {
		return fmt.Errorf("no kinds")
	}
	if lt.Policy == nil {
		return fmt.Errorf("nil policy")
	}
	if p, ok := lt.Policy.(MaxAgeUnlessReferenced); ok {
		for _, name := range p.ByRules {
			if r.byName[name] == nil {
				return fmt.Errorf("references unknown relationship %q", name)
			}
		}
		if p.Age <= 0 {
			return fmt.Errorf("non-positive age")
		}
	}
	return nil
}

func validateCap(c Cap) error {
	if c.Name == "" {
		return fmt.Errorf("empty name")
	}
	if len(c.Kinds) == 0 {
		return fmt.Errorf("no kinds")
	}
	if c.Max <= 0 {
		return fmt.Errorf("non-positive max")
	}
	if c.Window < 0 {
		return fmt.Errorf("negative window")
	}
	return nil
}

func validateBackfill(b Backfill) error {
	if b.Name == "" {
		return fmt.Errorf("empty name")
	}
	if !validIdent(b.Name) {
		return fmt.Errorf("name must be a lowercase identifier ([a-z0-9_])")
	}
	if len(b.Kinds) == 0 {
		return fmt.Errorf("no kinds")
	}
	if b.Resync < 0 {
		return fmt.Errorf("negative resync")
	}
	return nil
}

// CanonicalName derives the conventional rule name from kinds and a ref
// descriptor: k<kinds joined by _>_<ref>, e.g. CanonicalName([]int{6, 16},
// "e") == "k6_16_e". Keeping names mechanical keeps the client-visible
// vocabulary kind-based rather than concept-based.
func CanonicalName(kinds []int, ref string) string {
	ks := append([]int(nil), kinds...)
	sort.Ints(ks)
	parts := make([]string, 0, len(ks)+1)
	for _, k := range ks {
		parts = append(parts, strconv.Itoa(k))
	}
	return "k" + strings.Join(parts, "_") + "_" + ref
}

func validIdent(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if (c < 'a' || c > 'z') && (c < '0' || c > '9') && c != '_' {
			return false
		}
	}
	return true
}
