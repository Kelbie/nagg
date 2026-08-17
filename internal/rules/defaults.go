package rules

import (
	"fmt"
	"time"
)

// Default declares nagg's production rule set. Semantics intentionally match
// the previous hand-written aggregates one-to-one so the generic layer can be
// verified against known behavior:
//
//	k7_e          ← note_like_counts   (unique reacting pubkeys per event)
//	k6_16_e       ← note_repost_counts (unique reposting pubkeys per event)
//	k1_q          ← note_quote_counts  (unique quoting events per event)
//	k9735_e       ← note_zap_totals    (sat sum + unique receipts per event)
//	k1_1111_e_reply ← note_direct_reply_counts (unique direct replies; the
//	                  NIP-10 direct-parent resolution runs in the rollup, so
//	                  this rule is periodic — the rollup owns its writes)
//	k1_1111_author  ← user_post_counts (unique events created per pubkey)
//
// capMax is the per-window event cap for the ingest cap rule (the
// NAGG_POST_CAP_PER_DAY config value, default 20); capMax <= 0 omits the cap
// rule entirely, matching the old "0 disables" semantics.
func Default(capMax int) (*Registry, error) {
	relationships := []Relationship{
		{
			Name:    CanonicalName([]int{7}, "e"),
			Kinds:   []int{7},
			Ref:     Ref{TagKey: "e", Target: TargetEventID},
			Metrics: []Metric{{Name: "actors", Agg: AggUniqActors}},
			Refresh: RefreshIngest,
		},
		{
			Name:    CanonicalName([]int{6, 16}, "e"),
			Kinds:   []int{6, 16},
			Ref:     Ref{TagKey: "e", Target: TargetEventID},
			Metrics: []Metric{{Name: "actors", Agg: AggUniqActors}},
			Refresh: RefreshIngest,
		},
		{
			Name:    CanonicalName([]int{1}, "q"),
			Kinds:   []int{1},
			Ref:     Ref{TagKey: "q", Target: TargetEventID},
			Metrics: []Metric{{Name: "sources", Agg: AggUniqSources}},
			Refresh: RefreshIngest,
		},
		{
			Name:  CanonicalName([]int{9735}, "e"),
			Kinds: []int{9735},
			Ref:   Ref{Extractor: "zap_target", Target: TargetEventID},
			Metrics: []Metric{
				{Name: "value_total", Agg: AggSumValue},
				{Name: "sources", Agg: AggUniqSources},
			},
			Refresh: RefreshIngest,
		},
		{
			// Direct replies: an e reference resolved to the NIP-10 direct
			// parent (reply marker > unmarked trailing e > root marker, q
			// exclusions). That resolution needs the reply-edge pass, so the
			// rollup recomputes this aggregate periodically.
			Name:    CanonicalName([]int{1, 1111}, "e_reply"),
			Kinds:   []int{1, 1111},
			Ref:     Ref{TagKey: "e", Marker: "reply", Target: TargetEventID},
			Metrics: []Metric{{Name: "sources", Agg: AggUniqSources}},
			Refresh: RefreshPeriodic,
		},
		{
			Name:    CanonicalName([]int{1, 1111}, "author"),
			Kinds:   []int{1, 1111},
			Ref:     Ref{Author: true, Target: TargetPubkey},
			Metrics: []Metric{{Name: "sources", Agg: AggUniqSources}},
			Refresh: RefreshIngest,
		},
	}

	// Latest-per-author projections: the kind-0 metadata table and the
	// kind-3 reference-list table, declared instead of hand-written.
	projections := []Projection{
		k0Projection(),
		{
			Name:   "k3",
			Kinds:  []int{3},
			Fields: []ProjField{{Name: "refs", TagKey: "p"}},
		},
	}

	// Superseded-version pruning is opt-in per kind; anything not listed
	// here keeps every version forever. These kinds are pruned because
	// relays and every reader only ever use the newest version (measured:
	// 80-98% of stored rows were superseded dead weight).
	supersessions := []Supersession{
		{Name: "replaceable_latest", Kinds: []int{0, 3, 10050, 10051}},
		{Name: "param_replaceable_latest", Kinds: []int{30078, 38000}, PerDTag: true},
	}

	lifetimes := []Lifetime{
		{
			// Gift wraps addressed to nobody this app-view serves are dead
			// weight (99% of stored wraps when measured); the matching
			// AddresseeGate stops new ones at the firehose and this rule
			// erodes the stored backlog.
			Name:   "k1059_known_addressee",
			Kinds:  []int{1059},
			Policy: KeepAddressedToKnown{},
		},
		{
			// Events of kinds 1/1111 that nothing ever referenced expire
			// after a year. The referencing relationships' aggregate tables
			// outlive the referencing events, so pruning a referencing event
			// never un-references its target.
			Name:  "k1_1111_unreferenced_1y",
			Kinds: []int{1, 1111},
			Policy: MaxAgeUnlessReferenced{
				Age: 365 * 24 * time.Hour,
				ByRules: []string{
					CanonicalName([]int{7}, "e"),
					CanonicalName([]int{6, 16}, "e"),
					CanonicalName([]int{1}, "q"),
					CanonicalName([]int{9735}, "e"),
					CanonicalName([]int{1, 1111}, "e_reply"),
				},
			},
		},
	}

	var caps []Cap
	if capMax > 0 {
		caps = append(caps, Cap{
			// A non-exempt pubkey may ingest at most capMax events of these
			// kinds per 24h window (known viewers and their follows exempt).
			Name:               "k1_1111_6_16_daily",
			Kinds:              []int{1, 1111, 6, 16},
			Max:                capMax,
			Window:             24 * time.Hour,
			ExemptKnownViewers: true,
		})
	}

	// Relay-history backfills: kinds served as a browsable network-wide corpus
	// need the events that already exist on the relays, not just the live
	// trickle. Measured 2026-07: the configured relay set held ~1.5k kind-38000
	// events while months of live listening had captured 23.
	backfills := []Backfill{
		{Name: "k38000_history", Kinds: []int{38000}, Resync: 24 * time.Hour},
	}

	// Firehose addressee gates: kinds only ever readable by their p-tagged
	// recipient are ingested only when that recipient is in the exemption
	// universe. Pairs with the k1059_known_addressee lifetime above.
	addresseeGates := []AddresseeGate{
		{Name: "k1059_known_addressee", Kinds: []int{1059}},
	}

	return New(relationships, projections, supersessions, lifetimes, caps, backfills, addresseeGates)
}

// k0Projection is the latest-kind-0-per-author extraction backing latest_k0 —
// the table every profile read goes through (Store.LatestK0). Both rule sets
// declare it, so it lives here rather than being copied: a field added for the
// full app-view must not silently miss the mint deployment's operator profiles.
func k0Projection() Projection {
	return Projection{
		Name:  "k0",
		Kinds: []int{0},
		Fields: []ProjField{
			{Name: "name", JSONPath: "name"},
			{Name: "display_name", JSONPath: "display_name"},
			{Name: "picture", JSONPath: "picture"},
			{Name: "about", JSONPath: "about"},
			{Name: "nip05", JSONPath: "nip05"},
			{Name: "lud16", JSONPath: "lud16"},
			{Name: "lud06", JSONPath: "lud06"},
			{Name: "banner", JSONPath: "banner"},
			{Name: "website", JSONPath: "website"},
			{Name: "raw_json", RawContent: true},
		},
	}
}

// Mint declares the rule set of a mint-only deployment (NAGG_MODULES=mint): the
// cashu mint observatory and nothing else.
//
// What it keeps and why:
//
//   - the kind-0 projection, because /nostr/mint/reviews and /nostr/mint/discover
//     bundle each reviewer's and operator's profile through Store.LatestK0. Those
//     profiles arrive via on-demand relay fetches for the handful of pubkeys that
//     actually appear, NOT from a global kind-0 firehose subscription.
//   - NIP-01 replaceable pruning for kinds 0 and 38000, so re-seen versions of a
//     profile or a mint recommendation don't accumulate.
//   - the NIP-87 relay-history walk, because a live firehose alone captures
//     almost no kind-38000 (measured 2026-07: 23 live vs ~1.5k on the relays).
//
// What it drops, and what that buys: no relationships, so GeneratedDDL emits no
// aggregate tables, no materialized views and no event_refs — and with no
// extractor rules, InsertEvents skips the event_refs batch entirely (see
// Registry.IngestExtractorRules). No caps and no addressee gates either: at
// kind-38000 volume there is nothing to ration.
func Mint() (*Registry, error) {
	projections := []Projection{k0Projection()}

	supersessions := []Supersession{
		{Name: "replaceable_latest", Kinds: []int{0}},
		{Name: "param_replaceable_latest", Kinds: []int{38000}, PerDTag: true},
	}

	backfills := []Backfill{
		{Name: "k38000_history", Kinds: []int{38000}, Resync: 24 * time.Hour},
	}

	return New(nil, projections, supersessions, nil, nil, backfills, nil)
}

// MustMint is Mint for wiring paths without an error channel, mirroring
// MustDefault: the rule set is compile-time data validated by unit tests, so a
// failure is a programming error worth crashing on.
func MustMint() *Registry {
	r, err := Mint()
	if err != nil {
		panic(fmt.Sprintf("rules: invalid mint rule set: %v", err))
	}
	return r
}

// HistoryFloorBackfill builds the deep-history walk rule for the firehose
// kind set: walk every kind down to floor (unix seconds, NAGG_HISTORY_FLOOR).
// Kinds already covered by an exhaustion backfill (Floor == 0) in existing are
// excluded — those walks are strictly deeper than any floor walk. ok is false
// when the floor is unset or no kinds remain. The literal is valid by
// construction (lowercase-ident name, non-empty kinds, positive floor), so it
// never needs to round-trip through Registry validation.
func HistoryFloorBackfill(kinds []int, floor int64, existing []Backfill) (Backfill, bool) {
	if floor <= 0 || len(kinds) == 0 {
		return Backfill{}, false
	}
	covered := map[int]bool{}
	for _, b := range existing {
		if b.Floor == 0 {
			for _, k := range b.Kinds {
				covered[k] = true
			}
		}
	}
	out := make([]int, 0, len(kinds))
	for _, k := range kinds {
		if !covered[k] {
			out = append(out, k)
		}
	}
	if len(out) == 0 {
		return Backfill{}, false
	}
	return Backfill{Name: "firehose_floor", Kinds: out, Floor: floor}, true
}

// MustDefault is Default for wiring paths without an error channel (config
// construction): the default rule set is compile-time data validated by unit
// tests, so a failure is a programming error worth crashing on.
func MustDefault(capMax int) *Registry {
	r, err := Default(capMax)
	if err != nil {
		panic(fmt.Sprintf("rules: invalid default rule set: %v", err))
	}
	return r
}
