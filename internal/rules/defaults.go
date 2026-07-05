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

	return New(relationships, supersessions, lifetimes, caps)
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
