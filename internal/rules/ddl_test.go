package rules

import (
	"strings"
	"testing"
)

func mustDefault(t *testing.T) *Registry {
	t.Helper()
	r, err := Default(20)
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	return r
}

func TestGeneratedDDLTagRule(t *testing.T) {
	r := mustDefault(t)
	ddl := strings.Join(r.GeneratedDDL(), "\n\n")

	// The k7_e pair must reproduce the semantics of the retired
	// note_like_counts: an AggregatingMergeTree of uniq(pubkey) states keyed
	// by the 64-hex e-tag target, fed by an MV over event_tags.
	wantTable := `CREATE TABLE IF NOT EXISTS agg_k7_e
(
    target FixedString(64),
    actors AggregateFunction(uniq, FixedString(64))
)
ENGINE = AggregatingMergeTree
ORDER BY target;`
	if !strings.Contains(ddl, wantTable) {
		t.Errorf("missing k7_e table DDL; got:\n%s", ddl)
	}

	wantView := `CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_k7_e
TO agg_k7_e
AS
SELECT
    tag_value AS target,
    uniqState(pubkey) AS actors
FROM event_tags
WHERE kind IN (7) AND tag_key = 'e' AND length(tag_value) = 64
GROUP BY target;`
	if !strings.Contains(ddl, wantView) {
		t.Errorf("missing k7_e view DDL; got:\n%s", ddl)
	}

	if !strings.Contains(ddl, "WHERE kind IN (6, 16) AND tag_key = 'e'") {
		t.Errorf("k6_16_e view must filter kinds 6 and 16")
	}
	if !strings.Contains(ddl, "uniqState(event_id) AS sources") {
		t.Errorf("k1_q view must count unique source events")
	}
}

func TestGeneratedDDLExtractorRule(t *testing.T) {
	r := mustDefault(t)
	ddl := strings.Join(r.GeneratedDDL(), "\n\n")

	if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS event_refs") {
		t.Fatalf("extractor rules require the event_refs table")
	}
	wantView := `CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_k9735_e
TO agg_k9735_e
AS
SELECT
    target,
    sumState(value) AS value_total,
    uniqState(source_event_id) AS sources
FROM event_refs
WHERE rule = 'k9735_e'
GROUP BY target;`
	if !strings.Contains(ddl, wantView) {
		t.Errorf("missing k9735_e view DDL; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "value_total AggregateFunction(sum, UInt64)") {
		t.Errorf("sum metric column must be AggregateFunction(sum, UInt64)")
	}
}

func TestGeneratedDDLAuthorRule(t *testing.T) {
	r := mustDefault(t)
	ddl := strings.Join(r.GeneratedDDL(), "\n\n")

	wantView := `CREATE MATERIALIZED VIEW IF NOT EXISTS mv_agg_k1_1111_author
TO agg_k1_1111_author
AS
SELECT
    pubkey AS target,
    uniqState(id) AS sources
FROM nostr_events
WHERE kind IN (1, 1111)
GROUP BY target;`
	if !strings.Contains(ddl, wantView) {
		t.Errorf("missing author-rule view DDL; got:\n%s", ddl)
	}
}

func TestPeriodicRuleGetsTableButNoView(t *testing.T) {
	r := mustDefault(t)
	ddl := strings.Join(r.GeneratedDDL(), "\n\n")

	if !strings.Contains(ddl, "CREATE TABLE IF NOT EXISTS agg_k1_1111_e_reply") {
		t.Errorf("periodic rule must still own an aggregate table")
	}
	if strings.Contains(ddl, "mv_agg_k1_1111_e_reply") {
		t.Errorf("periodic rule must not generate a materialized view")
	}
}

func TestMarkerFilter(t *testing.T) {
	rel := Relationship{
		Name:    "k1_e_reply_marked",
		Kinds:   []int{1},
		Ref:     Ref{TagKey: "e", Marker: "reply", Target: TargetEventID},
		Metrics: []Metric{{Name: "sources", Agg: AggUniqSources}},
		Refresh: RefreshIngest,
	}
	r, err := New([]Relationship{rel}, nil, nil, nil, nil, nil, nil)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ddl := strings.Join(r.GeneratedDDL(), "\n\n")
	if !strings.Contains(ddl, "arrayElement(tag_extra, 2) = 'reply'") {
		t.Errorf("marker filter missing from view; got:\n%s", ddl)
	}
}

func TestBackfillSQL(t *testing.T) {
	r := mustDefault(t)

	sql, ok := BackfillSQL(*r.Relationship("k7_e"))
	if !ok {
		t.Fatalf("tag rule must have backfill SQL")
	}
	if !strings.HasPrefix(sql, "INSERT INTO agg_k7_e\nSELECT") {
		t.Errorf("backfill = %q", sql)
	}

	if _, ok := BackfillSQL(*r.Relationship("k9735_e")); ok {
		t.Errorf("extractor rule must not claim SQL backfill")
	}

	if sql, ok := BackfillSQL(*r.Relationship("k1_1111_author")); !ok || !strings.Contains(sql, "FROM nostr_events") {
		t.Errorf("author rule backfill = %q, ok=%v", sql, ok)
	}
}

func TestReadSpec(t *testing.T) {
	r := mustDefault(t)

	spec, ok := r.ReadSpec("k9735_e", "value_total")
	if !ok || spec.Table != "agg_k9735_e" || spec.Column != "value_total" || spec.MergeFunc != "sumMerge" {
		t.Errorf("spec = %+v ok=%v", spec, ok)
	}
	spec, ok = r.ReadSpec("k7_e", "actors")
	if !ok || spec.MergeFunc != "uniqMerge" {
		t.Errorf("spec = %+v ok=%v", spec, ok)
	}
	if _, ok := r.ReadSpec("k7_e", "nope"); ok {
		t.Errorf("unknown metric must not resolve")
	}
	if _, ok := r.ReadSpec("nope", "actors"); ok {
		t.Errorf("unknown rule must not resolve")
	}
}

func TestLifetimePredicates(t *testing.T) {
	r := mustDefault(t)

	var unref *Lifetime
	for i := range r.Lifetimes() {
		if r.Lifetimes()[i].Name == "k1_1111_unreferenced_1y" {
			unref = &r.Lifetimes()[i]
		}
	}
	if unref == nil {
		t.Fatalf("missing k1_1111_unreferenced_1y lifetime")
	}
	pred := unref.Policy.DeletePredicate(unref.Kinds, "id")
	for _, want := range []string{
		"kind IN (1, 1111)",
		"SELECT target AS ref_target FROM agg_k7_e",
		"UNION ALL SELECT target AS ref_target FROM agg_k9735_e",
		"id NOT IN",
	} {
		if !strings.Contains(pred, want) {
			t.Errorf("predicate missing %q:\n%s", want, pred)
		}
	}

	latest := KeepLatestPerAuthor{}.DeletePredicate([]int{0, 3}, "id")
	if !strings.Contains(latest, "argMax(id, created_at)") || !strings.Contains(latest, "GROUP BY kind, pubkey") {
		t.Errorf("keep-latest predicate:\n%s", latest)
	}

	addressed := KeepAddressedToKnown{}.DeletePredicate([]int{1059}, "id")
	for _, want := range []string{
		"kind IN (1059)",
		// Fail closed on an empty viewer registry: without this guard a
		// wiped known_viewers table would mass-delete every wrap.
		"(SELECT count() FROM known_viewers) > 0",
		"tag_key = 'p'",
		"arrayJoin(refs) FROM latest_k3 FINAL",
		"id NOT IN",
	} {
		if !strings.Contains(addressed, want) {
			t.Errorf("addressed-to-known predicate missing %q:\n%s", want, addressed)
		}
	}
}
