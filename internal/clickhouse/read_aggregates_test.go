package clickhouse

import (
	"regexp"
	"strings"
	"testing"

	"github.com/vertex-lab/nagg/internal/rules"
)

func TestBuildEventAggregatesQuery(t *testing.T) {
	reg, err := rules.Default(20)
	if err != nil {
		t.Fatalf("rules.Default: %v", err)
	}
	query, cols, bindSlots := buildEventAggregatesQuery(reg)

	for _, want := range []string{
		"SELECT target AS id FROM agg_k7_e WHERE target IN (?)",
		"LEFT JOIN (SELECT target, uniqMerge(actors) AS actors FROM agg_k7_e WHERE target IN (?) GROUP BY target)",
		"LEFT JOIN (SELECT target, sumMerge(value_total) AS value_total, uniqMerge(sources) AS sources FROM agg_k9735_e WHERE target IN (?) GROUP BY target)",
		"gated_ref_counts",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query missing %q:\n%s", want, query)
		}
	}
	// The author rule targets pubkeys and must NOT join into the event read.
	if strings.Contains(query, "agg_k1_1111_author") {
		t.Errorf("author (pubkey-target) rule must not appear in event aggregates")
	}
	// Bind slots must match the number of ? placeholders.
	if got := strings.Count(query, "(?)"); got != bindSlots {
		t.Errorf("bindSlots = %d, placeholders = %d", bindSlots, got)
	}
	// Column mapping covers each event rule metric + the 7 vertex-real values.
	wantCols := map[string]bool{}
	for _, c := range cols {
		wantCols[c.rule+"."+c.metric] = true
	}
	for _, key := range []string{
		"k7_e.actors", "k6_16_e.actors", "k1_q.sources",
		"k9735_e.value_total", "k9735_e.sources", "k1_1111_e_reply.sources",
		"vertex_k7_e.actors", "vertex_actors.actors", "vertex_k9735_e.value_total",
	} {
		if !wantCols[key] {
			t.Errorf("cols missing %s (have %v)", key, wantCols)
		}
	}
}

// TestPubkeyStatsReadMatchesDeclaredColumns pins the BatchPubkeyStats read to
// the columns the schema actually declares — SQL strings aren't
// compile-checked, and a table rename that misses one read surfaces as a
// production 500 (this exact bug shipped once: the query still said
// `followers` after the pubkey_stats rename).
func TestPubkeyStatsReadMatchesDeclaredColumns(t *testing.T) {
	desired, err := parseDesiredSchema(embeddedMigrations())
	if err != nil {
		t.Fatalf("parseDesiredSchema: %v", err)
	}
	cols, ok := desired.tables["pubkey_stats"]
	if !ok {
		t.Fatalf("pubkey_stats not declared")
	}
	for _, want := range []string{"pubkey", "k3_out", "k3_in", "k1_1111_authored", "computed_at"} {
		if _, ok := cols[want]; !ok {
			t.Errorf("pubkey_stats missing declared column %q", want)
		}
	}
	for gone := range map[string]struct{}{"followers": {}, "following": {}, "posts": {}} {
		if _, ok := cols[gone]; ok {
			t.Errorf("pubkey_stats still declares retired column %q", gone)
		}
	}
	if !strings.Contains(batchPubkeyStatsQuery, "argMax(k3_in, computed_at)") {
		t.Errorf("BatchPubkeyStats must read k3_in:\n%s", batchPubkeyStatsQuery)
	}
}

// TestRankFeaturesReadMatchesDeclaredColumns pins the ranked-feed DB path's
// column references against the declared rank_features schema — the second
// rename-missed-a-read bug (the query aliased RAW columns as gated_* and
// referenced a nonexistent `actors`), caught live by the For-You feed.
func TestRankFeaturesReadMatchesDeclaredColumns(t *testing.T) {
	desired, err := parseDesiredSchema(embeddedMigrations())
	if err != nil {
		t.Fatalf("parseDesiredSchema: %v", err)
	}
	cols, ok := desired.tables["rank_features"]
	if !ok {
		t.Fatalf("rank_features not declared")
	}
	query, _ := buildRankedEventsByFeaturesQuery(FeatureRankInput{Limit: 10, Since: 1})
	for _, ref := range regexp.MustCompile(`argMax\(([a-z0-9_]+),`).FindAllStringSubmatch(query, -1) {
		if _, ok := cols[ref[1]]; !ok {
			t.Errorf("query references undeclared rank_features column %q", ref[1])
		}
	}
	for _, want := range []string{"gated_k7_e_actors", "gated_actors", "author_score"} {
		if !strings.Contains(query, "argMax("+want+",") {
			t.Errorf("query must read declared column %q", want)
		}
	}
}
