package clickhouse

import (
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
		"note_engagement_real",
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
