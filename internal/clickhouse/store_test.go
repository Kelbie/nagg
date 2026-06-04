package clickhouse

import (
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func TestExtractNoteZapUsesZapRequestAmount(t *testing.T) {
	targetID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	event := &nostr.Event{
		ID:     "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
		PubKey: "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
		Kind:   9735,
		Tags: nostr.Tags{
			{"e", targetID},
			{"description", `{"tags":[["amount","123000"],["e","dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"]]}`},
			{"bolt11", "lnbc999u1test"},
		},
	}

	zap, ok := extractNoteZap(event, time.Unix(1, 0))
	if !ok {
		t.Fatal("expected zap")
	}
	if zap.TargetEventID != targetID {
		t.Fatalf("target = %s", zap.TargetEventID)
	}
	if zap.Sats != 123 {
		t.Fatalf("sats = %d", zap.Sats)
	}
}

func TestSatsFromBolt11HRP(t *testing.T) {
	tests := map[string]uint64{
		"lnbc11test":      100_000_000,
		"lnbc2500u1test":  250_000,
		"lnbc20m1test":    2_000_000,
		"lnbc1000n1test":  100,
		"lnbc10000p1test": 1,
	}
	for invoice, want := range tests {
		if got := satsFromBolt11(invoice); got != want {
			t.Fatalf("%s sats = %d, want %d", invoice, got, want)
		}
	}
}

func TestTrendingCacheKeyBucketsSinceToMinute(t *testing.T) {
	since := time.Date(2026, 5, 31, 12, 34, 56, 0, time.UTC)
	roundedA, keyA := trendingCacheKey(since, 30)
	roundedB, keyB := trendingCacheKey(since.Add(3*time.Second), 30)

	if !roundedA.Equal(time.Date(2026, 5, 31, 12, 34, 0, 0, time.UTC)) {
		t.Fatalf("rounded = %s", roundedA)
	}
	if !roundedA.Equal(roundedB) || keyA != keyB {
		t.Fatalf("keys = %q/%q rounded = %s/%s", keyA, keyB, roundedA, roundedB)
	}
}

func TestNotificationPolicyThresholds(t *testing.T) {
	tests := map[string]struct {
		actor  float64
		viewer float64
	}{
		"RELAXED":  {actor: 0, viewer: 0},
		"MODERATE": {actor: 20, viewer: 60},
		"STRICT":   {actor: 50, viewer: 80},
		"":         {actor: 50, viewer: 80},
		"unknown":  {actor: 50, viewer: 80},
	}
	for policy, want := range tests {
		actor, viewer := notificationPolicyThresholds(policy)
		if actor != want.actor || viewer != want.viewer {
			t.Fatalf("%q thresholds = %.1f/%.1f, want %.1f/%.1f", policy, actor, viewer, want.actor, want.viewer)
		}
	}
}

func TestNotificationReasonForEvent(t *testing.T) {
	validEventID := strings.Repeat("a", 64)
	tests := map[string]struct {
		event EventView
		want  string
	}{
		"follow": {
			event: EventView{Kind: 3},
			want:  "follow",
		},
		"plain mention": {
			event: EventView{Kind: 1, Tags: [][]string{{"p", strings.Repeat("b", 64)}}},
			want:  "mention",
		},
		"reply": {
			event: EventView{Kind: 1, Tags: [][]string{{"e", validEventID}, {"p", strings.Repeat("b", 64)}}},
			want:  "reply",
		},
		"quote mention marker": {
			event: EventView{Kind: 1, Tags: [][]string{{"e", validEventID, "", "mention"}}},
			want:  "quote",
		},
		"q tag quote": {
			event: EventView{Kind: 1, Tags: [][]string{{"q", validEventID}, {"p", strings.Repeat("b", 64)}}},
			want:  "quote",
		},
		"reaction": {
			event: EventView{Kind: 7},
			want:  "reaction",
		},
		"zap": {
			event: EventView{Kind: 9735},
			want:  "zap",
		},
		"fallback": {
			event: EventView{Kind: 30023},
			want:  "custom",
		},
	}
	for name, tc := range tests {
		if got := notificationReasonForEvent(tc.event, "custom"); got != tc.want {
			t.Fatalf("%s reason = %q, want %q", name, got, tc.want)
		}
	}
}

func TestEventOrderByAddsShuffleTieBreaker(t *testing.T) {
	orderBy, args := eventOrderBy("e.created_at", "e.id", ShuffleInput{
		Seed:     "seed-a",
		Counter:  4,
		Strength: 0.2,
	})

	if !strings.Contains(orderBy, "cityHash64") {
		t.Fatalf("orderBy = %q", orderBy)
	}
	if len(args) != 2 || args[0] != "seed-a" || args[1] != 4 {
		t.Fatalf("args = %+v", args)
	}
}

func TestEventOrderByPreservesDefaultOrderWithoutShuffle(t *testing.T) {
	orderBy, args := eventOrderBy("e.created_at", "e.id", ShuffleInput{})

	if orderBy != "ORDER BY e.created_at DESC, e.id DESC" {
		t.Fatalf("orderBy = %q", orderBy)
	}
	if len(args) != 0 {
		t.Fatalf("args = %+v", args)
	}
}

func TestEventWhereAddsNegativeEventFilters(t *testing.T) {
	where, args := eventWhereInput("e", EventQueryInput{
		ExcludeIDs:     []string{"event-a"},
		ExcludePubKeys: []string{"pubkey-a"},
	})

	if !strings.Contains(where, "e.id NOT IN (?)") {
		t.Fatalf("where = %q", where)
	}
	if !strings.Contains(where, "e.pubkey NOT IN (?)") {
		t.Fatalf("where = %q", where)
	}
	if len(args) != 2 {
		t.Fatalf("args = %+v", args)
	}
}

func TestAggregateOrderByAddsShuffleTieBreaker(t *testing.T) {
	orderBy, args := aggregateOrderBy(aggSpec{
		groupDims:   []string{"tag_value"},
		orderMetric: "unique_pubkeys",
	}, ShuffleInput{
		Seed:     "agg-seed",
		Counter:  2,
		Strength: 0.5,
	})

	if !strings.Contains(orderBy, "unique_pubkeys DESC, cityHash64") {
		t.Fatalf("orderBy = %q", orderBy)
	}
	if len(args) != 2 || args[0] != "agg-seed" || args[1] != 2 {
		t.Fatalf("args = %+v", args)
	}
}

func TestDedupeTrendingClusterRowsKeepsFirstUniqueIDs(t *testing.T) {
	rows := []TrendingClusterRow{
		{ID: "cluster:a", Score: 10, EventCount: 10},
		{ID: "cluster:b", Score: 9, EventCount: 9},
		{ID: "cluster:a", Score: 8, EventCount: 8},
		{ID: "cluster:c", Score: 7, EventCount: 7},
	}

	got := dedupeTrendingClusterRows(rows, 3)

	if len(got) != 3 {
		t.Fatalf("rows = %+v", got)
	}
	if got[0].ID != "cluster:a" || got[1].ID != "cluster:b" || got[2].ID != "cluster:c" {
		t.Fatalf("rows = %+v", got)
	}
}
