package clickhouse

import (
	"strings"
	"testing"
)

func TestEstimateStoredBytes(t *testing.T) {
	tests := map[string]struct {
		count      uint64
		totalBytes uint64
		totalRows  uint64
		want       uint64
	}{
		"empty count":  {count: 0, totalBytes: 100, totalRows: 10, want: 0},
		"empty table":  {count: 10, totalBytes: 0, totalRows: 0, want: 0},
		"proportional": {count: 25, totalBytes: 1_000, totalRows: 100, want: 250},
		"rounded":      {count: 2, totalBytes: 10, totalRows: 3, want: 7},
	}
	for name, tc := range tests {
		if got := estimateStoredBytes(tc.count, tc.totalBytes, tc.totalRows); got != tc.want {
			t.Fatalf("%s estimate = %d, want %d", name, got, tc.want)
		}
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

func TestEventWhereAddsContentSearch(t *testing.T) {
	where, args := eventWhereInput("e", EventQueryInput{Search: "calle"})

	if !strings.Contains(where, "positionCaseInsensitiveUTF8(e.content, ?) > 0") {
		t.Fatalf("where = %q", where)
	}
	if len(args) != 1 || args[0] != "calle" {
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
