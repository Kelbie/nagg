package clickhouse

import (
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
