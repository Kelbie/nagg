package ingest

import (
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

func capTestPipeline(capPerDay int, exempt func(string) bool) *Pipeline {
	p := New(nil, Config{BatchSize: 10, PostCapPerDay: capPerDay}, WithExemption(exempt))
	base := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	p.cap.now = func() time.Time { return base }
	return p
}

func capTestEvent(pubkey string, kind int) *nostr.Event {
	return &nostr.Event{PubKey: pubkey, Kind: kind}
}

func TestPostCapDropsBeyondDailyLimit(t *testing.T) {
	p := capTestPipeline(3, func(string) bool { return false })
	author := strings.Repeat("a", 64)
	for i := 0; i < 3; i++ {
		if p.overPostCap(capTestEvent(author, 1)) {
			t.Fatalf("event %d should be under the cap", i+1)
		}
	}
	if !p.overPostCap(capTestEvent(author, 1)) {
		t.Fatal("4th event should be dropped by a cap of 3")
	}
	if p.cap.droppedSinceLog != 1 || p.cap.cappedAuthorsToday != 1 {
		t.Fatalf("summary counters = (%d dropped, %d authors), want (1, 1)",
			p.cap.droppedSinceLog, p.cap.cappedAuthorsToday)
	}
}

func TestPostCapAppliesOnlyToCappedKinds(t *testing.T) {
	p := capTestPipeline(1, func(string) bool { return false })
	author := strings.Repeat("b", 64)
	// Profiles (0), contact lists (3), DMs (1059) etc. are never capped.
	for _, kind := range []int{0, 3, 7, 1059, 30078} {
		for i := 0; i < 5; i++ {
			if p.overPostCap(capTestEvent(author, kind)) {
				t.Fatalf("kind %d must not be capped", kind)
			}
		}
	}
	// Posts (1, 1111) and reposts (6, 16) are.
	for _, kind := range []int{1, 1111, 6, 16} {
		p.overPostCap(capTestEvent(author, kind))
	}
	if !p.overPostCap(capTestEvent(author, 1)) {
		t.Fatal("capped kinds share one per-author counter")
	}
}

func TestPostCapExemptAuthorsAreNeverCappedOrCounted(t *testing.T) {
	exemptAuthor := strings.Repeat("c", 64)
	p := capTestPipeline(1, func(pubkey string) bool { return pubkey == exemptAuthor })
	for i := 0; i < 100; i++ {
		if p.overPostCap(capTestEvent(exemptAuthor, 1)) {
			t.Fatal("exempt author must never be capped")
		}
	}
	if len(p.cap.counts) != 0 {
		t.Fatalf("exempt authors must not occupy counter entries; got %d", len(p.cap.counts))
	}
}

func TestPostCapResetsOnUTCDayRollover(t *testing.T) {
	p := capTestPipeline(1, func(string) bool { return false })
	author := strings.Repeat("d", 64)
	now := time.Date(2026, 7, 4, 23, 59, 0, 0, time.UTC)
	p.cap.now = func() time.Time { return now }

	p.overPostCap(capTestEvent(author, 1))
	if !p.overPostCap(capTestEvent(author, 1)) {
		t.Fatal("2nd event of the day should be capped at 1/day")
	}
	now = now.Add(2 * time.Minute) // crosses into the next UTC day
	if p.overPostCap(capTestEvent(author, 1)) {
		t.Fatal("counter must reset on UTC day rollover")
	}
	// The rollover reset the author counters; this first post of the new day
	// itself reaches the cap of 1, so the daily author count is 1 (not 2).
	if p.cap.cappedAuthorsToday != 1 {
		t.Fatalf("cappedAuthorsToday after rollover = %d, want 1", p.cap.cappedAuthorsToday)
	}
}

func TestPostCapDisabledAndNoExemptionSource(t *testing.T) {
	// Cap 0 disables entirely.
	p := New(nil, Config{BatchSize: 10, PostCapPerDay: 0})
	for i := 0; i < 50; i++ {
		if p.overPostCap(capTestEvent(strings.Repeat("e", 64), 1)) {
			t.Fatal("cap 0 must disable capping")
		}
	}
	// No exemption source (nil) = cap applies to everyone.
	p = capTestPipeline(1, nil)
	author := strings.Repeat("f", 64)
	p.overPostCap(capTestEvent(author, 1))
	if !p.overPostCap(capTestEvent(author, 1)) {
		t.Fatal("nil exemption source should still cap")
	}
}

func TestPostCapFailsOpenWhenCounterFull(t *testing.T) {
	p := capTestPipeline(1, func(string) bool { return false })
	// Pre-fill the counter to the bound with distinct tracked authors.
	day := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC).Format("2006-01-02")
	p.cap.day = day
	p.cap.counts = make(map[string]int, postCapCounterMaxEntries)
	for i := 0; i < postCapCounterMaxEntries; i++ {
		p.cap.counts[time.Unix(int64(i), 0).String()] = 1
	}
	newAuthor := strings.Repeat("9", 64)
	for i := 0; i < 10; i++ {
		if p.overPostCap(capTestEvent(newAuthor, 1)) {
			t.Fatal("a full counter map must fail OPEN (never drop untracked authors)")
		}
	}
}
