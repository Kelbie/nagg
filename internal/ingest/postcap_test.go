package ingest

import (
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"

	"github.com/vertex-lab/nagg/internal/rules"
)

func capTestPipeline(capMax int, exempt func(string) bool) *Pipeline {
	reg := rules.MustDefault(capMax)
	p := New(nil, Config{BatchSize: 10, Caps: reg.Caps()}, WithExemption(exempt))
	base := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	p.capNow = func() time.Time { return base }
	return p
}

func capTestEvent(pubkey string, kind int) *nostr.Event {
	return &nostr.Event{PubKey: pubkey, Kind: kind}
}

func TestCapDropsBeyondWindowLimit(t *testing.T) {
	p := capTestPipeline(3, func(string) bool { return false })
	author := strings.Repeat("a", 64)
	for i := 0; i < 3; i++ {
		if p.overCap(capTestEvent(author, 1)) {
			t.Fatalf("event %d should be under the cap", i+1)
		}
	}
	if !p.overCap(capTestEvent(author, 1)) {
		t.Fatal("4th event should be dropped by a cap of 3")
	}
	if p.caps[0].droppedSinceLog != 1 || p.caps[0].cappedAuthorsBucket != 1 {
		t.Fatalf("summary counters = (%d dropped, %d authors), want (1, 1)",
			p.caps[0].droppedSinceLog, p.caps[0].cappedAuthorsBucket)
	}
}

func TestCapAppliesOnlyToRuleKinds(t *testing.T) {
	p := capTestPipeline(1, func(string) bool { return false })
	author := strings.Repeat("b", 64)
	// Kinds outside the rule (profiles, contacts, reactions, DMs, app data)
	// are never capped.
	for _, kind := range []int{0, 3, 7, 1059, 30078} {
		for i := 0; i < 5; i++ {
			if p.overCap(capTestEvent(author, kind)) {
				t.Fatalf("kind %d must not be capped", kind)
			}
		}
	}
	// The rule's kinds share one per-author counter.
	for _, kind := range []int{1, 1111, 6, 16} {
		p.overCap(capTestEvent(author, kind))
	}
	if !p.overCap(capTestEvent(author, 1)) {
		t.Fatal("rule kinds share one per-author counter")
	}
}

func TestCapExemptAuthorsAreNeverCappedOrCounted(t *testing.T) {
	exemptAuthor := strings.Repeat("c", 64)
	p := capTestPipeline(1, func(pubkey string) bool { return pubkey == exemptAuthor })
	for i := 0; i < 100; i++ {
		if p.overCap(capTestEvent(exemptAuthor, 1)) {
			t.Fatal("exempt author must never be capped")
		}
	}
	if len(p.caps[0].counts) != 0 {
		t.Fatalf("exempt authors must not occupy counter entries; got %d", len(p.caps[0].counts))
	}
}

func TestCapResetsOnWindowRollover(t *testing.T) {
	p := capTestPipeline(1, func(string) bool { return false })
	author := strings.Repeat("d", 64)
	now := time.Date(2026, 7, 4, 23, 59, 0, 0, time.UTC)
	p.capNow = func() time.Time { return now }

	p.overCap(capTestEvent(author, 1))
	if !p.overCap(capTestEvent(author, 1)) {
		t.Fatal("2nd event of the window should be capped at 1")
	}
	now = now.Add(2 * time.Minute) // crosses into the next 24h bucket (UTC day)
	if p.overCap(capTestEvent(author, 1)) {
		t.Fatal("counter must reset on window rollover")
	}
	// The rollover reset the author counters; this first event of the new
	// window itself reaches the cap of 1, so the bucket author count is 1.
	if p.caps[0].cappedAuthorsBucket != 1 {
		t.Fatalf("cappedAuthorsBucket after rollover = %d, want 1", p.caps[0].cappedAuthorsBucket)
	}
}

func TestCapDisabledAndNoExemptionSource(t *testing.T) {
	// capMax 0 declares no cap rule at all.
	p := New(nil, Config{BatchSize: 10, Caps: rules.MustDefault(0).Caps()})
	for i := 0; i < 50; i++ {
		if p.overCap(capTestEvent(strings.Repeat("e", 64), 1)) {
			t.Fatal("no cap rules must disable capping")
		}
	}
	// No exemption source (nil) = cap applies to everyone.
	p = capTestPipeline(1, nil)
	author := strings.Repeat("f", 64)
	p.overCap(capTestEvent(author, 1))
	if !p.overCap(capTestEvent(author, 1)) {
		t.Fatal("nil exemption source should still cap")
	}
}

func TestCapFailsOpenWhenCounterFull(t *testing.T) {
	p := capTestPipeline(1, func(string) bool { return false })
	// Pre-fill the counter to the bound with distinct tracked authors.
	c := p.caps[0]
	c.bucket = c.bucketKey(time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC))
	c.counts = make(map[string]int, capCounterMaxEntries)
	for i := 0; i < capCounterMaxEntries; i++ {
		c.counts[time.Unix(int64(i), 0).String()] = 1
	}
	newAuthor := strings.Repeat("9", 64)
	for i := 0; i < 10; i++ {
		if p.overCap(capTestEvent(newAuthor, 1)) {
			t.Fatal("a full counter map must fail OPEN (never drop untracked authors)")
		}
	}
}

func TestLifetimeCapBucketsOnce(t *testing.T) {
	reg, err := rules.New(nil, nil, nil, nil, []rules.Cap{{Name: "k30078_lifetime", Kinds: []int{30078}, Max: 2}})
	if err != nil {
		t.Fatalf("rules.New: %v", err)
	}
	p := New(nil, Config{BatchSize: 10, Caps: reg.Caps()})
	now := time.Date(2026, 7, 4, 12, 0, 0, 0, time.UTC)
	p.capNow = func() time.Time { return now }

	author := strings.Repeat("g", 64)
	p.overCap(capTestEvent(author, 30078))
	p.overCap(capTestEvent(author, 30078))
	if !p.overCap(capTestEvent(author, 30078)) {
		t.Fatal("3rd event should exceed the lifetime cap of 2")
	}
	// Days later the lifetime bucket must NOT roll over.
	now = now.Add(72 * time.Hour)
	if !p.overCap(capTestEvent(author, 30078)) {
		t.Fatal("lifetime caps must not reset with time")
	}
}
