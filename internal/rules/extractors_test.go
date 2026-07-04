package rules

import (
	"fmt"
	"testing"

	"github.com/nbd-wtf/go-nostr"
)

const testTarget = "41034a6969340b24898bbe04f991c04ce6c62b38e756b09906fc988807a46109"

func TestZapTargetFromETag(t *testing.T) {
	ev := &nostr.Event{
		Kind: 9735,
		Tags: nostr.Tags{
			{"e", testTarget},
			{"description", fmt.Sprintf(`{"tags":[["amount","21000"],["e","%s"]]}`, testTarget)},
		},
	}
	refs := Extractor("zap_target")(ev)
	if len(refs) != 1 {
		t.Fatalf("refs = %v", refs)
	}
	if refs[0].Target != testTarget {
		t.Errorf("target = %q", refs[0].Target)
	}
	if refs[0].Value != 21 {
		t.Errorf("value = %d, want 21 (21000 msats)", refs[0].Value)
	}
}

func TestZapTargetFromDescription(t *testing.T) {
	ev := &nostr.Event{
		Kind: 9735,
		Tags: nostr.Tags{
			{"description", fmt.Sprintf(`{"tags":[["e","%s"],["amount","5000"]]}`, testTarget)},
		},
	}
	refs := Extractor("zap_target")(ev)
	if len(refs) != 1 || refs[0].Target != testTarget || refs[0].Value != 5 {
		t.Fatalf("refs = %v", refs)
	}
}

func TestZapTargetBolt11Fallback(t *testing.T) {
	ev := &nostr.Event{
		Kind: 9735,
		Tags: nostr.Tags{
			{"e", testTarget},
			{"bolt11", "lnbc210n1pjqqqqq"},
		},
	}
	refs := Extractor("zap_target")(ev)
	if len(refs) != 1 {
		t.Fatalf("refs = %v", refs)
	}
	if refs[0].Value != 21 {
		t.Errorf("value = %d, want 21 (210n = 21 sats)", refs[0].Value)
	}
}

func TestZapTargetNoTarget(t *testing.T) {
	ev := &nostr.Event{Kind: 9735, Tags: nostr.Tags{{"p", testTarget}}}
	if refs := Extractor("zap_target")(ev); refs != nil {
		t.Fatalf("refs = %v, want none", refs)
	}
}

func TestSatsFromBolt11(t *testing.T) {
	cases := []struct {
		invoice string
		want    uint64
	}{
		{"lnbc1pjqqqqq", 0},           // no amount
		{"lnbc21m1pjqqqqq", 2_100_000}, // 21 milli-BTC
		{"lnbc210u1pjqqqqq", 21_000},   // 210 micro-BTC
		{"lnbc210n1pjqqqqq", 21},       // 210 nano-BTC
		{"lnbc2100p1pjqqqqq", 0},       // pico rounds below 1 sat... (2100p = 0.21 sat)
		{"not-an-invoice", 0},
	}
	for _, c := range cases {
		if got := satsFromBolt11(c.invoice); got != c.want {
			t.Errorf("satsFromBolt11(%q) = %d, want %d", c.invoice, got, c.want)
		}
	}
}

func TestUnknownExtractorIsNil(t *testing.T) {
	if Extractor("nope") != nil {
		t.Fatalf("unknown extractor must be nil")
	}
}
