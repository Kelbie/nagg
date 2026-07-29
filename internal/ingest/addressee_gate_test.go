package ingest

import (
	"strings"
	"testing"

	"github.com/nbd-wtf/go-nostr"

	"github.com/vertex-lab/nagg/internal/rules"
)

func gateTestPipeline(exempt func(string) bool) *Pipeline {
	reg := rules.MustDefault(0)
	opts := []Option{}
	if exempt != nil {
		opts = append(opts, WithExemption(exempt))
	}
	return New(nil, Config{BatchSize: 10, AddresseeGates: reg.AddresseeGates()}, opts...)
}

func gateTestWrap(addressees ...string) *nostr.Event {
	tags := nostr.Tags{}
	for _, p := range addressees {
		tags = append(tags, nostr.Tag{"p", p})
	}
	return &nostr.Event{PubKey: strings.Repeat("f", 64), Kind: 1059, Tags: tags}
}

func TestGateDropsWrapsAddressedToStrangers(t *testing.T) {
	viewer := strings.Repeat("a", 64)
	p := gateTestPipeline(func(pubkey string) bool { return pubkey == viewer })

	if !p.unaddressed(gateTestWrap(strings.Repeat("b", 64))) {
		t.Fatal("wrap addressed only to a stranger must be dropped")
	}
	if p.gates[0].droppedSinceLog != 1 {
		t.Fatalf("dropped counter = %d, want 1", p.gates[0].droppedSinceLog)
	}
}

func TestGateKeepsWrapsAddressedToExempt(t *testing.T) {
	viewer := strings.Repeat("a", 64)
	p := gateTestPipeline(func(pubkey string) bool { return pubkey == viewer })

	// Any single exempt addressee keeps the wrap, whatever else is tagged.
	if p.unaddressed(gateTestWrap(strings.Repeat("b", 64), viewer)) {
		t.Fatal("wrap with an exempt addressee must be kept")
	}
}

func TestGateAppliesOnlyToRuleKinds(t *testing.T) {
	p := gateTestPipeline(func(string) bool { return false })
	stranger := strings.Repeat("b", 64)
	for _, kind := range []int{0, 1, 3, 4, 7, 9735} {
		event := gateTestWrap(stranger)
		event.Kind = kind
		if p.unaddressed(event) {
			t.Fatalf("kind %d must not be gated", kind)
		}
	}
}

func TestGateFailsOpenWithoutExemptionSource(t *testing.T) {
	// No exemption source configured (nil exempt): before the relevance
	// tracker exists, dropping events over missing bookkeeping is never
	// acceptable.
	p := gateTestPipeline(nil)
	if p.unaddressed(gateTestWrap(strings.Repeat("b", 64))) {
		t.Fatal("gate must fail open with no exemption source")
	}
}

func TestGateIgnoresMalformedPTags(t *testing.T) {
	p := gateTestPipeline(func(string) bool { return true })
	event := &nostr.Event{PubKey: strings.Repeat("f", 64), Kind: 1059, Tags: nostr.Tags{
		{"p"},                          // no value
		{"p", "short"},                 // not a pubkey
		{"e", strings.Repeat("a", 64)}, // not a p tag
	}}
	if !p.unaddressed(event) {
		t.Fatal("wrap with no well-formed p tag addresses nobody and must be dropped")
	}
}
