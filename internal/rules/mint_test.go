package rules

import (
	"strings"
	"testing"
)

// The mint rule set's whole point is what it does NOT generate. If a
// relationship ever creeps in, the mint deployment silently starts creating
// aggregate tables and materialized views that fire on every insert.
func TestMintGeneratesNoAggregatesOrEventRefs(t *testing.T) {
	reg, err := Mint()
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	if got := len(reg.Relationships()); got != 0 {
		t.Fatalf("Relationships() = %d, want 0", got)
	}
	if got := len(reg.IngestExtractorRules()); got != 0 {
		t.Fatalf("IngestExtractorRules() = %d, want 0", got)
	}
	ddl := strings.Join(reg.GeneratedDDL(), "\n\n")
	if strings.Contains(ddl, "event_refs") {
		t.Error("mint DDL declares event_refs; InsertEvents would then open a batch per insert for a table nothing reads")
	}
	if strings.Contains(ddl, "agg_") {
		t.Errorf("mint DDL declares an aggregate table:\n%s", ddl)
	}
	if got := len(reg.Caps()); got != 0 {
		t.Fatalf("Caps() = %d, want 0", got)
	}
	if got := len(reg.AddresseeGates()); got != 0 {
		t.Fatalf("AddresseeGates() = %d, want 0", got)
	}
}

// latest_k0 is load-bearing: /nostr/mint/reviews and /nostr/mint/discover bundle
// each reviewer's and operator's profile through Store.LatestK0.
func TestMintKeepsTheKind0Projection(t *testing.T) {
	reg, err := Mint()
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	projections := reg.Projections()
	if len(projections) != 1 || projections[0].Name != "k0" {
		t.Fatalf("Projections() = %v, want exactly k0", projections)
	}
	ddl := strings.Join(reg.GeneratedDDL(), "\n\n")
	for _, want := range []string{"latest_k0", "mv_latest_k0"} {
		if !strings.Contains(ddl, want) {
			t.Errorf("mint DDL missing %q", want)
		}
	}
}

// The k0 projection is shared, not copied: a field added for the full app-view
// must reach the mint deployment's operator profiles too.
func TestMintAndDefaultShareTheK0Projection(t *testing.T) {
	full, err := Default(20)
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	mint, err := Mint()
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	var fullK0 *Projection
	for i, p := range full.Projections() {
		if p.Name == "k0" {
			fullK0 = &full.Projections()[i]
			break
		}
	}
	if fullK0 == nil {
		t.Fatal("Default declares no k0 projection")
	}
	mintK0 := mint.Projections()[0]
	if len(mintK0.Fields) != len(fullK0.Fields) {
		t.Fatalf("k0 fields drifted: mint has %d, default has %d", len(mintK0.Fields), len(fullK0.Fields))
	}
	for i := range mintK0.Fields {
		if mintK0.Fields[i] != fullK0.Fields[i] {
			t.Fatalf("k0 field %d drifted: mint=%+v default=%+v", i, mintK0.Fields[i], fullK0.Fields[i])
		}
	}
}

// A live firehose alone captures almost no kind-38000 (measured 2026-07: 23 live
// events vs ~1.5k already on the relays), so the history walk is the feature.
func TestMintWalksNIP87History(t *testing.T) {
	reg, err := Mint()
	if err != nil {
		t.Fatalf("Mint: %v", err)
	}
	backfills := reg.Backfills()
	if len(backfills) != 1 || backfills[0].Name != "k38000_history" {
		t.Fatalf("Backfills() = %v, want exactly k38000_history", backfills)
	}
	if got := backfills[0].Kinds; len(got) != 1 || got[0] != 38000 {
		t.Fatalf("backfill kinds = %v, want [38000]", got)
	}
}

func TestMustMint(t *testing.T) {
	if reg := MustMint(); reg == nil {
		t.Fatal("MustMint returned nil")
	}
}
