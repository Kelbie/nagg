package config

import (
	"testing"

	"github.com/vertex-lab/nagg/internal/modules"
)

// The production-safety test for this whole change. Merging NAGG_MODULES
// redeploys the live nagg, so an unset NAGG_MODULES must resolve to exactly the
// behavior that shipped before it existed.
func TestUnsetModulesIsTodaysProductionConfig(t *testing.T) {
	t.Setenv("NAGG_MODULES", "")
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}

	if !cfg.Modules.IsAll() {
		t.Fatalf("modules = %s, want every module", cfg.Modules)
	}
	wantKinds := []int{0, 1, 3, 4, 6, 7, 16, 443, 444, 445, 1059, 1063, 9735, 10050, 10051, 30078, 38000}
	assertKinds(t, "stored kinds", cfg.StoredKinds, wantKinds)
	// Before the stored/firehose split there was one list, so they must agree.
	assertKinds(t, "firehose kinds", cfg.Firehose.Kinds, wantKinds)

	if !cfg.RunIngester || !cfg.RunEnricher || !cfg.RunRollup || !cfg.RunMintInfo {
		t.Fatalf("workers must all default on: ingester=%v enricher=%v rollup=%v mintinfo=%v",
			cfg.RunIngester, cfg.RunEnricher, cfg.RunRollup, cfg.RunMintInfo)
	}
	if !cfg.Auditor.Enabled || !cfg.Routstr.Enabled {
		t.Fatalf("upstream clients must all default on: auditor=%v routstr=%v", cfg.Auditor.Enabled, cfg.Routstr.Enabled)
	}
	// The full rule set: six relationships, two projections, the ingest cap.
	if got := len(cfg.ClickHouse.Rules.Relationships()); got == 0 {
		t.Fatal("unset modules must load the full rule registry")
	}
	if got := len(cfg.ClickHouse.Rules.Caps()); got != 1 {
		t.Fatalf("caps = %d, want the default post cap", got)
	}
}

func TestMintModuleDefaults(t *testing.T) {
	t.Setenv("NAGG_MODULES", "mint")
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}

	if cfg.Modules.Has(modules.Nostr) || !cfg.Modules.Has(modules.Mint) {
		t.Fatalf("modules = %s, want mint only", cfg.Modules)
	}

	// The kind split is the point: kind 0 is KEPT (on-demand reviewer and
	// operator profiles) but never SUBSCRIBED (a global kind-0 firehose is
	// hundreds of thousands of events a day).
	assertKinds(t, "stored kinds", cfg.StoredKinds, []int{0, 38000})
	assertKinds(t, "firehose kinds", cfg.Firehose.Kinds, []int{38000})

	if !cfg.RunIngester {
		t.Fatal("the mint module needs the ingester for the kind-38000 slice and its history walk")
	}
	if !cfg.RunMintInfo || !cfg.Auditor.Enabled {
		t.Fatalf("mint module must run the snapshotter and the auditor: mintinfo=%v auditor=%v", cfg.RunMintInfo, cfg.Auditor.Enabled)
	}
	if cfg.RunEnricher || cfg.RunRollup || cfg.Routstr.Enabled {
		t.Fatalf("nostr/app workers must be off: enricher=%v rollup=%v routstr=%v", cfg.RunEnricher, cfg.RunRollup, cfg.Routstr.Enabled)
	}

	// The mint rule set generates no aggregate tables and no event_refs.
	if got := len(cfg.ClickHouse.Rules.Relationships()); got != 0 {
		t.Fatalf("relationships = %d, want 0 in a mint deployment", got)
	}
	if got := len(cfg.ClickHouse.Rules.Projections()); got != 1 {
		t.Fatalf("projections = %d, want only k0", got)
	}
}

// Every module-derived default stays individually overridable — the flag sets
// the default, it does not take the knob away.
func TestExplicitEnvOverridesModuleDefaults(t *testing.T) {
	t.Setenv("NAGG_MODULES", "mint")
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_KINDS", "0,7,38000")
	t.Setenv("NAGG_FIREHOSE_KINDS", "7")
	t.Setenv("NAGG_RUN_ROLLUP", "true")
	t.Setenv("NAGG_AUDITOR_ENABLED", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	assertKinds(t, "stored kinds", cfg.StoredKinds, []int{0, 7, 38000})
	assertKinds(t, "firehose kinds", cfg.Firehose.Kinds, []int{7})
	if !cfg.RunRollup {
		t.Fatal("NAGG_RUN_ROLLUP=true must override the module default")
	}
	if cfg.Auditor.Enabled {
		t.Fatal("NAGG_AUDITOR_ENABLED=false must override the module default")
	}
}

// NAGG_FIREHOSE_KINDS defaults to the stored set, so a deployment that only
// narrows NAGG_KINDS still subscribes to exactly what it keeps.
func TestFirehoseKindsDefaultToStoredKinds(t *testing.T) {
	t.Setenv("NAGG_MODULES", "")
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_KINDS", "1,7")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	assertKinds(t, "firehose kinds", cfg.Firehose.Kinds, []int{1, 7})
}

func TestUnknownModuleFailsLoad(t *testing.T) {
	t.Setenv("NAGG_MODULES", "mint,feed")
	if _, err := Load(); err == nil {
		t.Fatal("Load accepted an unknown module name")
	}
}

func assertKinds(t *testing.T, label string, got, want []int) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("%s = %v, want %v", label, got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("%s = %v, want %v", label, got, want)
		}
	}
}
