package config

import (
	"slices"
	"testing"
	"time"
)

// TestAIProvidersConfigReachesTheService is a regression test with a scar.
//
// AIProvidersConfig embeds aiproviders.Config, which already declares Relays
// and Seeds. Declaring them a second time on the outer struct compiled, read
// fine, and passed every test that looked at cfg.AIProviders.Seeds — because
// the shallower field wins. What it did was hand NewService(cfg.AIProviders.Config, …)
// an embedded Config whose Relays and Seeds were never assigned: no relays to
// query, no seed nodes to probe, so /app/ai-providers answered "warming"
// forever against a directory that had never been given one node to look at.
//
// This test therefore reads the EMBEDDED Config, the value the service is
// actually constructed from, not the promoted name.
func TestAIProvidersConfigReachesTheService(t *testing.T) {
	for _, key := range []string{
		"NAGG_AI_PROVIDERS_ENABLED", "NAGG_AI_PROVIDERS_RELAYS", "NAGG_AI_PROVIDERS_SEEDS",
		"NAGG_AI_PROVIDERS_INTERVAL", "NAGG_AI_PROVIDERS_MAX_AGE", "NAGG_AI_PROVIDERS_CATALOG_MIN_AGE",
		"NAGG_ROUTSTR_URL", "NAGG_ROUTSTR_FALLBACK_URLS", "NAGG_VERTEX_PRIVATE_KEY",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("NAGG_MODULES", "mint,app")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	inner := cfg.AIProviders.Config
	if !cfg.AIProviders.Enabled {
		t.Fatal("the app module must enable the provider directory by default")
	}
	// Seeds default to the AI nodes nagg is already configured to pay, so
	// discovery still answers with every relay down.
	want := append([]string{cfg.Routstr.URL}, cfg.Routstr.FallbackURLs...)
	if !slices.Equal(inner.Seeds, want) {
		t.Fatalf("seeds = %v, want the Routstr node and its fallbacks %v", inner.Seeds, want)
	}
	if len(inner.Relays) == 0 || !slices.Contains(inner.Relays, "wss://relay.routstr.com") {
		t.Fatalf("relays = %v, want the Routstr announcement relays", inner.Relays)
	}
	if inner.Interval != 5*time.Minute || inner.CatalogMinAge != 30*time.Minute || inner.MaxAge != 2*time.Hour {
		t.Fatalf("durations = %s / %s / %s", inner.Interval, inner.CatalogMinAge, inner.MaxAge)
	}
	if inner.Timeout != 10*time.Second || inner.Concurrency != 6 || inner.MaxProviders != 100 || inner.MaxDirectorySources != 8 {
		t.Fatalf("bounds = %+v", inner)
	}

	// Explicit config wins, and it must land on the embedded Config too.
	t.Setenv("NAGG_AI_PROVIDERS_ENABLED", "false")
	t.Setenv("NAGG_AI_PROVIDERS_SEEDS", "https://seed.example, https://seed.example ,https://other.example")
	t.Setenv("NAGG_AI_PROVIDERS_RELAYS", "wss://relay.example,not-a-relay")
	t.Setenv("NAGG_AI_PROVIDERS_INTERVAL", "15m")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	inner = cfg.AIProviders.Config
	if cfg.AIProviders.Enabled {
		t.Fatal("NAGG_AI_PROVIDERS_ENABLED=false must leave the route 503")
	}
	if !slices.Equal(inner.Seeds, []string{"https://seed.example", "https://other.example"}) {
		t.Fatalf("seeds = %v, want trimmed and deduped", inner.Seeds)
	}
	if !slices.Equal(inner.Relays, []string{"wss://relay.example"}) {
		t.Fatalf("relays = %v, want the invalid entry dropped", inner.Relays)
	}
	if inner.Interval != 15*time.Minute {
		t.Fatalf("interval = %s", inner.Interval)
	}
}

// TestAIProvidersDisabledOutsideTheAppModule: a mint-only deployment must not
// run a worker for a route it does not mount.
func TestAIProvidersDisabledOutsideTheAppModule(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_AI_PROVIDERS_ENABLED", "")
	t.Setenv("NAGG_MODULES", "mint")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.AIProviders.Enabled {
		t.Fatal("a mint-only deployment enabled the provider directory")
	}
}

// TestSocialReachConfigReachesTheService guards the same scar as the test
// above: SocialGraphConfig embeds socialgraph.Config, and anything declared
// twice would hand NewService an empty value while every caller read the
// populated outer copy.
func TestSocialReachConfigReachesTheService(t *testing.T) {
	for _, key := range []string{
		"NAGG_SOCIAL_REACH_ENABLED", "NAGG_SOCIAL_REACH_RELAYS", "NAGG_SOCIAL_REACH_TTL",
		"NAGG_SOCIAL_REACH_INTERVAL", "NAGG_SOCIAL_REACH_CONCURRENCY",
		"NAGG_SOCIAL_REACH_SCAN_TIMEOUT", "NAGG_SOCIAL_REACH_MAX_TRACKED", "NAGG_VERTEX_PRIVATE_KEY",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("NAGG_MODULES", "mint,app,vertex")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.SocialGraph.Enabled {
		t.Fatal("a deployment serving mint discovery or the app surface needs operator reach")
	}
	// The scan reuses the relays nagg already dials; it needs no extra relay
	// set and no credentials.
	if !slices.Equal(cfg.SocialGraph.Relays, cfg.Firehose.Relays) {
		t.Fatalf("relays = %v, want the firehose relay set %v", cfg.SocialGraph.Relays, cfg.Firehose.Relays)
	}
	inner := cfg.SocialGraph.Config
	if inner.TTL != 6*time.Hour || inner.Interval != time.Minute || inner.Concurrency != 4 {
		t.Fatalf("resolver config = %+v", inner)
	}
	if inner.ScanTimeout != 20*time.Second || inner.MaxTracked != 500 {
		t.Fatalf("resolver bounds = %+v", inner)
	}

	t.Setenv("NAGG_SOCIAL_REACH_ENABLED", "false")
	t.Setenv("NAGG_SOCIAL_REACH_TTL", "1h")
	cfg, err = Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.SocialGraph.Enabled || cfg.SocialGraph.Config.TTL != time.Hour {
		t.Fatalf("overrides = %v / %s", cfg.SocialGraph.Enabled, cfg.SocialGraph.Config.TTL)
	}
}
