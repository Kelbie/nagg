package config

import (
	"slices"
	"testing"
	"time"
)

func TestRatesConfigModuleDefaultsAndOverrides(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_RATES_ENABLED", "")
	t.Setenv("NAGG_RATES_INTERVAL", "")
	t.Setenv("NAGG_RATES_MAX_AGE", "")
	t.Setenv("NAGG_RATES_STALE_FOR", "")
	t.Setenv("NAGG_RATES_RELAYS", "")
	t.Setenv("NAGG_RATES_HTTP_ENABLED", "")
	t.Setenv("NAGG_RATES_EXTRA_SOURCES", "")
	for _, module := range []string{"mint", "mint,app", ""} {
		t.Setenv("NAGG_MODULES", module)
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		r := cfg.Rates
		if r.Enabled != (module != "mint") || !r.HTTPEnabled || r.Interval != time.Hour || r.MaxAge != 6*time.Hour || r.StaleFor != 24*time.Hour || !slices.Equal(r.Relays, cfg.Firehose.Relays) || len(r.Sources) != 7 {
			t.Fatalf("%q config %+v", module, r)
		}
	}
	t.Setenv("NAGG_MODULES", "mint")
	t.Setenv("NAGG_RATES_ENABLED", "true")
	t.Setenv("NAGG_RATES_HTTP_ENABLED", "false")
	t.Setenv("NAGG_RATES_INTERVAL", "2h")
	t.Setenv("NAGG_RATES_RELAYS", "wss://example.com")
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Rates.Enabled || cfg.Rates.HTTPEnabled || len(cfg.Rates.Sources) != 3 || cfg.Rates.Interval != 2*time.Hour || !slices.Equal(cfg.Rates.Relays, []string{"wss://example.com"}) {
		t.Fatalf("override %+v", cfg.Rates)
	}
	for _, key := range []string{"NAGG_RATES_INTERVAL", "NAGG_RATES_MAX_AGE", "NAGG_RATES_STALE_FOR"} {
		t.Run(key, func(t *testing.T) {
			t.Setenv(key, "invalid")
			if _, err := Load(); err == nil {
				t.Fatal("accepted invalid duration")
			}
		})
	}
}
