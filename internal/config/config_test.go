package config

import (
	"strings"
	"testing"
)

func TestLoadDefaultRelaysExcludeExternalCacheHost(t *testing.T) {
	t.Setenv("NAGG_RELAYS", "")
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Firehose.Relays) == 0 {
		t.Fatal("expected default relays")
	}
	if cfg.OnDemand.UserFeed {
		t.Fatal("on-demand user feed backfill should be opt-in by default")
	}
	if cfg.OnDemand.Wait != 0 {
		t.Fatalf("on-demand wait = %s, want instant default", cfg.OnDemand.Wait)
	}
	if cfg.ClickHouse.MaxOpenConns != 30 || cfg.ClickHouse.MaxIdleConns != 10 {
		t.Fatalf("clickhouse pool = open %d idle %d", cfg.ClickHouse.MaxOpenConns, cfg.ClickHouse.MaxIdleConns)
	}
	if !containsKind(cfg.Firehose.Kinds, 38000) {
		t.Fatalf("default kinds = %v, want mint review kind 38000", cfg.Firehose.Kinds)
	}
	if cfg.Vertex.ProfileMinFollowers != 500 {
		t.Fatalf("profile min followers = %d, want 500", cfg.Vertex.ProfileMinFollowers)
	}
	for _, relay := range cfg.Firehose.Relays {
		if strings.Contains(strings.ToLower(relay), "pri"+"mal") {
			t.Fatalf("default relay set includes external cache host %q", relay)
		}
	}
}

func TestLoadVertexProfileMinFollowersOverride(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_PROFILE_MIN_FOLLOWERS", "750")

	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Vertex.ProfileMinFollowers != 750 {
		t.Fatalf("profile min followers = %d, want 750", cfg.Vertex.ProfileMinFollowers)
	}
}

func TestLoadRejectsNegativeVertexProfileMinFollowers(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	t.Setenv("NAGG_VERTEX_PROFILE_MIN_FOLLOWERS", "-1")

	_, err := Load()
	if err == nil {
		t.Fatal("expected error")
	}
	if !strings.Contains(err.Error(), "NAGG_VERTEX_PROFILE_MIN_FOLLOWERS") {
		t.Fatalf("error = %v", err)
	}
}

func containsKind(kinds []int, want int) bool {
	for _, kind := range kinds {
		if kind == want {
			return true
		}
	}
	return false
}
