package config

import (
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr/nip19"
	"github.com/vertex-lab/nagg/internal/btcmap"
	"github.com/vertex-lab/nagg/internal/wallpapers"
)

func TestWallpaperBtcmapConfigDefaultsAndOverrides(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	for _, key := range []string{"NAGG_WALLPAPERS_ENABLED", "NAGG_WALLPAPERS_INTERVAL", "NAGG_WALLPAPERS_ADMIN_PUBKEY", "NAGG_WALLPAPERS_RELAYS", "NAGG_BTCMAP_ENABLED", "NAGG_BTCMAP_URL"} {
		t.Setenv(key, "")
	}
	for _, module := range []string{"mint", "mint,app", "app", ""} {
		t.Setenv("NAGG_MODULES", module)
		cfg, err := Load()
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Wallpapers.Enabled != (module != "mint") || cfg.Btcmap.Enabled != (module != "mint") || cfg.Wallpapers.Interval != time.Hour || cfg.Wallpapers.AdminPubkey != wallpapers.DefaultAdminPubkey || !slices.Equal(cfg.Wallpapers.Relays, cfg.Firehose.Relays) || cfg.Btcmap.URL != btcmap.DefaultURL {
			t.Fatalf("%s defaults %+v %+v", module, cfg.Wallpapers, cfg.Btcmap)
		}
		if module == "mint,app" {
			assertKinds(t, "stored kinds", cfg.StoredKinds, []int{0, 38000})
			assertKinds(t, "firehose kinds", cfg.Firehose.Kinds, []int{38000})
		}
	}
	t.Setenv("NAGG_MODULES", "mint,app")
	t.Setenv("NAGG_WALLPAPERS_ENABLED", "false")
	t.Setenv("NAGG_BTCMAP_ENABLED", "false")
	t.Setenv("NAGG_WALLPAPERS_INTERVAL", "2h")
	t.Setenv("NAGG_WALLPAPERS_RELAYS", "wss://relay.example,wss://relay.example")
	t.Setenv("NAGG_BTCMAP_URL", "http://localhost:9000")
	pubkey, err := nip19.EncodePublicKey(wallpapers.DefaultAdminPubkey)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("NAGG_WALLPAPERS_ADMIN_PUBKEY", pubkey)
	cfg, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Wallpapers.Enabled || cfg.Btcmap.Enabled || cfg.Wallpapers.Interval != 2*time.Hour || cfg.Wallpapers.AdminPubkey != wallpapers.DefaultAdminPubkey || !slices.Equal(cfg.Wallpapers.Relays, []string{"wss://relay.example"}) || cfg.Btcmap.URL != "http://localhost:9000" {
		t.Fatalf("overrides %+v %+v", cfg.Wallpapers, cfg.Btcmap)
	}
	t.Setenv("NAGG_MODULES", "mint")
	t.Setenv("NAGG_WALLPAPERS_ENABLED", "true")
	t.Setenv("NAGG_BTCMAP_ENABLED", "true")
	t.Setenv("NAGG_WALLPAPERS_ADMIN_PUBKEY", strings.ToUpper(wallpapers.DefaultAdminPubkey))
	cfg, err = Load()
	if err != nil || !cfg.Wallpapers.Enabled || !cfg.Btcmap.Enabled || cfg.Wallpapers.AdminPubkey != wallpapers.DefaultAdminPubkey {
		t.Fatalf("explicit enables %v", err)
	}
}

func TestWallpaperBtcmapConfigRejectsInvalid(t *testing.T) {
	t.Setenv("NAGG_VERTEX_PRIVATE_KEY", "")
	for key, values := range map[string][]string{
		"NAGG_WALLPAPERS_INTERVAL":     {"bad", "0s", "-1h"},
		"NAGG_WALLPAPERS_ADMIN_PUBKEY": {"bad", "npub1bad", strings.Repeat("g", 64)},
		"NAGG_BTCMAP_URL":              {"ftp://example.com", "https://user:pass@example.com", "https://example.com?token=secret", "https://example.com#fragment", "bad"},
	} {
		for _, value := range values {
			t.Run(key+"/"+value, func(t *testing.T) {
				t.Setenv(key, value)
				_, err := Load()
				if err == nil || !strings.Contains(err.Error(), key) {
					t.Fatalf("want error for %s, got %v", key, err)
				}
			})
		}
	}
}
