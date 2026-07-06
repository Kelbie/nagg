package appview

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/routstr"
)

var aiNow = time.Unix(1_751_000_000, 0) // fixed "now" for deterministic freshness

func chatModel(id, slug string, created int64, prompt, completion float64) routstr.Model {
	return routstr.Model{
		ID:               id,
		Name:             id,
		Created:          created,
		ContextLength:    200_000,
		CanonicalSlug:    slug,
		Enabled:          true,
		InputModalities:  []string{"text"},
		OutputModalities: []string{"text"},
		Pricing: routstr.Pricing{
			Prompt:            prompt,
			Completion:        completion,
			Request:           0.001,
			MaxCost:           1000,
			MaxCompletionCost: completion * 128_000,
		},
	}
}

func testCatalog() []routstr.Model {
	fresh := aiNow.Add(-30 * 24 * time.Hour).Unix()
	return []routstr.Model{
		chatModel("claude-haiku", "anthropic/claude-haiku", fresh, 0.0002, 0.001),
		chatModel("claude-sonnet", "anthropic/claude-sonnet", fresh, 0.002, 0.01),
		chatModel("claude-opus", "anthropic/claude-opus", fresh, 0.01, 0.05),
		chatModel("gpt-mini", "openai/gpt-mini", fresh, 0.0001, 0.0004),
		chatModel("gpt-pro", "openai/gpt-pro", fresh, 0.02, 0.08),
		chatModel("gpt-mid", "openai/gpt-mid", fresh, 0.002, 0.008),
	}
}

func TestBuildAILineupTiersByTurnCost(t *testing.T) {
	resp := buildAILineup(testCatalog(), "https://api.routstr.com/", []string{"openai", "anthropic"}, nil, aiNow)

	if resp.Node.BaseURL != "https://api.routstr.com" {
		t.Fatalf("node base url = %q", resp.Node.BaseURL)
	}
	if len(resp.Providers) != 2 {
		t.Fatalf("providers = %d, want 2", len(resp.Providers))
	}
	if resp.Providers[0].ID != "openai" || resp.Providers[1].ID != "claude" {
		t.Fatalf("provider ids = %s,%s", resp.Providers[0].ID, resp.Providers[1].ID)
	}
	got := map[string]string{}
	for _, m := range resp.Providers[0].Models {
		got[m.Tier] = m.ID
	}
	want := map[string]string{"auto": "gpt-mini", "pro": "gpt-mid", "max": "gpt-pro"}
	for tier, id := range want {
		if got[tier] != id {
			t.Fatalf("openai %s = %q, want %q (all: %v)", tier, got[tier], id, got)
		}
	}
}

func TestBuildAILineupExcludesNonChatAndStale(t *testing.T) {
	fresh := aiNow.Add(-30 * 24 * time.Hour).Unix()
	stale := aiNow.Add(-3 * 365 * 24 * time.Hour).Unix()

	embedding := chatModel("text-embed", "openai/text-embed", fresh, 0.0001, 0)
	embedding.Pricing.Completion = 0 // embeddings bill prompt-only

	imageGen := chatModel("img-gen", "openai/img-gen", fresh, 0.001, 0.004)
	imageGen.OutputModalities = []string{"image"}

	disabled := chatModel("gpt-off", "openai/gpt-off", fresh, 0.0001, 0.0004)
	disabled.Enabled = false

	alias := chatModel("rolling", "~openai/rolling", fresh, 0.0001, 0.0004)

	tiny := chatModel("gpt-tiny-ctx", "openai/gpt-tiny-ctx", fresh, 0.0001, 0.0004)
	tiny.ContextLength = 8192

	relic := chatModel("o1-relic", "openai/o1-relic", stale, 0.5, 2.0)

	catalog := append(testCatalog(), embedding, imageGen, disabled, alias, tiny, relic)
	resp := buildAILineup(catalog, "https://api.routstr.com", []string{"openai"}, nil, aiNow)

	if len(resp.Providers) != 1 {
		t.Fatalf("providers = %d, want 1", len(resp.Providers))
	}
	for _, m := range resp.Providers[0].Models {
		switch m.ID {
		case "text-embed", "img-gen", "gpt-off", "rolling", "gpt-tiny-ctx", "o1-relic":
			t.Fatalf("excluded model %q appeared in lineup (tier %s)", m.ID, m.Tier)
		}
	}
	// The stale relic is the priciest model; freshness must keep gpt-pro as max.
	for _, m := range resp.Providers[0].Models {
		if m.Tier == "max" && m.ID != "gpt-pro" {
			t.Fatalf("max = %q, want gpt-pro", m.ID)
		}
	}
}

func TestBuildAILineupPinsOverrideDerived(t *testing.T) {
	pins := map[string]map[string]string{
		"anthropic": {"max": "claude-haiku", "pro": "no-such-model"},
	}
	resp := buildAILineup(testCatalog(), "https://api.routstr.com", []string{"anthropic"}, pins, aiNow)

	got := map[string]string{}
	for _, m := range resp.Providers[0].Models {
		got[m.Tier] = m.ID
	}
	if got["max"] != "claude-haiku" {
		t.Fatalf("pinned max = %q, want claude-haiku", got["max"])
	}
	// A pin naming a model absent from the catalog degrades to the derived pick.
	if got["pro"] != "claude-sonnet" {
		t.Fatalf("pro = %q, want derived claude-sonnet", got["pro"])
	}
}

func TestBuildAILineupDegradesWithFewModels(t *testing.T) {
	fresh := aiNow.Add(-30 * 24 * time.Hour).Unix()
	catalog := []routstr.Model{
		chatModel("grok-a", "x-ai/grok-a", fresh, 0.001, 0.004),
		chatModel("grok-b", "x-ai/grok-b", fresh, 0.01, 0.04),
	}
	resp := buildAILineup(catalog, "https://api.routstr.com", []string{"x-ai", "google"}, nil, aiNow)

	// google has no models → tab omitted entirely, not emitted empty.
	if len(resp.Providers) != 1 {
		t.Fatalf("providers = %d, want 1", len(resp.Providers))
	}
	p := resp.Providers[0]
	if p.ID != "grok" {
		t.Fatalf("provider id = %q, want grok", p.ID)
	}
	got := map[string]string{}
	for _, m := range p.Models {
		got[m.Tier] = m.ID
	}
	if got["auto"] != "grok-a" || got["max"] != "grok-b" || got["pro"] != "" {
		t.Fatalf("two-model degrade = %v, want auto=grok-a max=grok-b no pro", got)
	}
}

type stubRoutstr struct {
	models []routstr.Model
	err    error
}

func (s stubRoutstr) Models(context.Context) ([]routstr.Model, error) { return s.models, s.err }
func (s stubRoutstr) BaseURL() string                                 { return "https://api.routstr.com" }

func TestAILineupRoute(t *testing.T) {
	h := New(nil, WithAILineup(stubRoutstr{models: testCatalog()}, []string{"anthropic"}, nil))

	rec := httptest.NewRecorder()
	h.aiLineup(rec, httptest.NewRequest(http.MethodGet, "/app/ai-lineup", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	var resp AILineupResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Version != 1 || len(resp.Providers) != 1 || resp.Providers[0].ID != "claude" {
		t.Fatalf("unexpected response: %+v", resp)
	}

	// Upstream error → 502 so the app falls back to its client-side derive.
	h = New(nil, WithAILineup(stubRoutstr{err: errors.New("down")}, []string{"anthropic"}, nil))
	rec = httptest.NewRecorder()
	h.aiLineup(rec, httptest.NewRequest(http.MethodGet, "/app/ai-lineup", nil))
	if rec.Code != http.StatusBadGateway {
		t.Fatalf("upstream-down status = %d, want 502", rec.Code)
	}

	// Not configured → 503.
	h = New(nil)
	rec = httptest.NewRecorder()
	h.aiLineup(rec, httptest.NewRequest(http.MethodGet, "/app/ai-lineup", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured status = %d, want 503", rec.Code)
	}
}

func TestParseAILineupPins(t *testing.T) {
	if pins := ParseAILineupPins(""); pins != nil {
		t.Fatalf("empty pins = %v", pins)
	}
	if pins := ParseAILineupPins("{broken"); pins != nil {
		t.Fatalf("invalid pins = %v, want nil", pins)
	}
	pins := ParseAILineupPins(`{"anthropic":{"max":"claude-opus"}}`)
	if pins["anthropic"]["max"] != "claude-opus" {
		t.Fatalf("pins = %v", pins)
	}
}
