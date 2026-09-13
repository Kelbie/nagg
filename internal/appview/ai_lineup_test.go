package appview

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/vertex-lab/nagg/internal/capabilities"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync/atomic"
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
	resp, _ := buildAILineup(testCatalog(), "https://api.routstr.com/", []string{"openai", "anthropic"}, nil, aiNow)

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
	resp, _ := buildAILineup(catalog, "https://api.routstr.com", []string{"openai"}, nil, aiNow)

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
	resp, _ := buildAILineup(testCatalog(), "https://api.routstr.com", []string{"anthropic"}, pins, aiNow)

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
	resp, _ := buildAILineup(catalog, "https://api.routstr.com", []string{"x-ai", "google"}, nil, aiNow)

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

func (s stubRoutstr) Catalog(context.Context) (routstr.Catalog, error) {
	return routstr.Catalog{Models: s.models, BaseURL: "https://api.routstr.com", UpdatedAt: aiNow}, s.err
}

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

func TestAILineupMissingPinsAndAuthMode(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	disabled := chatModel("disabled", "anthropic/disabled", aiNow.Unix(), 1, 1)
	disabled.Enabled = false
	pins := map[string]map[string]string{"anthropic": {"auto": "disabled", "pro": "missing", "max": "claude-haiku"}}
	client := &snapshotRoutstr{catalog: routstr.Catalog{Models: append(testCatalog(), disabled), BaseURL: "https://fixture.invalid", UpdatedAt: aiNow}}
	h := New(nil, WithAILineup(client, []string{"anthropic"}, pins), WithAIAuthMode("bearer"))
	for range 3 {
		rec := httptest.NewRecorder()
		h.aiLineup(rec, httptest.NewRequest(http.MethodGet, "/app/ai-lineup", nil))
		var got AILineupResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if rec.Code != 200 || !reflect.DeepEqual(got.PinsMissing, []string{"anthropic/auto:disabled", "anthropic/pro:missing"}) || got.Node.AuthMode != "bearer" {
			t.Fatalf("response: %s", rec.Body.String())
		}
	}
	if strings.Count(logs.String(), "ai_lineup.pin_missing") != 1 || !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("logs: %s", logs.String())
	}
	client.catalog.UpdatedAt = aiNow.Add(time.Minute)
	h.aiLineup(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/app/ai-lineup", nil))
	if strings.Count(logs.String(), "ai_lineup.pin_missing") != 2 {
		t.Fatalf("refresh logs: %s", logs.String())
	}
	h = New(nil, WithAILineup(client, []string{"anthropic"}, nil))
	rec := httptest.NewRecorder()
	h.aiLineup(rec, httptest.NewRequest(http.MethodGet, "/app/ai-lineup", nil))
	if strings.Contains(rec.Body.String(), "authMode") || !strings.Contains(rec.Body.String(), `"pinsMissing":[]`) {
		t.Fatalf("optional/empty fields: %s", rec.Body.String())
	}
}

type snapshotRoutstr struct{ catalog routstr.Catalog }

func (s *snapshotRoutstr) Catalog(context.Context) (routstr.Catalog, error) { return s.catalog, nil }

func TestAILineupNodeFailover(t *testing.T) {
	var primaryOK, fallbackOK atomic.Bool
	fallbackOK.Store(true)
	node := func(ok *atomic.Bool, id string) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !ok.Load() {
				http.NotFound(w, r)
				return
			}
			fmt.Fprintf(w, `{"data":[{"id":%q,"canonical_slug":"openai/fixture","enabled":true,"context_length":200000,"architecture":{"output_modalities":["text"]},"sats_pricing":{"completion":1,"max_cost":10}}]}`, id)
		}))
	}
	primary := node(&primaryOK, "fixture-primary")
	defer primary.Close()
	fallback := node(&fallbackOK, "fixture-fallback")
	defer fallback.Close()
	c := routstr.NewHTTPClient(primary.URL, routstr.WithFallbackURLs([]string{fallback.URL}), routstr.WithTTL(0))
	h := New(nil, WithAILineup(c, []string{"openai"}, nil), WithAIAuthMode("x-cashu"))
	check := func(baseURL, id string, fallbackUsed bool) {
		t.Helper()
		rec := httptest.NewRecorder()
		h.aiLineup(rec, httptest.NewRequest(http.MethodGet, "/app/ai-lineup", nil))
		var got AILineupResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &got); err != nil {
			t.Fatal(err)
		}
		if rec.Code != 200 || got.Node.BaseURL != baseURL || got.Node.FallbackUsed != fallbackUsed || got.Node.AuthMode != "x-cashu" || len(got.Providers) != 1 || got.Providers[0].Models[0].ID != id {
			t.Fatalf("response: %s", rec.Body.String())
		}
	}
	check(fallback.URL, "fixture-fallback", true)
	fallbackOK.Store(false)
	check(fallback.URL, "fixture-fallback", true)
	primaryOK.Store(true)
	check(primary.URL, "fixture-primary", false)
	primaryOK.Store(false)
	check(primary.URL, "fixture-primary", false)
}

func TestCapabilitiesAILineupPinsMissing(t *testing.T) {
	if !slices.Contains(capabilities.Names, "app.aiLineup.pinsMissing") {
		t.Fatal("missing pinsMissing capability")
	}
}

func TestQualifiesForAILineupRejectsBatchVariants(t *testing.T) {
	base := routstr.Model{ID: "openai/gpt-5.2:batch", Enabled: true, ContextLength: 200000, OutputModalities: []string{"text"}}
	base.Pricing.Completion = 0.001
	base.Pricing.MaxCost = 1
	if qualifiesForAILineup(base) {
		t.Fatal("batch variant must not qualify")
	}
	base.ID = "openai/gpt-5.2"
	if !qualifiesForAILineup(base) {
		t.Fatal("interactive variant must qualify")
	}
}
