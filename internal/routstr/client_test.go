package routstr

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Synthetic catalog IDs deliberately do not imply availability on a live node.
const fixture = `{"data":[
 {"id":"fixture-chat","name":"Fixture Chat","created":1751000000,"context_length":200000,"canonical_slug":"anthropic/fixture-chat","enabled":true,
 "architecture":{"input_modalities":["text","image"],"output_modalities":["text"]},"top_provider":{"max_completion_tokens":8192},
 "sats_pricing":{"prompt":0.001,"completion":0.005,"request":0.1,"max_cost":50,"max_prompt_cost":10,"max_completion_cost":40}},
 {"id":"openai/fixture-default","sats_pricing":{"completion":0.01}},
 {"id":"disabled","enabled":false,"sats_pricing":{"prompt":0.1}},
 {"id":"image","architecture":{"output_modalities":["image"]},"sats_pricing":{"request":1}},
 {"id":"unpriced"},{"id":"","sats_pricing":{}},
 {"id":"~openai","sats_pricing":{}},
 {"id":"rolling","canonical_slug":"~anthropic/rolling","sats_pricing":{}}
]}`

func TestRoutstrCatalogParsingAndFreshTTL(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/v1/models" || r.Method != http.MethodGet {
			t.Errorf("request = %s %s", r.Method, r.URL.Path)
		}
		fmt.Fprint(w, fixture)
	}))
	defer server.Close()
	client := NewHTTPClient(server.URL + "/")
	first, err := client.Catalog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Models) != 4 {
		t.Fatalf("models = %+v", first.Models)
	}
	m := first.Models[0]
	if m.ID != "fixture-chat" || m.Name != "Fixture Chat" || m.Created != 1751000000 || m.ContextLength != 200000 || m.CanonicalSlug != "anthropic/fixture-chat" || m.Vendor() != "anthropic" || !m.Enabled || m.MaxCompletionTokens != 8192 {
		t.Fatalf("model = %+v", m)
	}
	if !reflect.DeepEqual(m.InputModalities, []string{"text", "image"}) || !reflect.DeepEqual(m.OutputModalities, []string{"text"}) {
		t.Fatalf("modalities = %+v", m)
	}
	if m.Pricing != (Pricing{Prompt: .001, Completion: .005, Request: .1, MaxCost: 50, MaxPromptCost: 10, MaxCompletionCost: 40}) {
		t.Fatalf("pricing = %+v", m.Pricing)
	}
	if !first.Models[1].Enabled || first.Models[1].Vendor() != "openai" || first.Models[2].Enabled {
		t.Fatal("enabled default/override or vendor fallback lost")
	}
	if first.UpdatedAt.IsZero() || first.BaseURL != server.URL || first.FallbackUsed || client.ActiveBaseURL() != server.URL {
		t.Fatalf("snapshot = %+v", first)
	}
	second, err := client.Catalog(context.Background())
	if err != nil || requests.Load() != 1 || !first.UpdatedAt.Equal(second.UpdatedAt) {
		t.Fatalf("fresh request: count=%d err=%v", requests.Load(), err)
	}
	client.mu.Lock()
	client.cached.UpdatedAt = time.Now().Add(-16 * time.Minute)
	client.mu.Unlock()
	if _, err := client.Models(context.Background()); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 2 {
		t.Fatalf("expired TTL requests = %d", requests.Load())
	}
}

func TestRoutstrFailoverRecoveryAndStale(t *testing.T) {
	var logs bytes.Buffer
	old := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(old) })
	var primaryOK, fallbackOK atomic.Bool
	fallbackOK.Store(true)
	var primaryCalls, fallbackCalls atomic.Int32
	node := func(ok *atomic.Bool, calls *atomic.Int32) *httptest.Server {
		return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			if !ok.Load() {
				http.NotFound(w, r)
				return
			}
			fmt.Fprint(w, fixture)
		}))
	}
	primary := node(&primaryOK, &primaryCalls)
	defer primary.Close()
	fallback := node(&fallbackOK, &fallbackCalls)
	defer fallback.Close()
	c := NewHTTPClient(primary.URL, WithFallbackURLs([]string{fallback.URL}))
	first, err := c.Catalog(context.Background())
	if err != nil || first.BaseURL != fallback.URL || !first.FallbackUsed || c.ActiveBaseURL() != fallback.URL {
		t.Fatalf("fallback = %+v, %v", first, err)
	}
	for range 5 {
		if _, err := c.Catalog(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if strings.Count(logs.String(), "routstr.node.switched") != 1 || primaryCalls.Load() != 1 || fallbackCalls.Load() != 1 {
		t.Fatalf("fresh requests/logs: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "from="+primary.URL) || !strings.Contains(logs.String(), "to="+fallback.URL) {
		t.Fatalf("switch attributes: %s", logs.String())
	}
	c.ttl = 0 // deterministic forced refresh, no wall-clock sleeps
	if _, err := c.Catalog(context.Background()); err != nil {
		t.Fatal(err)
	}
	if strings.Count(logs.String(), "routstr.node.switched") != 1 {
		t.Fatal("unchanged active node logged again")
	}
	primaryOK.Store(true)
	recovered, err := c.Catalog(context.Background())
	if err != nil || recovered.BaseURL != primary.URL || recovered.FallbackUsed || !recovered.UpdatedAt.After(first.UpdatedAt) || c.ActiveBaseURL() != primary.URL {
		t.Fatalf("recovery = %+v, %v", recovered, err)
	}
	if strings.Count(logs.String(), "routstr.node.switched") != 2 {
		t.Fatalf("logs: %s", logs.String())
	}
	primaryOK.Store(false)
	fallbackOK.Store(false)
	// The addendum requires retaining even a catalog older than the former 24h cutoff.
	c.cached.UpdatedAt = time.Now().Add(-48 * time.Hour)
	stale, err := c.Catalog(context.Background())
	if err != nil || !reflect.DeepEqual(stale, c.cached) || stale.BaseURL != primary.URL || len(stale.Models) == 0 {
		t.Fatalf("stale = %+v, %v", stale, err)
	}
	if strings.Count(logs.String(), "routstr.node.switched") != 2 {
		t.Fatal("failed refresh logged a switch")
	}
	cold := NewHTTPClient(primary.URL, WithFallbackURLs([]string{fallback.URL}))
	if _, err := cold.Catalog(context.Background()); err == nil {
		t.Fatal("cold outage must fail")
	}
}

func TestRoutstrSkipsInvalidCatalogsInOrder(t *testing.T) {
	for _, body := range []string{`broken`, `{}`, `{"data":[]}`, `{"data":[{"id":"disabled","enabled":false,"sats_pricing":{}}]}`, `{"data":[{"id":"unpriced"}]}`, `{"data":[{"id":"~alias","sats_pricing":{}}]}`} {
		t.Run(body, func(t *testing.T) {
			primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, body) }))
			defer primary.Close()
			fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, fixture) }))
			defer fallback.Close()
			unused := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { t.Error("probed beyond first valid node") }))
			defer unused.Close()
			c := NewHTTPClient(primary.URL, WithFallbackURLs([]string{fallback.URL, unused.URL}))
			got, err := c.Catalog(context.Background())
			if err != nil || got.BaseURL != fallback.URL {
				t.Fatalf("snapshot = %+v, %v", got, err)
			}
		})
	}
}

func TestRoutstrConcurrentColdRequestsShareRefresh(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { requests.Add(1); fmt.Fprint(w, fixture) }))
	defer server.Close()
	c := NewHTTPClient(server.URL)
	var wg sync.WaitGroup
	for range 20 {
		wg.Go(func() {
			if _, err := c.Catalog(context.Background()); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if requests.Load() != 1 {
		t.Fatalf("requests = %d", requests.Load())
	}
}

func TestRoutstrSlowPrimaryLeavesBudgetForFallback(t *testing.T) {
	primary := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done() }))
	defer primary.Close()
	fallback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, fixture) }))
	defer fallback.Close()
	c := NewHTTPClient(primary.URL, WithFallbackURLs([]string{fallback.URL}))
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	got, err := c.Catalog(ctx)
	if err != nil || got.BaseURL != fallback.URL {
		t.Fatalf("fallback starved: %+v, %v", got, err)
	}
}
