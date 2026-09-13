package appview

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/cache"
	"github.com/vertex-lab/nagg/internal/capabilities"
	"github.com/vertex-lab/nagg/internal/rates"
)

type ratesHTTPFake struct{}

func (ratesHTTPFake) Fetch(context.Context, string) ([]byte, error) {
	return []byte(`{"GBP":57274}`), nil
}

func TestRatesRouteWarmShapeAndCache(t *testing.T) {
	s := rates.NewService(rates.Config{Sources: []rates.Source{{ID: "mempool", Currency: "GBP", Kind: rates.HTTPJSON, JSONKey: "GBP"}}}, nil, ratesHTTPFake{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	h := New(nil, WithRates(s), WithModules(mustParseModules(t, "mint,app")), WithResponseCache(cache.NewMemory(1<<20), time.Second, time.Second))
	mux := http.NewServeMux()
	h.Register(mux)
	for _, path := range []string{"/app/rates", "/v1/app/rates"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != 503 || rec.Body.String() != "{\"error\":\"rates warming\"}\n" {
			t.Fatalf("cold %s: %d %s", path, rec.Code, rec.Body.String())
		}
	}
	s.RunOnce(context.Background())
	for _, path := range []string{"/app/rates", "/v1/app/rates"} {
		for i := 0; i < 2; i++ {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != 200 || rec.Header().Get("Cache-Control") != "public, max-age=60" || rec.Header().Get("X-Nagg-Cache") != "" {
				t.Fatalf("%s read %d: %d %v", path, i, rec.Code, rec.Header())
			}
			var snap rates.Snapshot
			if err := json.Unmarshal(rec.Body.Bytes(), &snap); err != nil {
				t.Fatal(err)
			}
			r := snap.Rates["GBP"]
			if snap.Version != 1 || snap.Base != "BTC" || snap.UpdatedAt <= 0 || !snap.Degraded || r.Price != 57274 || r.At <= 0 || r.Samples != 1 || r.Confidence != "single-source" || !slices.Equal(r.Sources, []string{"mempool"}) {
				t.Fatalf("snapshot %+v", snap)
			}
			if len(snap.Sources) != 1 || !snap.Sources[0].OK || snap.Sources[0].LastSuccessAt == nil || snap.Sources[0].LastErrorAt != nil || snap.Sources[0].LastError != "" {
				t.Fatalf("sources %+v", snap.Sources)
			}
		}
	}
	if !slices.Contains(capabilities.Names, "app.rates") {
		t.Fatal("missing capability")
	}
}

type expiringRatesProvider struct{ available bool }

func (p *expiringRatesProvider) Snapshot() (rates.Snapshot, bool) {
	return rates.Snapshot{Rates: map[string]rates.Rate{"GBP": {Price: 57274}}}, p.available
}

func TestRatesExpiryCannotBeExtendedByResponseCache(t *testing.T) {
	for _, path := range []string{"/app/rates", "/v1/app/rates"} {
		t.Run(path, func(t *testing.T) {
			provider := &expiringRatesProvider{available: true}
			mux := http.NewServeMux()
			New(nil, WithRates(provider), WithModules(mustParseModules(t, "app")), WithResponseCache(cache.NewMemory(1<<20), time.Minute, 24*time.Hour)).Register(mux)
			warm := httptest.NewRecorder()
			mux.ServeHTTP(warm, httptest.NewRequest(http.MethodGet, path, nil))
			if warm.Code != http.StatusOK {
				t.Fatalf("warm status = %d", warm.Code)
			}
			provider.available = false
			expired := httptest.NewRecorder()
			mux.ServeHTTP(expired, httptest.NewRequest(http.MethodGet, path, nil))
			if expired.Code != http.StatusServiceUnavailable {
				t.Fatalf("expired rate served from cache: %d %s", expired.Code, expired.Body)
			}
		})
	}
}

func TestRatesRouteDisabledAndMethods(t *testing.T) {
	h := New(nil)
	for _, method := range []string{http.MethodGet, http.MethodPost, http.MethodPut} {
		rec := httptest.NewRecorder()
		h.appRates(rec, httptest.NewRequest(method, "/app/rates", nil))
		if method == http.MethodGet {
			if rec.Code != 503 {
				t.Fatalf("disabled status=%d", rec.Code)
			}
		} else if rec.Code != 405 || rec.Header().Get("Allow") != "GET" {
			t.Fatalf("method status=%d", rec.Code)
		}
	}
	mux := http.NewServeMux()
	New(nil, WithModules(mustParseModules(t, "mint"))).Register(mux)
	if _, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, "/app/rates", nil)); pattern != "" {
		t.Fatal("rates mounted without app")
	}
}
