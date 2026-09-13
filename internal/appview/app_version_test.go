package appview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/cache"
	"github.com/vertex-lab/nagg/internal/capabilities"
)

func TestLatestVersionReturnsConfiguredVersion(t *testing.T) {
	for _, method := range []string{http.MethodGet, http.MethodPost} {
		for _, body := range []string{"", `{"storage":{"version":"1.0.0"}}`, "ignored malformed body"} {
			t.Run(method+"/"+body, func(t *testing.T) {
				handler := New(nil, WithAppVersion(" 1.2.3 ", " Tap to update ", " 1.1.0 "))
				rec := httptest.NewRecorder()
				handler.latestVersion(rec, httptest.NewRequest(method, "/app/latest-version", strings.NewReader(body)))
				if rec.Code != http.StatusOK {
					t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
				}
				var resp LatestVersionResponse
				if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
					t.Fatal(err)
				}
				if resp.Version != "1.2.3" || resp.Message != "Tap to update" || resp.MinVersion != "1.1.0" {
					t.Fatalf("response = %+v", resp)
				}
				if got := rec.Header().Get("Cache-Control"); got != "public, max-age=60" {
					t.Fatalf("Cache-Control = %q", got)
				}
			})
		}
	}
}

func TestLatestVersionOmitsEmptyOptionalFields(t *testing.T) {
	for _, empty := range []string{"", "  "} {
		h := New(nil, WithAppVersion("", empty, empty))
		rec := httptest.NewRecorder()
		h.latestVersion(rec, httptest.NewRequest(http.MethodGet, "/app/latest-version", nil))
		var resp map[string]any
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		if len(resp) != 1 || resp["version"] != "" {
			t.Fatalf("response = %v, want only empty version", resp)
		}
	}
}

func TestLatestVersionRejectsUnsupportedMethods(t *testing.T) {
	h := New(nil)
	for _, method := range []string{http.MethodPut, http.MethodDelete, http.MethodPatch, http.MethodHead} {
		rec := httptest.NewRecorder()
		h.latestVersion(rec, httptest.NewRequest(method, "/app/latest-version", nil))
		if rec.Code != http.StatusMethodNotAllowed || rec.Header().Get("Allow") != "GET, POST" {
			t.Fatalf("%s: status = %d Allow = %q", method, rec.Code, rec.Header().Get("Allow"))
		}
	}
}

func TestLatestVersionMintAppRoutesAndCache(t *testing.T) {
	h := New(nil, WithModules(mustParseModules(t, "mint,app")),
		WithAppVersion("0.1.3", "", "0.1.2"),
		WithResponseCache(cache.NewMemory(1<<20), time.Second, time.Second))
	mux := http.NewServeMux()
	h.Register(mux)
	for _, path := range []string{"/app/latest-version", "/v1/app/latest-version"} {
		for _, state := range []string{"miss", "hit"} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if rec.Code != http.StatusOK || rec.Header().Get("X-Nagg-Cache") != state || rec.Header().Get("Cache-Control") != "public, max-age=60" {
				t.Fatalf("%s %s: status = %d headers = %v", path, state, rec.Code, rec.Header())
			}
			var resp LatestVersionResponse
			if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
				t.Fatal(err)
			}
			if resp.Version != "0.1.3" || resp.MinVersion != "0.1.2" {
				t.Fatalf("response = %+v", resp)
			}
		}
	}
	for _, path := range []string{"/app/ai-lineup", "/v1/app/ai-lineup"} {
		if _, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil)); pattern == "" {
			t.Fatalf("%s is not mounted", path)
		}
	}
	if _, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, "/nostr/feed", nil)); pattern != "" {
		t.Fatal("mint,app mounts social feed")
	}
	if !slices.Contains(capabilities.Names, "app.latestVersion.minVersion") {
		t.Fatal("minVersion capability is missing")
	}
}
