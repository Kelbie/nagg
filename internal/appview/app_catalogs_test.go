package appview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/btcmap"
	"github.com/vertex-lab/nagg/internal/cache"
	"github.com/vertex-lab/nagg/internal/capabilities"
)

type wallpaperSnapshotFake struct{ warm bool }

func (f *wallpaperSnapshotFake) Snapshot() (json.RawMessage, bool) {
	return json.RawMessage(`{"wallpapers":[],"albums":[],"lastUpdated":1000}`), f.warm
}

func TestWallpaperRouteWarmExpiryAndCache(t *testing.T) {
	f := &wallpaperSnapshotFake{}
	mux := http.NewServeMux()
	New(nil, WithWallpapers(f), WithModules(mustParseModules(t, "mint,app")), WithResponseCache(cache.NewMemory(1<<20), time.Second, time.Second)).Register(mux)
	for _, path := range []string{"/app/wallpapers", "/v1/app/wallpapers"} {
		for _, warm := range []bool{false, true, false} {
			f.warm = warm
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
			if warm {
				if rec.Code != 200 || rec.Header().Get("Cache-Control") != "public, max-age=300" || rec.Body.String() != `{"wallpapers":[],"albums":[],"lastUpdated":1000}`+"\n" {
					t.Fatalf("warm %d %v %s", rec.Code, rec.Header(), rec.Body)
				}
			} else if rec.Code != 503 {
				t.Fatalf("cold/expired served cached response: %d", rec.Code)
			}
		}
	}
}

func TestBtcmapRoutePassthroughAndCache(t *testing.T) {
	var calls atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/v4/places/42" || r.URL.Query().Get("fields") != "id,osm:payment:lightning" {
			t.Errorf("request %s", r.URL)
		}
		_, _ = w.Write([]byte(`{"id":42,"osm:payment:lightning":"yes"}`))
	}))
	defer upstream.Close()
	mux := http.NewServeMux()
	New(nil, WithBtcmap(btcmap.NewClient(upstream.URL)), WithModules(mustParseModules(t, "mint,app")), WithResponseCache(cache.NewMemory(1<<20), time.Second, time.Second)).Register(mux)
	for _, path := range []string{"/app/btcmap/places/42", "/v1/app/btcmap/places/42"} {
		for _, state := range []string{"miss", "hit"} {
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest("GET", path+"?fields=id,osm:payment:lightning", nil))
			if rec.Code != 200 || rec.Header().Get("Cache-Control") != "public, max-age=3600" || rec.Header().Get("X-Nagg-Cache") != state || !strings.Contains(rec.Body.String(), `"osm:payment:lightning":"yes"`) {
				t.Fatalf("response %d %v %s", rec.Code, rec.Header(), rec.Body)
			}
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("cache missed: %d calls", calls.Load())
	}
}

func TestWallpaperBtcmapCapabilitiesAndModuleParity(t *testing.T) {
	paths := []string{"/app/wallpapers", "/app/btcmap/places", "/app/btcmap/places/42"}
	for _, module := range []string{"mint", "mint,app"} {
		mux := http.NewServeMux()
		h := New(nil, WithModules(mustParseModules(t, module)))
		h.Register(mux)
		for _, prefix := range []string{"", "/v1"} {
			for _, path := range paths {
				rec := httptest.NewRecorder()
				mux.ServeHTTP(rec, httptest.NewRequest("GET", prefix+path, nil))
				want := 404
				if module == "mint,app" {
					want = 503
				}
				if rec.Code != want {
					t.Fatalf("%s %s %d want %d", module, path, rec.Code, want)
				}
				if module == "mint,app" {
					rec = httptest.NewRecorder()
					mux.ServeHTTP(rec, httptest.NewRequest("POST", prefix+path, nil))
					if rec.Code != 405 || rec.Header().Get("Allow") != "GET" {
						t.Fatalf("POST %s: %d %v", path, rec.Code, rec.Header())
					}
				}
			}
		}
		if slices.Contains(h.mountedRoutes(), "/app/wallpapers") != (module == "mint,app") || slices.Contains(h.mountedRoutes(), "/app/btcmap/places/{id}") != (module == "mint,app") {
			t.Fatal("capability route mismatch")
		}
		if _, pattern := mux.Handler(httptest.NewRequest("POST", "/nostr/events/query", nil)); pattern != "" {
			t.Fatal("nostr routes mounted")
		}
	}
	for _, capability := range []string{"app.wallpapers", "app.btcmap"} {
		if !slices.Contains(capabilities.Names, capability) {
			t.Fatalf("missing %s", capability)
		}
	}
}

func TestBtcmapRouteErrors(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(404)
		_, _ = w.Write([]byte("private detail"))
	}))
	defer server.Close()
	mux := http.NewServeMux()
	New(nil, WithBtcmap(btcmap.NewClient(server.URL))).Register(mux)
	for _, tc := range []struct {
		path   string
		status int
	}{
		{"/app/btcmap/places/12", 404}, {"/v1/app/btcmap/places/12", 404},
		{"/app/btcmap/places/saved", 400}, {"/app/btcmap/places?fields=%ZZ", 400},
		{"/app/btcmap/places/12/comments", 404},
	} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", tc.path, nil))
		if rec.Code != tc.status || strings.Contains(rec.Body.String(), "private") {
			t.Fatalf("%s %d %s", tc.path, rec.Code, rec.Body)
		}
	}
}
