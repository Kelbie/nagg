package appview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"

	"github.com/vertex-lab/nagg/internal/modules"
)

func mustParseModules(t *testing.T, csv string) modules.Set {
	t.Helper()
	set, err := modules.Parse(csv)
	if err != nil {
		t.Fatalf("parse modules %q: %v", csv, err)
	}
	return set
}

// The mint deployment's entire REST surface, pinned. A route appearing here
// that isn't listed is one the mint service serves against tables it does not
// have.
func TestMintModuleMountsOnlyMintRoutes(t *testing.T) {
	h := &Handler{modules: mustParseModules(t, "mint")}
	want := []string{
		"/nostr/capabilities",
		"/nostr/mint/changes",
		"/nostr/mint/discover",
		"/nostr/mint/history",
		"/nostr/mint/reviews",
	}
	got := h.mountedRoutes()
	sort.Strings(got)
	if len(got) != len(want) {
		t.Fatalf("mounted %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("mounted %v, want %v", got, want)
		}
	}
}

// A mint-only Handler must not even register the feed routes: an unmounted path
// 404s, which is honest, while a mounted one would 500 on a missing table.
func TestMintModuleDoesNotServeFeedRoutes(t *testing.T) {
	mux := http.NewServeMux()
	New(nil, WithModules(mustParseModules(t, "mint"))).Register(mux)

	for _, path := range []string{"/nostr/feed", "/nostr/notifications", "/nostr/thread", "/app/ai-lineup"} {
		if _, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil)); pattern != "" {
			t.Errorf("%s is mounted in a mint-only deployment (pattern %q)", path, pattern)
		}
	}
	for _, path := range []string{"/nostr/mint/changes", "/nostr/capabilities", "/v1/nostr/mint/history"} {
		if _, pattern := mux.Handler(httptest.NewRequest(http.MethodGet, path, nil)); pattern == "" {
			t.Errorf("%s is NOT mounted in a mint-only deployment", path)
		}
	}
}

// Clients feature-gate on /nostr/capabilities, so a partial deployment must
// advertise exactly what it serves — promising a route that 404s is worse than
// not promising it.
func TestCapabilitiesAdvertiseOnlyMountedRoutes(t *testing.T) {
	h := New(nil, WithModules(mustParseModules(t, "mint")))
	rec := httptest.NewRecorder()
	h.capabilities(rec, httptest.NewRequest(http.MethodGet, "/nostr/capabilities", nil))

	var body struct {
		AppViews []struct {
			Routes []string `json:"routes"`
		} `json:"appViews"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode capabilities: %v", err)
	}
	if len(body.AppViews) != 1 {
		t.Fatalf("appViews = %d, want 1", len(body.AppViews))
	}
	for _, route := range body.AppViews[0].Routes {
		if route == "/nostr/feed" {
			t.Fatal("a mint-only deployment advertises /nostr/feed")
		}
	}
	if len(body.AppViews[0].Routes) != len(h.mountedRoutes()) {
		t.Fatalf("advertised %d routes, mounts %d", len(body.AppViews[0].Routes), len(h.mountedRoutes()))
	}
}

// The compatibility contract: a Handler that never mentions modules must mount
// every declared route, exactly as it did before modules existed.
func TestUnconfiguredHandlerMountsEveryRoute(t *testing.T) {
	h := &Handler{}
	if got, want := len(h.mountedRoutes()), len(h.routes()); got != want {
		t.Fatalf("unconfigured Handler mounts %d of %d routes", got, want)
	}
	full := &Handler{modules: modules.All()}
	if got, want := len(full.mountedRoutes()), len(h.routes()); got != want {
		t.Fatalf("modules.All() mounts %d of %d routes", got, want)
	}
}

// Every route must claim an owner, or it silently ships to every deployment.
func TestEveryRouteDeclaresAModule(t *testing.T) {
	for _, r := range (&Handler{}).routes() {
		if r.module == "" {
			t.Errorf("route %q declares no module", r.path)
		}
	}
}
