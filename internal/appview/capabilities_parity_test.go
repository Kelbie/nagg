package appview

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/vertex-lab/nagg/internal/capabilities"
)

// TestCapabilitiesRouteParity pins the advertised capabilities manifest to the
// routes Register actually mounts. The manifest is what clients feature-gate
// on; before this test it had silently drifted to under-report seven live
// routes.
func TestCapabilitiesRouteParity(t *testing.T) {
	h := &Handler{}
	mounted := map[string]bool{}
	for _, r := range h.routes() {
		mounted[r.path] = true
	}
	advertised := map[string]bool{}
	for _, p := range capabilities.AppViewRoutes {
		advertised[p] = true
	}
	for p := range mounted {
		if !advertised[p] {
			t.Errorf("route %q is mounted but missing from capabilities.AppViewRoutes", p)
		}
	}
	for p := range advertised {
		if !mounted[p] {
			t.Errorf("capabilities.AppViewRoutes advertises %q but Register does not mount it", p)
		}
	}
}

func TestCapabilitiesDiscoverUptime(t *testing.T) {
	rec := httptest.NewRecorder()
	capabilities.WriteHeaders(rec)
	if !strings.Contains(rec.Header().Get("X-Nagg-Capabilities"), "appview.mint.discover.uptime") {
		t.Fatal("uptime capability missing from headers")
	}
	info := capabilities.ServiceInfo()
	for _, name := range info["capabilities"].([]string) {
		if name == "appview.mint.discover.uptime" {
			return
		}
	}
	t.Fatal("uptime capability missing from manifest")
}
