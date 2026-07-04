package appview

import (
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
