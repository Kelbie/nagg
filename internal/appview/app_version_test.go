package appview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLatestVersionReturnsConfiguredVersion(t *testing.T) {
	handler := New(fakeStore{}, WithNIP05Validation(false), WithAppVersion("1.2.3", "Tap to update"))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/app/latest-version", strings.NewReader(`{"storage":{"version":"1.0.0"}}`))
	handler.latestVersion(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp LatestVersionResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Version != "1.2.3" || resp.Message != "Tap to update" {
		t.Fatalf("resp = %+v, want 1.2.3 / Tap to update", resp)
	}
}

func TestLatestVersionRejectsGet(t *testing.T) {
	handler := New(fakeStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	handler.latestVersion(rec, httptest.NewRequest(http.MethodGet, "/app/latest-version", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want 405", rec.Code)
	}
}
