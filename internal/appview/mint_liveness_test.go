package appview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/auditor"
	"github.com/vertex-lab/nagg/internal/mintliveness"
)

// stubLiveness answers from a fixed table, keyed like the service does so a
// row's own URL spelling finds its entry.
type stubLiveness map[string]mintliveness.Status

func (s stubLiveness) Liveness(keys []string) map[string]mintliveness.Status {
	out := make(map[string]mintliveness.Status, len(keys))
	for _, key := range keys {
		st, ok := s[mintliveness.Key(key)]
		if !ok {
			st = mintliveness.Unknown()
		}
		out[key] = st
	}
	return out
}

var livenessCheckedAt = time.Unix(1_790_000_000, 0).UTC()

func livenessFixture() stubLiveness {
	return stubLiveness{
		mintliveness.Key("https://up.example"):   {Status: mintliveness.StatusOnline, CheckedAt: &livenessCheckedAt, LatencyMs: 87},
		mintliveness.Key("https://down.example"): {Status: mintliveness.StatusOffline, CheckedAt: &livenessCheckedAt},
	}
}

func discoverRows(t *testing.T, handler *Handler) map[string]map[string]any {
	t.Helper()
	rec := httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	var resp struct {
		Mints []map[string]any `json:"mints"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	out := make(map[string]map[string]any, len(resp.Mints))
	for _, m := range resp.Mints {
		out[m["mintUrl"].(string)] = m
	}
	return out
}

func TestDiscoverCarriesMintLiveness(t *testing.T) {
	roster := fakeAuditor{mints: []auditor.Mint{
		{URL: "https://UP.example/", State: "OK", NMints: 10},
		{URL: "https://down.example", State: "OK", NMints: 10},
		{URL: "https://new.example", State: "OK", NMints: 10},
	}}
	handler := New(mintReviewStore{}, WithNIP05Validation(false), WithAuditor(roster), WithMintLiveness(livenessFixture()))
	rows := discoverRows(t, handler)
	if len(rows) != 3 {
		t.Fatalf("rows = %v", rows)
	}

	// The roster's own spelling is the row's mintUrl, and it still finds the
	// record the sweep filed under the normalized key.
	up := rows["https://UP.example/"]
	if up["status"] != "online" || up["latencyMs"] != float64(87) || up["checkedAt"] != livenessCheckedAt.Format(time.RFC3339) {
		t.Fatalf("online row = %v", up)
	}
	down := rows["https://down.example"]
	if down["status"] != "offline" || down["checkedAt"] != livenessCheckedAt.Format(time.RFC3339) {
		t.Fatalf("offline row = %v", down)
	}
	if _, has := down["latencyMs"]; has {
		t.Fatalf("offline row publishes a latency: %v", down)
	}
	unknown := rows["https://new.example"]
	if unknown["status"] != "unknown" {
		t.Fatalf("unprobed row = %v", unknown)
	}
	for _, key := range []string{"checkedAt", "latencyMs"} {
		if _, has := unknown[key]; has {
			t.Fatalf("unprobed row carries %s: %v", key, unknown)
		}
	}
}

func TestDiscoverWithoutLivenessIsUnknownNotAbsent(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false), WithAuditor(fakeAuditor{mints: []auditor.Mint{
		{URL: "https://up.example", State: "OK", NMints: 10},
	}}))
	rows := discoverRows(t, handler)
	row := rows["https://up.example"]
	if row["status"] != "unknown" {
		t.Fatalf("row without a sweep = %v, want status unknown", row)
	}
	if _, has := row["checkedAt"]; has {
		t.Fatalf("row without a sweep has checkedAt: %v", row)
	}
	if _, has := row["latencyMs"]; has {
		t.Fatalf("row without a sweep has latencyMs: %v", row)
	}
}

func TestMintInfoCarriesMintLiveness(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{{URL: "https://up.example", Name: "Up"}}}),
		WithTestnutMints(fakeTestnuts{}),
		WithMintLiveness(livenessFixture()))

	code, rows := mintInfoRequest(t, handler, "https://Up.Example/", "https://down.example", "https://private.example")
	if code != http.StatusOK || len(rows) != 3 {
		t.Fatalf("code=%d rows=%+v", code, rows)
	}
	up, down, private := rows[0], rows[1], rows[2]
	if up.Status != "online" || up.LatencyMs != 87 || up.CheckedAt == nil || !up.CheckedAt.Equal(livenessCheckedAt) {
		t.Fatalf("up = %+v", up)
	}
	if down.Status != "offline" || down.LatencyMs != 0 || down.CheckedAt == nil {
		t.Fatalf("down = %+v", down)
	}
	// Liveness is independent of Known: a mint nagg has never seen is unknown
	// on both axes, and neither optional field appears.
	if private.Known || private.Status != "unknown" || private.CheckedAt != nil || private.LatencyMs != 0 {
		t.Fatalf("private = %+v", private)
	}

	rec := httptest.NewRecorder()
	handler.mintInfos(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/info?u=https%3A%2F%2Fprivate.example", nil))
	if body := rec.Body.String(); !strings.Contains(body, `"status":"unknown"`) || strings.Contains(body, "checkedAt") || strings.Contains(body, "latencyMs") {
		t.Fatalf("body = %s", body)
	}
}

func TestMintInfoWithoutLivenessIsUnknown(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false), WithTestnutMints(fakeTestnuts{}))
	rec := httptest.NewRecorder()
	handler.mintInfos(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/info?u=https%3A%2F%2Fup.example", nil))
	if body := rec.Body.String(); rec.Code != http.StatusOK || !strings.Contains(body, `"status":"unknown"`) || strings.Contains(body, "checkedAt") {
		t.Fatalf("code=%d body=%s", rec.Code, body)
	}
}
