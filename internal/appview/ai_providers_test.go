package appview

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/vertex-lab/nagg/internal/aiproviders"
	"github.com/vertex-lab/nagg/internal/capabilities"
	"github.com/vertex-lab/nagg/internal/socialgraph"
)

var providersCheckedAt = time.Date(2026, 9, 25, 6, 0, 0, 0, time.UTC)

type stubProviders struct {
	directory aiproviders.Directory
	ready     bool
}

func (s stubProviders) Directory() (aiproviders.Directory, bool) { return s.directory, s.ready }

// Operators derives the reverse index from the fixture rows, the way the
// real service does from its records.
func (s stubProviders) Operators() map[string][]string {
	out := map[string][]string{}
	for _, p := range s.directory.Providers {
		if p.Pubkey != "" {
			out[p.Pubkey] = append(out[p.Pubkey], p.BaseURL)
		}
	}
	return out
}

func followers(n uint64) *uint64 { return &n }

func checkedAt(offset time.Duration) *time.Time {
	at := providersCheckedAt.Add(offset)
	return &at
}

// fixtureDirectory is already in served order: online before unknown before
// offline, sealed models first, then followers descending.
func fixtureDirectory() aiproviders.Directory {
	return aiproviders.Directory{
		Providers: []aiproviders.Provider{
			{
				BaseURL:             "https://ai.redsh1ft.com",
				Name:                "redsh1ft",
				Pubkey:              "aa3f3bf381ac923afcf5a3c16fb2957de94057de84df0c3e84a44c57fa031482",
				Followers:           followers(1234),
				FollowersSource:     socialgraph.SourceGraph,
				ModelCount:          564,
				EncryptedModelCount: 9,
				TEEModelCount:       13,
				Mints:               []string{"https://mint.minibits.cash/Bitcoin"},
				Status:              aiproviders.StatusOnline,
				CheckedAt:           checkedAt(0),
				LatencyMs:           340,
			},
			{
				// Discovered, never probed, operator reach never resolved:
				// two different unknowns on one row, both spelled as absence.
				BaseURL:    "https://new.example",
				Name:       "new.example",
				Mints:      []string{},
				ModelCount: 0,
				Status:     aiproviders.StatusUnknown,
			},
			{
				BaseURL:         "https://down.example",
				Name:            "down.example",
				Followers:       followers(99),
				FollowersSource: socialgraph.SourceRelays,
				ModelCount:      12,
				Mints:           []string{},
				Status:          aiproviders.StatusOffline,
				CheckedAt:       checkedAt(-time.Minute),
			},
		},
		CheckedAt:  providersCheckedAt,
		TTLSeconds: 300,
	}
}

// TestAIProvidersRouteContract pins the wire shape the Sovran app builds
// against: field names, the three statuses, and the omissions that make
// "unknown" readable as "not established" rather than "zero".
func TestAIProvidersRouteContract(t *testing.T) {
	h := New(nil, WithAIProviders(stubProviders{directory: fixtureDirectory(), ready: true}))

	rec := httptest.NewRecorder()
	h.aiProviders(rec, httptest.NewRequest(http.MethodGet, "/app/ai-providers", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d: %s", rec.Code, rec.Body)
	}

	var body struct {
		Providers []struct {
			BaseURL             string   `json:"baseUrl"`
			Name                string   `json:"name"`
			Pubkey              string   `json:"pubkey"`
			Followers           *uint64  `json:"followers"`
			FollowersSource     string   `json:"followersSource"`
			ModelCount          int      `json:"modelCount"`
			EncryptedModelCount int      `json:"encryptedModelCount"`
			TEEModelCount       int      `json:"teeModelCount"`
			Mints               []string `json:"mints"`
			Status              string   `json:"status"`
			CheckedAt           string   `json:"checkedAt"`
			LatencyMs           int      `json:"latencyMs"`
		} `json:"providers"`
		CheckedAt  string `json:"checkedAt"`
		TTLSeconds int    `json:"ttlSeconds"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode: %v (%s)", err, rec.Body)
	}
	if body.CheckedAt != "2026-09-25T06:00:00Z" || body.TTLSeconds != 300 {
		t.Fatalf("envelope = %q / %d", body.CheckedAt, body.TTLSeconds)
	}
	if len(body.Providers) != 3 {
		t.Fatalf("providers = %d", len(body.Providers))
	}

	first := body.Providers[0]
	if first.BaseURL != "https://ai.redsh1ft.com" || first.Name != "redsh1ft" {
		t.Fatalf("first provider = %+v", first)
	}
	if first.Followers == nil || *first.Followers != 1234 || first.FollowersSource != "graph" {
		t.Fatalf("followers = %v from %q, want 1234 from the exact source", first.Followers, first.FollowersSource)
	}
	// An approximate count says so, so the app can render "99+" rather than
	// presenting a relay floor as a measurement.
	if offlineRow := body.Providers[2]; offlineRow.FollowersSource != "relays" {
		t.Fatalf("relay-sourced row = %q, want its source named", offlineRow.FollowersSource)
	}
	// A count, never a flag: on this node 9 of 564 priced models are sealed
	// and the rest are plaintext, so "is this provider E2EE" has no true
	// answer. The two counts are separate claims and must stay separate on the
	// wire — encryptedModelCount is what a client will seal (the same test the
	// model picker badges on), teeModelCount is only what the node declares,
	// and the four in the gap are sent in the clear.
	if first.ModelCount != 564 || first.EncryptedModelCount != 9 || first.TEEModelCount != 13 {
		t.Fatalf("counts = %d/%d/%d, want 564/9/13", first.ModelCount, first.EncryptedModelCount, first.TEEModelCount)
	}
	if first.Status != "online" || first.CheckedAt != "2026-09-25T06:00:00Z" || first.LatencyMs != 340 {
		t.Fatalf("online row = %+v", first)
	}

	// The app sorts and labels on unknown vs offline, so the two must be
	// distinguishable on the wire, not just in nagg's head.
	unknown, offline := body.Providers[1], body.Providers[2]
	if unknown.Status != "unknown" || offline.Status != "offline" {
		t.Fatalf("statuses = %q, %q", unknown.Status, offline.Status)
	}
	if unknown.CheckedAt != "" {
		t.Fatalf("unknown row carried checkedAt %q; nothing has been established", unknown.CheckedAt)
	}
	// The other unknown on the same row: reach nagg never resolved must be
	// null, not 0. A zero here would read as "nobody follows this operator",
	// which is the defect that made every row on two live endpoints report 0.
	if unknown.Followers != nil {
		t.Fatalf("unresolved reach rendered as %d; it must be null", *unknown.Followers)
	}
	if unknown.FollowersSource != "" {
		t.Fatalf("unresolved reach named a source %q", unknown.FollowersSource)
	}
	if offline.CheckedAt != "2026-09-25T05:59:00Z" {
		t.Fatalf("offline row checkedAt = %q, want when ITS status was established", offline.CheckedAt)
	}
	// An offline provider stays listed with what it served when it was up: a
	// probe failure is not a reason to tell the app the provider is gone.
	if offline.ModelCount != 12 {
		t.Fatalf("offline row lost its model count: %+v", offline)
	}

	// pubkey and latencyMs are omitted rather than zero-valued, so "unknown
	// operator" never renders as a key of all zeroes.
	raw := rec.Body.String()
	// Scope to the providers array: the identities block below it names the
	// same key on purpose.
	rowsOnly := raw[:strings.Index(raw, `"identities"`)]
	if got := strings.Count(rowsOnly, `"pubkey"`); got != 1 {
		t.Fatalf("pubkey appeared %d times; it must be omitted when unknown: %s", got, raw)
	}
	if got := strings.Count(rowsOnly, `"latencyMs"`); got != 1 {
		t.Fatalf("latencyMs appeared %d times; it must be omitted without a successful probe: %s", got, raw)
	}
	if !json.Valid(rec.Body.Bytes()) {
		t.Fatal("invalid JSON")
	}
}

// TestAIProvidersUnconfiguredIs503 is the config contract: an unset
// NAGG_AI_PROVIDERS_* leaves this one route 503 and breaks nothing else, so the
// app falls back to discovering providers client-side exactly as it does today.
func TestAIProvidersUnconfiguredIs503(t *testing.T) {
	h := New(nil)
	rec := httptest.NewRecorder()
	h.aiProviders(rec, httptest.NewRequest(http.MethodGet, "/app/ai-providers", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("unconfigured status = %d, want 503", rec.Code)
	}

	// Configured but still warming is also 503 — never an empty provider list,
	// which the app would read as "there are no providers".
	h = New(nil, WithAIProviders(stubProviders{ready: false}))
	rec = httptest.NewRecorder()
	h.aiProviders(rec, httptest.NewRequest(http.MethodGet, "/app/ai-providers", nil))
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("warming status = %d, want 503", rec.Code)
	}

	h = New(nil, WithAIProviders(stubProviders{directory: fixtureDirectory(), ready: true}))
	rec = httptest.NewRecorder()
	h.aiProviders(rec, httptest.NewRequest(http.MethodPost, "/app/ai-providers", nil))
	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status = %d, want 405", rec.Code)
	}
}

// TestAIProvidersIsRegisteredWithItsV1Alias: clients feature-gate on the
// manifest, and both the bare and /v1-prefixed paths must serve.
func TestAIProvidersIsRegisteredWithItsV1Alias(t *testing.T) {
	if !slices.Contains(capabilities.Names, "app.aiProviders") {
		t.Fatal("missing app.aiProviders capability")
	}
	if !slices.Contains(capabilities.AppViewRoutes, "/app/ai-providers") {
		t.Fatal("/app/ai-providers is not advertised")
	}
	h := New(nil, WithAIProviders(stubProviders{directory: fixtureDirectory(), ready: true}))
	mux := http.NewServeMux()
	h.Register(mux)
	for _, path := range []string{"/app/ai-providers", "/v1/app/ai-providers"} {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("%s = %d, want 200", path, rec.Code)
		}
	}
}
