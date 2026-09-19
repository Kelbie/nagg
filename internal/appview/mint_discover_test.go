package appview

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/vertex-lab/nagg/internal/auditor"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/mintinfo"
)

type fakeAuditor struct {
	mints []auditor.Mint
}

func (f fakeAuditor) Mints(context.Context) ([]auditor.Mint, error) { return f.mints, nil }

func TestDiscoverMintsMergesAuditorReviewsAndOperator(t *testing.T) {
	const opPk = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	store := mintReviewStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.K0Row{
				opPk: {PubKey: opPk, DisplayName: "Op Account", Picture: "https://op/pic.png"},
			},
			counts: chstore.PubkeyStats{Followers: 1234, Follows: 56},
		},
		events: []chstore.EventView{
			reviewEvent("1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "https://m1", "great [5/5]", 100),
			reviewEvent("2", "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd", "https://m1", "i recommend this", 90), // no score → favourite
		},
	}
	auditorClient := fakeAuditor{mints: []auditor.Mint{{
		URL: "https://m1", Name: "Mint One", State: "OK",
		NMints: 100, NMelts: 40, NErrors: 2,
		Units: []string{"sat", "usd"}, IconURL: "https://m1/icon.png",
		OperatorContact: opPk,
	}}}
	handler := New(store, WithNIP05Validation(false), WithAuditor(auditorClient))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/mint/discover", nil)
	handler.discoverMints(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp DiscoverMintsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Mints) != 1 {
		t.Fatalf("mints = %d, want 1", len(resp.Mints))
	}
	m := resp.Mints[0]
	if m.MintURL != "https://m1" || !m.HasAudit || m.State != "OK" || m.NMints != 100 {
		t.Fatalf("audit fields wrong: %+v", m)
	}
	if len(m.SupportedUnits) != 2 {
		t.Fatalf("units = %v, want 2", m.SupportedUnits)
	}
	if m.AverageScore == nil || *m.AverageScore != 5 {
		t.Fatalf("averageScore = %v, want 5", m.AverageScore)
	}
	if m.ReviewCount != 2 || m.FavouriteCount != 1 {
		t.Fatalf("reviewCount=%d favouriteCount=%d, want 2/1", m.ReviewCount, m.FavouriteCount)
	}
	if m.OperatorPubkey != opPk || m.Followers != 1234 || m.Follows != 56 {
		t.Fatalf("operator social wrong: pubkey=%s followers=%d follows=%d", m.OperatorPubkey, m.Followers, m.Follows)
	}
	if got, ok := resp.Profiles[opPk]; !ok || got.Name != "Op Account" {
		t.Fatalf("operator profile = %+v ok=%v, want Op Account", got, ok)
	}
}

func f(v float64) *float64 { return &v }

func TestSortDiscoverMintsGreenFirstThenWeighted(t *testing.T) {
	greenStrong := DiscoverMint{
		MintURL: "https://green-strong", HasAudit: true, State: "OK",
		NMints: 900, NMelts: 900, NErrors: 10, // ~99% uptime
		AverageScore: f(4.8), ReviewCount: 40, Followers: 5000,
	}
	greenWeak := DiscoverMint{
		MintURL: "https://green-weak", HasAudit: true, State: "OK",
		NMints: 5, NMelts: 5, NErrors: 0, // 100% uptime but tiny
		AverageScore: nil, ReviewCount: 0, Followers: 0,
	}
	errorBusy := DiscoverMint{
		MintURL: "https://error-busy", HasAudit: true, State: "ERROR",
		NMints: 10, NMelts: 10, NErrors: 500, // error-heavy
		AverageScore: f(5.0), ReviewCount: 200, Followers: 99999,
	}
	noAudit := DiscoverMint{
		MintURL: "https://no-audit", HasAudit: false,
		AverageScore: f(4.0), ReviewCount: 10,
	}

	mints := []DiscoverMint{errorBusy, noAudit, greenWeak, greenStrong}
	sortDiscoverMints(mints)
	order := []string{mints[0].MintURL, mints[1].MintURL, mints[2].MintURL, mints[3].MintURL}

	// Both green-passing mints rank above the (busier, higher-scored) non-green
	// ones — green is always first. greenStrong outranks greenWeak on the blend.
	want := []string{
		"https://green-strong",
		"https://green-weak",
		"https://error-busy", // higher weighted score than no-audit, but both non-green
		"https://no-audit",
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("order[%d] = %s, want %s (full: %v)", i, order[i], want[i], order)
		}
	}
}

func TestDiscoverMintsDegradesWithoutAuditor(t *testing.T) {
	store := mintReviewStore{events: []chstore.EventView{
		reviewEvent("1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "https://m1", "[4/5]", 100),
	}}
	handler := New(store, WithNIP05Validation(false)) // no auditor wired

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/mint/discover", nil)
	handler.discoverMints(rec, req)

	var resp DiscoverMintsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Mints) != 1 || resp.Mints[0].HasAudit {
		t.Fatalf("expected 1 review-only mint without audit, got %+v", resp.Mints)
	}
}

func TestDiscoverUptimeJSONAndMintFilter(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false), WithAuditor(fakeAuditor{mints: []auditor.Mint{
		{URL: "https://other", State: "OK", NMints: 100},
		{URL: "https://mint.example/", State: "ERROR", Uptime24h: f(0), AvgLatencyMs: f(4446.2), Source: "ucash", UpdatedAt: 12345},
	}}))
	for _, query := range []string{"https://MINT.example", "https://mint.example/"} {
		rec := httptest.NewRecorder()
		handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover?limit=1&mint="+url.QueryEscape(query), nil))
		if rec.Code != http.StatusOK {
			t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
		}
		var response struct {
			Mints []map[string]any `json:"mints"`
		}
		if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		if len(response.Mints) != 1 {
			t.Fatalf("mints=%v", response.Mints)
		}
		m := response.Mints[0]
		if m["uptime24h"] != float64(0) || m["avgLatencyMs"] != 4446.2 || m["auditSource"] != "ucash" || m["auditUpdatedAt"] != float64(12345) {
			t.Fatalf("audit JSON=%v", m)
		}
	}
	rec := httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover?mint=https://missing", nil))
	var response DiscoverMintsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Mints == nil || len(response.Mints) != 0 {
		t.Fatalf("missing mint: %s", rec.Body.String())
	}
}

func TestDiscoverRankPrefersUptime24h(t *testing.T) {
	oldSuccess := DiscoverMint{MintURL: "https://old", HasAudit: true, State: "OK", NMints: 1000, Uptime24h: f(0)}
	recentSuccess := DiscoverMint{MintURL: "https://recent", HasAudit: true, State: "OK", NErrors: 1000, Uptime24h: f(100)}
	mints := []DiscoverMint{oldSuccess, recentSuccess}
	sortDiscoverMints(mints)
	if mints[0].MintURL != recentSuccess.MintURL {
		t.Fatal("lifetime counters overrode measured uptime")
	}
	oldSuccess.Uptime24h = nil
	if discoverRankScore(oldSuccess) != weightUptime {
		t.Fatal("legacy uptime fallback lost")
	}
}

type discoverHistory struct {
	document json.RawMessage
	calls    []string
	err      error
}

func (p *discoverHistory) LatestInfo(_ context.Context, mint string) (json.RawMessage, error) {
	p.calls = append(p.calls, mint)
	return p.document, p.err
}
func (p *discoverHistory) History(context.Context, string, bool) (*mintinfo.History, bool, error) {
	return nil, false, nil
}
func (p *discoverHistory) GlobalChanges(context.Context, int) (*mintinfo.GlobalChanges, error) {
	return nil, nil
}

func TestDiscoverNIP87OnlyBackfill(t *testing.T) {
	store := mintReviewStore{events: []chstore.EventView{
		reviewEvent("1", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "https://mint/", "[4/5]", 100),
		reviewEvent("2", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "https://audited", "[5/5]", 100),
	}}
	history := &discoverHistory{document: json.RawMessage(`{"name":"Latest name","icon_url":"https://icon","description":"Latest description","nuts":{"4":{"methods":[{"unit":"sat"}]},"5":{"methods":[{"unit":"usd"}]}}}`)}
	handler := New(store, WithNIP05Validation(false), WithMintHistory(history), WithAuditor(fakeAuditor{mints: []auditor.Mint{{URL: "https://audited", Name: "Audit name"}}}))
	rec := httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover", nil))
	var response DiscoverMintsResponse
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if len(history.calls) != 1 || history.calls[0] != normalizeMintURL("https://mint/") {
		t.Fatalf("history calls=%v", history.calls)
	}
	if len(response.Mints) != 2 {
		t.Fatalf("mints=%v", response.Mints)
	}
	for _, m := range response.Mints {
		if m.MintURL == "https://audited" {
			if m.Name != "Audit name" {
				t.Fatal("overwrote auditor metadata")
			}
			continue
		}
		if m.HasAudit || m.AuditSource != "" || m.Uptime24h != nil || m.Name != "Latest name" || m.Description != "Latest description" || m.IconURL != "https://icon" || len(m.Nuts) == 0 || len(m.SupportedUnits) != 2 || m.ReviewCount != 1 {
			t.Fatalf("backfilled row=%+v", m)
		}
	}
	history.err = errors.New("snapshot unavailable")
	rec = httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover?mint=https://mint", nil))
	if rec.Code != http.StatusOK {
		t.Fatal("history error failed discovery")
	}
}

// TestDiscoverScansFullReviewSet pins the regression where the discovery
// aggregate went through QueryEvents, whose page clamp turned the 5000-wide
// scan into the 50 newest reviews: mints reviewed earlier vanished and popular
// mints under-counted. The scan must ask the dedicated reader for the full cap,
// and every reviewed mint must surface regardless of how many rows precede it.
func TestDiscoverScansFullReviewSet(t *testing.T) {
	var scans []uint64
	events := make([]chstore.EventView, 0, 120)
	for i := 0; i < 120; i++ {
		// 120 distinct reviewers of one popular mint, newest first...
		events = append(events, reviewEvent(fmt.Sprintf("p%03d", i), fmt.Sprintf("%064d", i), "https://popular", "[5/5]", int64(10_000-i)))
	}
	// ...then an older review of a second mint that a 50-row page would never reach.
	events = append(events, reviewEvent("old", fmt.Sprintf("%064d", 999), "https://older", "[4/5]", 1))
	store := mintReviewStore{events: events, scanLimits: &scans}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
	if len(scans) != 1 || scans[0] != discoverReviewScanCap {
		t.Fatalf("scan widths=%v, want one scan of %d", scans, discoverReviewScanCap)
	}
	var resp DiscoverMintsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Mints) != 2 {
		t.Fatalf("mints=%d, want 2", len(resp.Mints))
	}
	for _, m := range resp.Mints {
		switch m.MintURL {
		case "https://popular":
			if m.ReviewCount != 120 {
				t.Fatalf("popular reviewCount=%d, want 120", m.ReviewCount)
			}
		case "https://older":
			if m.ReviewCount != 1 {
				t.Fatalf("older reviewCount=%d, want 1", m.ReviewCount)
			}
		default:
			t.Fatalf("unexpected mint %q", m.MintURL)
		}
	}
}

type fakeTestnuts struct {
	urls []string
	err  error
}

func (f fakeTestnuts) TestnutMintURLs(context.Context) ([]string, error) { return f.urls, f.err }

func TestDiscoverTestnutFlagAndFilter(t *testing.T) {
	auditorClient := WithAuditor(fakeAuditor{mints: []auditor.Mint{
		{URL: "https://real.example", State: "OK", NMints: 10},
		{URL: "https://nofee.testnut.example", State: "OK", NMints: 5},
		{URL: "https://unprobed.example", State: "OK", NMints: 1},
	}})
	// The probe stores its own URL normalization (host case kept in path only);
	// discover must still match it.
	handler := New(mintReviewStore{}, WithNIP05Validation(false), auditorClient,
		WithTestnutMints(fakeTestnuts{urls: []string{"https://NoFee.Testnut.example/"}}))

	discover := func(query string) (int, map[string]bool) {
		t.Helper()
		rec := httptest.NewRecorder()
		handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover"+query, nil))
		if rec.Code != http.StatusOK {
			return rec.Code, nil
		}
		var resp DiscoverMintsResponse
		if err := json.Unmarshal(rec.Body.Bytes(), &resp); err != nil {
			t.Fatal(err)
		}
		out := map[string]bool{}
		for _, m := range resp.Mints {
			out[m.MintURL] = m.Testnut
		}
		return rec.Code, out
	}

	if _, all := discover(""); len(all) != 3 || !all["https://nofee.testnut.example"] || all["https://real.example"] || all["https://unprobed.example"] {
		t.Fatalf("unfiltered = %v", all)
	}
	if _, only := discover("?testnut=true"); len(only) != 1 || !only["https://nofee.testnut.example"] {
		t.Fatalf("testnut=true = %v", only)
	}
	if _, none := discover("?testnut=false"); len(none) != 2 || none["https://real.example"] || none["https://unprobed.example"] {
		t.Fatalf("testnut=false = %v", none)
	}
	if code, _ := discover("?testnut=maybe"); code != http.StatusBadRequest {
		t.Fatalf("invalid testnut status = %d, want 400", code)
	}
}

func TestDiscoverTestnutLookupFailure(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false),
		WithAuditor(fakeAuditor{mints: []auditor.Mint{{URL: "https://m1", State: "OK"}}}),
		WithTestnutMints(fakeTestnuts{err: errors.New("clickhouse down")}))

	rec := httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("unfiltered should degrade, got %d", rec.Code)
	}
	rec = httptest.NewRecorder()
	handler.discoverMints(rec, httptest.NewRequest(http.MethodGet, "/nostr/mint/discover?testnut=false", nil))
	if rec.Code == http.StatusOK {
		t.Fatalf("filtered request must not answer without verdicts: %s", rec.Body.String())
	}
}
