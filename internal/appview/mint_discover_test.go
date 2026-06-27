package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/vertex-lab/nagg/internal/auditor"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

type fakeAuditor struct {
	mints []auditor.Mint
}

func (f fakeAuditor) Mints(context.Context) ([]auditor.Mint, error) { return f.mints, nil }

func TestDiscoverMintsMergesAuditorReviewsAndOperator(t *testing.T) {
	const opPk = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	store := mintReviewStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				opPk: {PubKey: opPk, DisplayName: "Op Account", Picture: "https://op/pic.png"},
			},
			counts: chstore.FollowCounts{Followers: 1234, Follows: 56},
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
