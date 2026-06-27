package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// mintReviewStore returns a fixed set of kind-38000 events for any QueryEvents
// call, so the handler's parse/dedupe/average logic can be tested without CH.
type mintReviewStore struct {
	fakeStore
	events []chstore.EventView
}

func (s mintReviewStore) QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error) {
	return s.events, nil
}

func reviewEvent(id, pubkey, mintURL, content string, createdAt int64) chstore.EventView {
	return chstore.EventView{
		ID:        id,
		PubKey:    pubkey,
		Kind:      38000,
		Content:   content,
		Tags:      [][]string{{"k", "38172"}, {"u", mintURL}, {"d", "x"}},
		CreatedAt: time.Unix(createdAt, 0),
	}
}

const mintA = "https://mint.example"

func TestMintReviewsAggregatesAndDedupes(t *testing.T) {
	store := mintReviewStore{events: []chstore.EventView{
		reviewEvent("1", "alice", mintA, "great [5/5]", 100),
		reviewEvent("2", "alice", mintA, "actually [4/5]", 200), // newer alice → wins
		reviewEvent("3", "bob", mintA, "ok [3/5]", 150),
		reviewEvent("4", "carol", "https://other.mint", "[1/5]", 50), // different mint → excluded
		reviewEvent("5", "dave", mintA, "no score here", 175),        // counts, no score
	}}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/mint/reviews?u="+mintA, nil)
	handler.mintReviews(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp MintReviewsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	// alice (deduped to her latest), bob, dave → 3 reviews for this mint
	if resp.Summary.ReviewCount != 3 {
		t.Fatalf("reviewCount = %d, want 3", resp.Summary.ReviewCount)
	}
	// scored: alice 4, bob 3 → avg 3.5 (dave has no score, excluded from average)
	if resp.Summary.AverageScore == nil || *resp.Summary.AverageScore != 3.5 {
		t.Fatalf("averageScore = %v, want 3.5", resp.Summary.AverageScore)
	}
	// newest first
	if resp.Reviews[0].CreatedAt < resp.Reviews[len(resp.Reviews)-1].CreatedAt {
		t.Fatalf("reviews not newest-first: %+v", resp.Reviews)
	}
}

func TestMintReviewsBundlesReviewerProfiles(t *testing.T) {
	const alicePk = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const bobPk = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := mintReviewStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{
			alicePk: {PubKey: alicePk, DisplayName: "Alice", Picture: "https://a/pic.png"},
			// bob has no kind-0 row → absent from the profiles map, not an error
		}},
		events: []chstore.EventView{
			reviewEvent("1", alicePk, mintA, "great [5/5]", 100),
			reviewEvent("2", bobPk, mintA, "ok [3/5]", 90),
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/mint/reviews?u="+mintA, nil)
	handler.mintReviews(rec, req)

	var resp MintReviewsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Profiles == nil {
		t.Fatalf("profiles map must be present (never nil)")
	}
	got, ok := resp.Profiles[alicePk]
	if !ok || got.Name != "Alice" || got.Picture != "https://a/pic.png" {
		t.Fatalf("alice profile = %+v (ok=%v), want Alice/https://a/pic.png", got, ok)
	}
	if _, ok := resp.Profiles[bobPk]; ok {
		t.Fatalf("bob has no kind-0; should be absent from profiles, got %+v", resp.Profiles[bobPk])
	}
}

func TestMintReviewsTrailingSlashIsSameMint(t *testing.T) {
	store := mintReviewStore{events: []chstore.EventView{
		reviewEvent("1", "alice", "https://mint.example", "[5/5]", 100),
	}}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/mint/reviews?u=https://mint.example/", nil)
	handler.mintReviews(rec, req)

	var resp MintReviewsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Summary.ReviewCount != 1 {
		t.Fatalf("reviewCount = %d, want 1 (trailing slash should match)", resp.Summary.ReviewCount)
	}
}

func TestMintReviewsRequiresMintURL(t *testing.T) {
	handler := New(mintReviewStore{}, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/mint/reviews", nil)
	handler.mintReviews(rec, req)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
}

func TestDiscoverMintsGroupsBestAttestedFirst(t *testing.T) {
	store := mintReviewStore{events: []chstore.EventView{
		reviewEvent("1", "alice", "https://m1", "[5/5]", 100),
		reviewEvent("2", "bob", "https://m1", "[4/5]", 100),
		reviewEvent("3", "alice", "https://m2", "[3/5]", 100),
	}}
	handler := New(store, WithNIP05Validation(false))

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
	if len(resp.Mints) != 2 {
		t.Fatalf("mints = %d, want 2", len(resp.Mints))
	}
	if resp.Mints[0].MintURL != "https://m1" || resp.Mints[0].ReviewCount != 2 {
		t.Fatalf("first mint = %+v, want m1 with 2 reviews", resp.Mints[0])
	}
}
