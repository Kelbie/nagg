package appview

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/vertex"
)

const testPubkey = "82341f05fdb1dffbc78894993292171ed03abbed34a95f22f55f9b6371723ee6"

type fakeStore struct {
	profiles map[string]chstore.ProfileRow
	counts   chstore.FollowCounts
}

func (s fakeStore) FollowsFeed(context.Context, []string, int64, uint64, uint64) ([]chstore.EventView, error) {
	return nil, nil
}

func (s fakeStore) TrendingFeed(context.Context, time.Time, uint64) ([]chstore.EventView, error) {
	return nil, nil
}

func (s fakeStore) QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error) {
	return nil, nil
}

func (s fakeStore) NoteStats(context.Context, []string) (map[string]chstore.NoteStats, error) {
	return map[string]chstore.NoteStats{}, nil
}

func (s fakeStore) LatestProfiles(_ context.Context, pubkeys []string) (map[string]chstore.ProfileRow, error) {
	out := make(map[string]chstore.ProfileRow, len(pubkeys))
	for _, pubkey := range pubkeys {
		if profile, ok := s.profiles[pubkey]; ok {
			out[pubkey] = profile
		}
	}
	return out, nil
}

func (s fakeStore) FollowCounts(context.Context, string) (chstore.FollowCounts, error) {
	return s.counts, nil
}

func (s fakeStore) ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	return nil, nil, nil
}

type fakeVertex struct {
	profile vertex.ProfileResult
	search  []vertex.SearchResult
}

func (v fakeVertex) Search(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, bool, error) {
	return v.search, true, nil
}

func (v fakeVertex) Recommended(context.Context, vertex.RecommendedArgs) ([]vertex.SearchResult, bool, error) {
	return v.search, true, nil
}

func (v fakeVertex) Profile(context.Context, string) (vertex.ProfileResult, bool, error) {
	return v.profile, true, nil
}

func TestProfileMergesVertexWithLocalProfile(t *testing.T) {
	score := 42.5
	nodes := 1000
	createdAt := time.Unix(1_710_000_000, 0)
	handler := New(
		fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:      testPubkey,
					CreatedAt:   createdAt,
					Name:        "sovran",
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
				},
			},
			counts: chstore.FollowCounts{Follows: 3, Followers: 10},
		},
		WithVertex(fakeVertex{
			profile: vertex.ProfileResult{
				PubKey: testPubkey,
				Npub:   vertex.Npub(testPubkey),
				Rank:   0.01,
				Score:  &score,
				Nodes:  &nodes,
			},
		}),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/profile?pubkey="+testPubkey, nil)
	handler.profile(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response ProfileResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.PubKey != testPubkey || response.Npub == "" {
		t.Fatalf("identity fields not set: %+v", response)
	}
	if response.Name == nil || *response.Name != "sovran" {
		t.Fatalf("name = %v", response.Name)
	}
	if response.Followers != 10 || response.Follows != 3 {
		t.Fatalf("counts = followers %d follows %d", response.Followers, response.Follows)
	}
	if response.Score == nil || *response.Score != score {
		t.Fatalf("score = %v", response.Score)
	}
	if response.CreatedAt == nil || *response.CreatedAt != createdAt.Unix() {
		t.Fatalf("created_at = %v", response.CreatedAt)
	}
}

func TestSearchEnrichesRowsWithLocalProfiles(t *testing.T) {
	rank := 0.01
	score := 42.5
	handler := New(
		fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:      testPubkey,
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
				},
			},
		},
		WithVertex(fakeVertex{
			search: []vertex.SearchResult{{
				PubKey: testPubkey,
				Npub:   vertex.Npub(testPubkey),
				Rank:   &rank,
				Score:  &score,
			}},
		}),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/search?query=sovran&limit=1", nil)
	handler.search(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response struct {
		Query     string         `json:"query"`
		Results   []SearchResult `json:"results"`
		FromCache bool           `json:"fromCache"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.FromCache || response.Query != "sovran" {
		t.Fatalf("response metadata = %+v", response)
	}
	if len(response.Results) != 1 {
		t.Fatalf("results len = %d", len(response.Results))
	}
	if response.Results[0].DisplayName == nil || *response.Results[0].DisplayName != "Sovran" {
		t.Fatalf("displayName = %v", response.Results[0].DisplayName)
	}
}

func TestRegisterAppliesRateLimit(t *testing.T) {
	handler := New(
		fakeStore{},
		WithVertex(fakeVertex{}),
		WithRateLimit(1, time.Minute),
		WithNIP05Validation(false),
	)
	mux := http.NewServeMux()
	handler.Register(mux)

	first := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/search?query=sovran&limit=1", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	mux.ServeHTTP(first, req)
	if first.Code != http.StatusOK {
		t.Fatalf("first status = %d body = %s", first.Code, first.Body.String())
	}

	second := httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/nostr/search?query=sovran&limit=1", nil)
	req.RemoteAddr = "192.0.2.1:1234"
	mux.ServeHTTP(second, req)
	if second.Code != http.StatusTooManyRequests {
		t.Fatalf("second status = %d body = %s", second.Code, second.Body.String())
	}
}
