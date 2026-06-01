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

type sequencedFeedStore struct {
	fakeStore
	feeds [][]chstore.EventView
	calls int
}

func (s *sequencedFeedStore) FollowsFeed(context.Context, []string, int64, uint64, uint64) ([]chstore.EventView, error) {
	if len(s.feeds) == 0 {
		s.calls++
		return nil, nil
	}
	idx := s.calls
	if idx >= len(s.feeds) {
		idx = len(s.feeds) - 1
	}
	s.calls++
	return s.feeds[idx], nil
}

type sequencedEventStore struct {
	fakeStore
	events [][]chstore.EventView
	calls  int
}

func (s *sequencedEventStore) QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error) {
	if len(s.events) == 0 {
		s.calls++
		return nil, nil
	}
	idx := s.calls
	if idx >= len(s.events) {
		idx = len(s.events) - 1
	}
	s.calls++
	return s.events[idx], nil
}

type sequencedThreadStore struct {
	fakeStore
	root    chstore.EventView
	threads [][]chstore.EventView
	calls   int
}

func (s *sequencedThreadStore) ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	idx := s.calls
	if idx >= len(s.threads) {
		idx = len(s.threads) - 1
	}
	s.calls++
	return &s.root, s.threads[idx], nil
}

type fakeUserBackfiller struct {
	calls  int
	pubkey string
	limit  uint64
}

func (f *fakeUserBackfiller) BackfillUserFeed(_ context.Context, pubkey string, limit uint64) error {
	f.calls++
	f.pubkey = pubkey
	f.limit = limit
	return nil
}

type fakeAppBackfiller struct {
	fakeUserBackfiller
	eventCalls      int
	eventIDs        []string
	profileCalls    int
	profilePubkeys  []string
	engagementCalls int
	engagementIDs   []string
	threadCalls     int
	threadID        string
	threadLimit     int
	followCalls     int
	followPubkey    string
}

func (f *fakeAppBackfiller) BackfillEvents(_ context.Context, ids []string) error {
	f.eventCalls++
	f.eventIDs = append([]string(nil), ids...)
	return nil
}

func (f *fakeAppBackfiller) BackfillProfiles(_ context.Context, pubkeys []string) error {
	f.profileCalls++
	f.profilePubkeys = append([]string(nil), pubkeys...)
	return nil
}

func (f *fakeAppBackfiller) BackfillEngagement(_ context.Context, ids []string) error {
	f.engagementCalls++
	f.engagementIDs = append([]string(nil), ids...)
	return nil
}

func (f *fakeAppBackfiller) BackfillThread(_ context.Context, id string, limit int) error {
	f.threadCalls++
	f.threadID = id
	f.threadLimit = limit
	return nil
}

func (f *fakeAppBackfiller) BackfillFollows(_ context.Context, pubkey string) error {
	f.followCalls++
	f.followPubkey = pubkey
	return nil
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

func TestUserFeedBackfillRunsWhenFirstPageShort(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
		feeds: [][]chstore.EventView{
			nil,
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "hello",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeUserBackfiller{}
	handler := New(
		store,
		WithUserFeedBackfill(backfiller),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/feed/user?pubkey="+testPubkey+"&limit=5", nil)
	handler.userFeed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.limit != 5 {
		t.Fatalf("backfill call = %+v", backfiller)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2", store.calls)
	}
	var response FeedResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 || response.Items[0].Event == nil || response.Items[0].Event.Content != "hello" {
		t.Fatalf("items = %+v", response.Items)
	}
}

func TestEventsEndpointBackfillsMissingEventsBeforeEnrichment(t *testing.T) {
	const eventID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	store := &sequencedEventStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
		events: [][]chstore.EventView{
			nil,
			{{
				ID:        eventID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "quoted",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeAppBackfiller{}
	handler := New(
		store,
		WithAppViewBackfill(backfiller),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/events?ids="+eventID, nil)
	handler.events(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if backfiller.eventCalls != 1 || len(backfiller.eventIDs) != 1 || backfiller.eventIDs[0] != eventID {
		t.Fatalf("event backfill = %+v", backfiller)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2", store.calls)
	}
	var response EnrichmentResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Quoted[eventID].Content != "quoted" {
		t.Fatalf("quoted = %+v", response.Quoted)
	}
}

func TestThreadBackfillsWhenIndexedRepliesAreShort(t *testing.T) {
	const rootID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const replyID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	root := chstore.EventView{
		ID:        rootID,
		PubKey:    testPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_000, 0),
		Content:   "root",
		Tags:      [][]string{},
		Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	reply := chstore.EventView{
		ID:        replyID,
		PubKey:    testPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_001, 0),
		Content:   "reply",
		Tags:      [][]string{{"e", rootID, "", "reply"}},
		Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
	}
	store := &sequencedThreadStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
		root:      root,
		threads: [][]chstore.EventView{
			nil,
			{reply},
		},
	}
	backfiller := &fakeAppBackfiller{}
	handler := New(
		store,
		WithAppViewBackfill(backfiller),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/thread?id="+rootID+"&limit=2", nil)
	handler.thread(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if backfiller.threadCalls != 1 || backfiller.threadID != rootID || backfiller.threadLimit != 2 {
		t.Fatalf("thread backfill = %+v", backfiller)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2", store.calls)
	}
	var response struct {
		Root   FeedEvent   `json:"root"`
		Events []FeedEvent `json:"events"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Root.ID != rootID || len(response.Events) != 1 || response.Events[0].ID != replyID {
		t.Fatalf("thread response = %+v", response)
	}
}

func TestProfilesEndpointReturnsPictureEvenWhenNameMissing(t *testing.T) {
	handler := New(
		fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:  testPubkey,
					EventID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Picture: "https://example.test/avatar.png",
				},
			},
		},
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/profiles?pubkeys="+testPubkey, nil)
	handler.profiles(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response EnrichmentResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	profile, ok := response.Profiles[testPubkey]
	if !ok || profile.Picture != "https://example.test/avatar.png" {
		t.Fatalf("profiles = %+v", response.Profiles)
	}
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
