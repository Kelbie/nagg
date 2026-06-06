package appview

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr/nip19"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/vertex"
)

const testPubkey = "82341f05fdb1dffbc78894993292171ed03abbed34a95f22f55f9b6371723ee6"

type fakeStore struct {
	profiles       map[string]chstore.ProfileRow
	counts         chstore.FollowCounts
	firstEventAt   *time.Time
	cachedVertex   vertex.ProfileResult
	cachedVertexOK bool
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

func (s fakeStore) ProfileFirstEventCreatedAt(context.Context, string) (*time.Time, error) {
	return s.firstEventAt, nil
}

func (s fakeStore) CachedVertexProfile(context.Context, string) (vertex.ProfileResult, bool, error) {
	return s.cachedVertex, s.cachedVertexOK, nil
}

func (s fakeStore) SaveVertexProfile(context.Context, vertex.ProfileResult) error {
	return nil
}

func (s fakeStore) ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	return nil, nil, nil
}

type followCountSpyStore struct {
	fakeStore
	calls   int
	pubkeys []string
}

func (s *followCountSpyStore) FollowCounts(ctx context.Context, pubkey string) (chstore.FollowCounts, error) {
	s.calls++
	s.pubkeys = append(s.pubkeys, pubkey)
	return s.fakeStore.FollowCounts(ctx, pubkey)
}

type profilePolicySpyStore struct {
	fakeStore
	cacheCalls int
	saved      []vertex.ProfileResult
}

func (s *profilePolicySpyStore) CachedVertexProfile(ctx context.Context, pubkey string) (vertex.ProfileResult, bool, error) {
	s.cacheCalls++
	return s.fakeStore.CachedVertexProfile(ctx, pubkey)
}

func (s *profilePolicySpyStore) SaveVertexProfile(_ context.Context, profile vertex.ProfileResult) error {
	s.saved = append(s.saved, profile)
	return nil
}

type sequencedFeedStore struct {
	fakeStore
	feeds         [][]chstore.EventView
	calls         int
	authors       [][]string
	trendingCalls int
}

func (s *sequencedFeedStore) FollowsFeed(_ context.Context, authors []string, _ int64, _ uint64, _ uint64) ([]chstore.EventView, error) {
	s.authors = append(s.authors, append([]string(nil), authors...))
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

func (s *sequencedFeedStore) TrendingFeed(context.Context, time.Time, uint64) ([]chstore.EventView, error) {
	s.trendingCalls++
	return nil, nil
}

type sequencedEventStore struct {
	fakeStore
	events      [][]chstore.EventView
	calls       int
	eventInputs []chstore.EventQueryInput
}

func (s *sequencedEventStore) QueryEvents(_ context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	s.eventInputs = append(s.eventInputs, input)
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

type appViewHydrationStore struct {
	fakeStore
	feed        []chstore.EventView
	events      map[string]chstore.EventView
	stats       map[string]chstore.NoteStats
	noteStatIDs []string
}

func (s *appViewHydrationStore) FollowsFeed(context.Context, []string, int64, uint64, uint64) ([]chstore.EventView, error) {
	return s.feed, nil
}

func (s *appViewHydrationStore) QueryEvents(_ context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	out := make([]chstore.EventView, 0, len(input.IDs))
	for _, id := range input.IDs {
		if event, ok := s.events[id]; ok {
			out = append(out, event)
		}
	}
	return out, nil
}

func (s *appViewHydrationStore) NoteStats(_ context.Context, ids []string) (map[string]chstore.NoteStats, error) {
	s.noteStatIDs = append([]string(nil), ids...)
	out := make(map[string]chstore.NoteStats, len(ids))
	for _, id := range ids {
		out[id] = s.stats[id]
	}
	return out, nil
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

type fakeHydratingUserBackfiller struct {
	fakeUserBackfiller
	completed bool
	hydrated  int
}

func (f *fakeHydratingUserBackfiller) HydrateUserFeed(ctx context.Context, pubkey string, limit uint64) (bool, error) {
	f.hydrated++
	return f.completed, f.BackfillUserFeed(ctx, pubkey, limit)
}

type fakeDMEnvelopeBackfiller struct {
	fakeUserBackfiller
	completed bool
	calls     int
	hydrated  int
	pubkey    string
	kinds     []int
	until     int64
	limit     uint64
}

func (f *fakeDMEnvelopeBackfiller) BackfillDMEnvelopes(_ context.Context, pubkey string, kinds []int, until int64, limit uint64) error {
	f.calls++
	f.pubkey = pubkey
	f.kinds = append([]int(nil), kinds...)
	f.until = until
	f.limit = limit
	return nil
}

func (f *fakeDMEnvelopeBackfiller) HydrateDMEnvelopes(ctx context.Context, pubkey string, kinds []int, until int64, limit uint64) (bool, error) {
	f.hydrated++
	return f.completed, f.BackfillDMEnvelopes(ctx, pubkey, kinds, until, limit)
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
	profile      vertex.ProfileResult
	profileErr   error
	profileCalls *int
	refreshCalls *int
	search       []vertex.SearchResult
}

func (v fakeVertex) Search(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, bool, error) {
	return v.search, true, nil
}

func (v fakeVertex) Recommended(context.Context, vertex.RecommendedArgs) ([]vertex.SearchResult, bool, error) {
	return v.search, true, nil
}

func (v fakeVertex) Profile(context.Context, string) (vertex.ProfileResult, bool, error) {
	if v.profileCalls != nil {
		(*v.profileCalls)++
	}
	if v.profileErr != nil {
		return vertex.ProfileResult{}, false, v.profileErr
	}
	return v.profile, true, nil
}

func (v fakeVertex) ProfileRefresh(context.Context, string) (vertex.ProfileResult, error) {
	if v.refreshCalls != nil {
		(*v.refreshCalls)++
	}
	if v.profileErr != nil {
		return vertex.ProfileResult{}, v.profileErr
	}
	return v.profile, nil
}

func TestConfiguredViewerPubkeyFallsBackForUserFeed(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
	}
	handler := New(
		store,
		WithViewerPubkey(testPubkey),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/feed/user?limit=5", nil)
	handler.userFeed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(store.authors) != 1 || len(store.authors[0]) != 1 || store.authors[0][0] != testPubkey {
		t.Fatalf("authors = %+v", store.authors)
	}
}

func TestConfiguredViewerPubkeyFallsBackForGenericFeed(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
	}
	handler := New(
		store,
		WithViewerPubkey(testPubkey),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/feed?limit=5", nil)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if store.trendingCalls != 0 {
		t.Fatalf("trending calls = %d", store.trendingCalls)
	}
	if len(store.authors) != 1 || len(store.authors[0]) != 1 || store.authors[0][0] != testPubkey {
		t.Fatalf("authors = %+v", store.authors)
	}
}

func TestConfiguredViewerPubkeyDoesNotOverrideTrendingFeedSpec(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
	}
	handler := New(
		store,
		WithViewerPubkey(testPubkey),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"trending-cluster\"}","limit":5}`),
	)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if store.trendingCalls != 1 {
		t.Fatalf("trending calls = %d", store.trendingCalls)
	}
	if len(store.authors) != 0 {
		t.Fatalf("authors = %+v", store.authors)
	}
}

func TestConfiguredViewerPubkeyFallsBackForFollows(t *testing.T) {
	store := &followCountSpyStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
	}
	handler := New(
		store,
		WithViewerPubkey(testPubkey),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/follows", nil)
	handler.follows(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(store.pubkeys) != 1 || store.pubkeys[0] != testPubkey {
		t.Fatalf("pubkeys = %+v", store.pubkeys)
	}
}

func TestExplicitInvalidPubkeyStillFailsWithViewerFallback(t *testing.T) {
	handler := New(
		fakeStore{profiles: map[string]chstore.ProfileRow{}},
		WithViewerPubkey(testPubkey),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/feed/user?pubkey=not-a-pubkey", nil)
	handler.userFeed(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
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

func TestUserFeedHydrationReturnsIndexedDataWhenHydrationIsSlow(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
		feeds: [][]chstore.EventView{
			nil,
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "eventually available",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeHydratingUserBackfiller{completed: false}
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
	if backfiller.hydrated != 1 || backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.limit != 5 {
		t.Fatalf("hydration call = %+v", backfiller)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}
	var response FeedResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 0 {
		t.Fatalf("items = %+v, want stale empty response", response.Items)
	}
}

func TestDMEnvelopesHydratesViewerInbox(t *testing.T) {
	store := &sequencedEventStore{fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}}}
	backfiller := &fakeDMEnvelopeBackfiller{}
	handler := New(
		store,
		WithUserFeedBackfill(backfiller),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/dm/envelopes?viewer="+testPubkey+"&limit=25&until=1710000000", nil)
	handler.dmEnvelopes(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if backfiller.hydrated != 1 || backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.until != 1_710_000_000 || backfiller.limit != 25 {
		t.Fatalf("dm hydration = %+v", backfiller)
	}
	if len(backfiller.kinds) != 1 || backfiller.kinds[0] != 1059 {
		t.Fatalf("dm kinds = %+v", backfiller.kinds)
	}
	if len(store.eventInputs) != 2 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	if len(store.eventInputs[0].PubKeys) != 1 || store.eventInputs[0].PubKeys[0] != testPubkey {
		t.Fatalf("authored input = %+v", store.eventInputs[0])
	}
	if len(store.eventInputs[1].Tags) != 1 || store.eventInputs[1].Tags[0].Key != "p" || store.eventInputs[1].Tags[0].Value != testPubkey {
		t.Fatalf("received input = %+v", store.eventInputs[1])
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

func TestFeedHydratesRootAndQuotedEvents(t *testing.T) {
	const rootID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const replyID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const quoteID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const nestedQuoteID = "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
	const rootPubkey = "1111111111111111111111111111111111111111111111111111111111111111"
	const quotePubkey = "2222222222222222222222222222222222222222222222222222222222222222"

	nestedQuoteNote := mustNoteURI(t, nestedQuoteID)
	root := chstore.EventView{
		ID:        rootID,
		PubKey:    rootPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_000, 0),
		Content:   "root",
		Tags:      [][]string{},
		Sig:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	reply := chstore.EventView{
		ID:        replyID,
		PubKey:    testPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_100, 0),
		Content:   "reply quoting by tag",
		Tags:      [][]string{{"e", rootID, "", "root"}, {"q", quoteID}},
		Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	quote := chstore.EventView{
		ID:        quoteID,
		PubKey:    quotePubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_050, 0),
		Content:   "quoted but nested should stay unresolved " + nestedQuoteNote,
		Tags:      [][]string{},
		Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	store := &appViewHydrationStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				rootPubkey:  {PubKey: rootPubkey, EventID: rootID, DisplayName: "Root Author"},
				testPubkey:  {PubKey: testPubkey, EventID: replyID, DisplayName: "Reply Author"},
				quotePubkey: {PubKey: quotePubkey, EventID: quoteID, DisplayName: "Quote Author"},
			},
		},
		feed:   []chstore.EventView{reply},
		events: map[string]chstore.EventView{rootID: root, quoteID: quote},
		stats: map[string]chstore.NoteStats{
			rootID:  {LikeCount: 2, RepostCount: 3, ReplyCount: 4, SatsZapped: 5},
			replyID: {LikeCount: 6, RepostCount: 7, ReplyCount: 8, SatsZapped: 9},
			quoteID: {LikeCount: 10, RepostCount: 11, ReplyCount: 12, SatsZapped: 13},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"feed\",\"pubkey\":\"`+testPubkey+`\"}","limit":10}`),
	)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response FeedResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("items = %+v", response.Items)
	}
	item := response.Items[0]
	if item.Event == nil || item.Event.ID != replyID {
		t.Fatalf("event = %+v", item.Event)
	}
	if item.RootEventID != rootID || item.RootEvent == nil || item.RootEvent.ID != rootID {
		t.Fatalf("root = id %q event %+v", item.RootEventID, item.RootEvent)
	}
	if response.Quoted[quoteID].Content != quote.Content {
		t.Fatalf("quoted = %+v", response.Quoted)
	}
	if _, ok := response.Quoted[nestedQuoteID]; ok {
		t.Fatalf("nested quote should not be hydrated: %+v", response.Quoted)
	}
	if response.Metrics[rootID].SatsZapped != 5 || response.Metrics[replyID].LikeCount != 6 || response.Metrics[quoteID].ReplyCount != 12 {
		t.Fatalf("metrics = %+v", response.Metrics)
	}
	if response.Profiles[rootPubkey].Name != "Root Author" || response.Profiles[quotePubkey].Name != "Quote Author" {
		t.Fatalf("profiles = %+v", response.Profiles)
	}
}

func TestRootEventIDUsesPositionalRootAndIgnoresMentions(t *testing.T) {
	const eventID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const mentionID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const rootID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	event := chstore.EventView{
		ID: eventID,
		Tags: [][]string{
			{"e", mentionID, "", "mention"},
			{"e", rootID},
		},
	}

	if got := rootEventID(event); got != rootID {
		t.Fatalf("rootEventID = %q, want %q", got, rootID)
	}
}

func TestFeedKeepsRootIDWhenRootUnavailable(t *testing.T) {
	const rootID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const replyID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

	reply := chstore.EventView{
		ID:        replyID,
		PubKey:    testPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_100, 0),
		Content:   "reply with missing root",
		Tags:      [][]string{{"e", rootID, "", "root"}},
		Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	store := &appViewHydrationStore{
		fakeStore: fakeStore{profiles: map[string]chstore.ProfileRow{}},
		feed:      []chstore.EventView{reply},
		events:    map[string]chstore.EventView{},
		stats:     map[string]chstore.NoteStats{replyID: {LikeCount: 1}},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"feed\",\"pubkey\":\"`+testPubkey+`\"}","limit":10}`),
	)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response FeedResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("items = %+v", response.Items)
	}
	item := response.Items[0]
	if item.RootEventID != rootID {
		t.Fatalf("root id = %q, want %q", item.RootEventID, rootID)
	}
	if item.RootEvent != nil {
		t.Fatalf("root event should be unavailable: %+v", item.RootEvent)
	}
}

func TestFeedResolvesUpstreamRootFromParentReply(t *testing.T) {
	const rootID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const parentID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const replyID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const rootPubkey = "1111111111111111111111111111111111111111111111111111111111111111"
	const parentPubkey = "2222222222222222222222222222222222222222222222222222222222222222"

	root := chstore.EventView{
		ID:        rootID,
		PubKey:    rootPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_000, 0),
		Content:   "root",
		Tags:      [][]string{},
		Sig:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	parent := chstore.EventView{
		ID:        parentID,
		PubKey:    parentPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_050, 0),
		Content:   "parent reply",
		Tags:      [][]string{{"e", rootID, "", "root"}},
		Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	reply := chstore.EventView{
		ID:        replyID,
		PubKey:    testPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_100, 0),
		Content:   "reply to parent",
		Tags:      [][]string{{"e", parentID, "", "reply"}},
		Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	store := &appViewHydrationStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				rootPubkey: {PubKey: rootPubkey, EventID: rootID, DisplayName: "Root Author"},
				testPubkey: {PubKey: testPubkey, EventID: replyID, DisplayName: "Reply Author"},
			},
		},
		feed:   []chstore.EventView{reply},
		events: map[string]chstore.EventView{rootID: root, parentID: parent},
		stats: map[string]chstore.NoteStats{
			rootID:  {LikeCount: 2, RepostCount: 3, ReplyCount: 4, SatsZapped: 5},
			replyID: {LikeCount: 6, RepostCount: 7, ReplyCount: 8, SatsZapped: 9},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"feed\",\"pubkey\":\"`+testPubkey+`\"}","limit":10}`),
	)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response FeedResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("items = %+v", response.Items)
	}
	item := response.Items[0]
	if item.RootEventID != rootID || item.RootEvent == nil || item.RootEvent.ID != rootID {
		t.Fatalf("root = id %q event %+v", item.RootEventID, item.RootEvent)
	}
	if _, ok := response.Metrics[parentID]; ok {
		t.Fatalf("parent should not be hydrated as displayed root: %+v", response.Metrics)
	}
	if response.Metrics[rootID].SatsZapped != 5 || response.Profiles[rootPubkey].Name != "Root Author" {
		t.Fatalf("hydration metrics=%+v profiles=%+v", response.Metrics, response.Profiles)
	}
}

func TestFeedHydratesRepostOriginalRoot(t *testing.T) {
	const rootID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const originalID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const repostID = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	const rootPubkey = "1111111111111111111111111111111111111111111111111111111111111111"
	const originalPubkey = "2222222222222222222222222222222222222222222222222222222222222222"

	root := chstore.EventView{
		ID:        rootID,
		PubKey:    rootPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_000, 0),
		Content:   "root",
		Tags:      [][]string{},
		Sig:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
	}
	original := chstore.EventView{
		ID:        originalID,
		PubKey:    originalPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_050, 0),
		Content:   "reply original",
		Tags:      [][]string{{"e", rootID, "", "root"}},
		Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
	repost := chstore.EventView{
		ID:        repostID,
		PubKey:    testPubkey,
		Kind:      6,
		CreatedAt: time.Unix(1_710_000_100, 0),
		Content:   "",
		Tags:      [][]string{{"e", originalID}},
		Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
	}
	store := &appViewHydrationStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				rootPubkey:     {PubKey: rootPubkey, EventID: rootID, DisplayName: "Root Author"},
				originalPubkey: {PubKey: originalPubkey, EventID: originalID, DisplayName: "Original Author"},
				testPubkey:     {PubKey: testPubkey, EventID: repostID, DisplayName: "Reposter"},
			},
		},
		feed:   []chstore.EventView{repost},
		events: map[string]chstore.EventView{rootID: root, originalID: original},
		stats: map[string]chstore.NoteStats{
			rootID:     {LikeCount: 2, RepostCount: 3, ReplyCount: 4, SatsZapped: 5},
			originalID: {LikeCount: 6, RepostCount: 7, ReplyCount: 8, SatsZapped: 9},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"feed\",\"pubkey\":\"`+testPubkey+`\"}","limit":10}`),
	)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response FeedResponse
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Items) != 1 {
		t.Fatalf("items = %+v", response.Items)
	}
	item := response.Items[0]
	if item.Type != "repost" || item.OriginalEvent == nil || item.OriginalEvent.ID != originalID {
		t.Fatalf("repost item = %+v", item)
	}
	if item.RootEventID != rootID || item.RootEvent == nil || item.RootEvent.ID != rootID {
		t.Fatalf("root = id %q event %+v", item.RootEventID, item.RootEvent)
	}
	if response.Metrics[rootID].SatsZapped != 5 || response.Metrics[originalID].LikeCount != 6 {
		t.Fatalf("metrics = %+v", response.Metrics)
	}
}

func mustNoteURI(t *testing.T, id string) string {
	t.Helper()
	note, err := nip19.EncodeNote(id)
	if err != nil {
		t.Fatal(err)
	}
	return "nostr:" + note
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

func TestProfileSkipsVertexBelowLocalFollowerThreshold(t *testing.T) {
	score := 42.5
	localCreatedAt := time.Unix(1_710_000_000, 0)
	firstEventAt := time.Unix(1_600_000_000, 0)
	vertexCreatedAt := int64(1_720_000_000)
	refreshCalls := 0
	store := &profilePolicySpyStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:      testPubkey,
					CreatedAt:   localCreatedAt,
					Name:        "sovran",
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
				},
			},
			counts:       chstore.FollowCounts{Follows: 7, Followers: 499},
			firstEventAt: &firstEventAt,
			cachedVertex: vertex.ProfileResult{
				PubKey:    testPubkey,
				Npub:      vertex.Npub(testPubkey),
				Rank:      0.99,
				Score:     &score,
				CreatedAt: &vertexCreatedAt,
			},
			cachedVertexOK: true,
		},
	}
	handler := New(
		store,
		WithVertex(fakeVertex{
			profile: vertex.ProfileResult{
				PubKey:    testPubkey,
				Npub:      vertex.Npub(testPubkey),
				Rank:      0.01,
				Score:     &score,
				CreatedAt: &vertexCreatedAt,
			},
			refreshCalls: &refreshCalls,
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
	if refreshCalls != 0 {
		t.Fatalf("vertex refresh calls = %d, want 0", refreshCalls)
	}
	if store.cacheCalls != 0 {
		t.Fatalf("cache calls = %d, want 0", store.cacheCalls)
	}
	if response.Score != nil || response.Rank != 0 || response.FromCache {
		t.Fatalf("vertex fields leaked into low-follower response: %+v", response)
	}
	if response.Followers != 499 || response.Follows != 7 {
		t.Fatalf("counts = followers %d follows %d", response.Followers, response.Follows)
	}
	if response.CreatedAt == nil || *response.CreatedAt != firstEventAt.Unix() {
		t.Fatalf("created_at = %v", response.CreatedAt)
	}
}

func TestProfileRefreshesAndSavesVertexAtFollowerThreshold(t *testing.T) {
	score := 42.5
	nodes := 1000
	firstEventAt := time.Unix(1_600_000_000, 0)
	vertexCreatedAt := int64(1_720_000_000)
	vertexFollowers := uint64(10)
	vertexFollows := uint64(3)
	refreshCalls := 0
	store := &profilePolicySpyStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:      testPubkey,
					Name:        "sovran",
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
				},
			},
			counts:       chstore.FollowCounts{Follows: 30, Followers: 500},
			firstEventAt: &firstEventAt,
		},
	}
	handler := New(
		store,
		WithVertex(fakeVertex{
			profile: vertex.ProfileResult{
				PubKey:    testPubkey,
				Npub:      vertex.Npub(testPubkey),
				Rank:      0.01,
				Score:     &score,
				Nodes:     &nodes,
				Followers: &vertexFollowers,
				Follows:   &vertexFollows,
				CreatedAt: &vertexCreatedAt,
			},
			refreshCalls: &refreshCalls,
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
	if refreshCalls != 1 {
		t.Fatalf("vertex refresh calls = %d, want 1", refreshCalls)
	}
	if len(store.saved) != 1 || store.saved[0].PubKey != testPubkey {
		t.Fatalf("saved profiles = %+v", store.saved)
	}
	if response.FromCache {
		t.Fatal("fresh vertex response should not be marked from cache")
	}
	if response.Followers != 500 || response.Follows != 30 {
		t.Fatalf("counts = followers %d follows %d", response.Followers, response.Follows)
	}
	if response.Score == nil || *response.Score != score {
		t.Fatalf("score = %v", response.Score)
	}
	if response.Nodes == nil || *response.Nodes != nodes {
		t.Fatalf("nodes = %v", response.Nodes)
	}
	if response.CreatedAt == nil || *response.CreatedAt != firstEventAt.Unix() {
		t.Fatalf("created_at = %v", response.CreatedAt)
	}
}

func TestProfileFallsBackToLocalCreatedAtWhenVertexMissing(t *testing.T) {
	createdAt := time.Unix(1_710_000_000, 0)
	handler := New(
		fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:    testPubkey,
					CreatedAt: createdAt,
				},
			},
		},
		WithVertex(fakeVertex{
			profile: vertex.ProfileResult{
				PubKey: testPubkey,
				Npub:   vertex.Npub(testPubkey),
				Rank:   0.01,
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
	if response.CreatedAt == nil || *response.CreatedAt != createdAt.Unix() {
		t.Fatalf("created_at = %v", response.CreatedAt)
	}
}

func TestProfileFallsBackToCachedVertexProfileWhenRefreshFails(t *testing.T) {
	score := 55.5
	refreshCalls := 0
	store := &profilePolicySpyStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:      testPubkey,
					Name:        "sovran",
					DisplayName: "Sovran",
				},
			},
			counts: chstore.FollowCounts{Follows: 30, Followers: 500},
			cachedVertex: vertex.ProfileResult{
				PubKey: testPubkey,
				Npub:   vertex.Npub(testPubkey),
				Rank:   0.01,
				Score:  &score,
			},
			cachedVertexOK: true,
		},
	}
	handler := New(
		store,
		WithVertex(fakeVertex{
			profileErr:   errors.New("vertex unavailable"),
			refreshCalls: &refreshCalls,
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
	if refreshCalls != 1 {
		t.Fatalf("vertex refresh calls = %d, want 1", refreshCalls)
	}
	if store.cacheCalls != 1 {
		t.Fatalf("cache calls = %d, want 1", store.cacheCalls)
	}
	if len(store.saved) != 0 {
		t.Fatalf("saved profiles = %+v, want none", store.saved)
	}
	if !response.FromCache {
		t.Fatal("expected cached vertex fallback")
	}
	if response.Score == nil || *response.Score != score {
		t.Fatalf("score = %v", response.Score)
	}
	if response.Followers != 500 || response.Follows != 30 {
		t.Fatalf("counts = followers %d follows %d", response.Followers, response.Follows)
	}
}

func TestProfileReturnsLocalOnlyWhenVertexAndCacheMiss(t *testing.T) {
	refreshCalls := 0
	store := &profilePolicySpyStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.ProfileRow{
				testPubkey: {
					PubKey:    testPubkey,
					CreatedAt: time.Unix(1_710_000_000, 0),
				},
			},
			counts: chstore.FollowCounts{Follows: 30, Followers: 500},
		},
	}
	handler := New(
		store,
		WithVertex(fakeVertex{
			profileErr:   errors.New("vertex unavailable"),
			refreshCalls: &refreshCalls,
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
	if refreshCalls != 1 {
		t.Fatalf("vertex refresh calls = %d, want 1", refreshCalls)
	}
	if store.cacheCalls != 1 {
		t.Fatalf("cache calls = %d, want 1", store.cacheCalls)
	}
	if response.FromCache || response.Score != nil || response.Rank != 0 {
		t.Fatalf("expected local-only response, got %+v", response)
	}
	if response.Followers != 500 || response.Follows != 30 {
		t.Fatalf("counts = followers %d follows %d", response.Followers, response.Follows)
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

func TestParseDmKindsDefaultsToNip04AndNip17(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want []int
	}{
		{name: "empty defaults to kind 4 + 1059", raw: "", want: []int{4, 1059}},
		{name: "whitespace defaults", raw: "  ", want: []int{4, 1059}},
		{name: "explicit single kind honored", raw: "1059", want: []int{1059}},
		{name: "explicit csv honored", raw: "4,1059", want: []int{4, 1059}},
		{name: "non-numeric tokens skipped", raw: "4,foo,1059", want: []int{4, 1059}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := parseDmKinds(tc.raw)
			if len(got) != len(tc.want) {
				t.Fatalf("parseDmKinds(%q) = %v, want %v", tc.raw, got, tc.want)
			}
			for i := range tc.want {
				if got[i] != tc.want[i] {
					t.Fatalf("parseDmKinds(%q) = %v, want %v", tc.raw, got, tc.want)
				}
			}
		})
	}
}
