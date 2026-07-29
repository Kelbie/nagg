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

const fakeFollowedAuthor = "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"

type fakeStore struct {
	followsByViewer map[string]map[string]struct{}
	profiles        map[string]chstore.K0Row
	counts          chstore.PubkeyStats
	firstEventAt    *time.Time
	cachedVertex    vertex.ProfileResult
	cachedVertexOK  bool
	profileSearch   []chstore.ProfileSearchRow
}

func (s fakeStore) SearchK0(context.Context, string, uint64) ([]chstore.ProfileSearchRow, error) {
	return s.profileSearch, nil
}

func (s fakeStore) FollowsFeed(context.Context, []string, int64, uint64, uint64) ([]chstore.EventView, error) {
	return nil, nil
}

func (s fakeStore) QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error) {
	return nil, nil
}

func (s fakeStore) EventAggregates(context.Context, []string) (map[string]map[string]map[string]uint64, error) {
	return map[string]map[string]map[string]uint64{}, nil
}

// followsByViewer lets tests declare each viewer's latest kind-3 reference
// set; a nil map means "follows one placeholder author" so viewer-anchored
// feed tests still resolve a non-empty author set.
func (s fakeStore) LatestK3Refs(_ context.Context, pubkeys []string) (map[string]map[string]struct{}, error) {
	out := make(map[string]map[string]struct{}, len(pubkeys))
	for _, pk := range pubkeys {
		if s.followsByViewer != nil {
			out[pk] = s.followsByViewer[pk]
			continue
		}
		out[pk] = map[string]struct{}{fakeFollowedAuthor: {}}
	}
	return out, nil
}

func (s fakeStore) LatestK0(_ context.Context, pubkeys []string) (map[string]chstore.K0Row, error) {
	out := make(map[string]chstore.K0Row, len(pubkeys))
	for _, pubkey := range pubkeys {
		if profile, ok := s.profiles[pubkey]; ok {
			out[pubkey] = profile
		}
	}
	return out, nil
}

func (s fakeStore) PubkeyStats(context.Context, string) (chstore.PubkeyStats, error) {
	return s.counts, nil
}

func (s fakeStore) ProfileFirstEventCreatedAt(context.Context, string) (*time.Time, error) {
	return s.firstEventAt, nil
}

func (s fakeStore) CachedVertexProfile(context.Context, string) (vertex.ProfileResult, bool, error) {
	return s.cachedVertex, s.cachedVertexOK, nil
}

func (s fakeStore) CachedVertexProfiles(_ context.Context, pubkeys []string) (map[string]vertex.ProfileResult, error) {
	out := make(map[string]vertex.ProfileResult, len(pubkeys))
	if s.cachedVertexOK {
		for _, pubkey := range pubkeys {
			out[pubkey] = s.cachedVertex
		}
	}
	return out, nil
}

func (s fakeStore) SaveVertexProfile(context.Context, vertex.ProfileResult) error {
	return nil
}

func (s fakeStore) DescendantEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	return nil, nil, nil
}

func (s fakeStore) ViewerFeed(context.Context, chstore.ViewerFeedInput) ([]chstore.ViewerFeedRow, error) {
	return nil, nil
}

func (s fakeStore) BatchPubkeyStats(_ context.Context, pubkeys []string) (map[string]chstore.PubkeyStats, error) {
	out := make(map[string]chstore.PubkeyStats, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = s.counts
	}
	return out, nil
}

func (s fakeStore) FollowEdges(_ context.Context, _ string, candidates []string) (map[string]chstore.FollowEdge, error) {
	out := make(map[string]chstore.FollowEdge, len(candidates))
	for _, candidate := range candidates {
		out[candidate] = chstore.FollowEdge{}
	}
	return out, nil
}

func (s fakeStore) RankedRefSources(context.Context, string, string, int, int) ([]string, error) {
	return nil, nil
}

func (s fakeStore) AuthoredRefSources(context.Context, string, string, int) ([]string, error) {
	return nil, nil
}

func (s fakeStore) FollowedRefs(context.Context, string, []string) (map[string]string, error) {
	return map[string]string{}, nil
}

type followCountSpyStore struct {
	fakeStore
	calls   int
	pubkeys []string
}

func (s *followCountSpyStore) PubkeyStats(ctx context.Context, pubkey string) (chstore.PubkeyStats, error) {
	s.calls++
	s.pubkeys = append(s.pubkeys, pubkey)
	return s.fakeStore.PubkeyStats(ctx, pubkey)
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
	feeds   [][]chstore.EventView
	calls   int
	authors [][]string
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

func (s *sequencedThreadStore) DescendantEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	idx := s.calls
	if idx >= len(s.threads) {
		idx = len(s.threads) - 1
	}
	s.calls++
	return &s.root, s.threads[idx], nil
}

// testCounts is the fixture shape for per-event counts; tests express the
// values in familiar terms and noteStatsAsAggregates maps them to rule names.
type testCounts struct {
	LikeCount   uint64
	RepostCount uint64
	ReplyCount  uint64
	SatsZapped  uint64
}

type appViewHydrationStore struct {
	fakeStore
	feed        []chstore.EventView
	events      map[string]chstore.EventView
	stats       map[string]testCounts
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

func (s *appViewHydrationStore) EventAggregates(_ context.Context, ids []string) (map[string]map[string]map[string]uint64, error) {
	s.noteStatIDs = append([]string(nil), ids...)
	out := make(map[string]map[string]map[string]uint64, len(ids))
	for _, id := range ids {
		if agg := noteStatsAsAggregates(s.stats[id]); len(agg) > 0 {
			out[id] = agg
		}
	}
	return out, nil
}

// noteStatsAsAggregates maps the legacy fixture counts onto the declared rule
// vocabulary, omitting zeros exactly like the real EventAggregates read.
func noteStatsAsAggregates(st testCounts) map[string]map[string]uint64 {
	out := map[string]map[string]uint64{}
	put := func(rule, metric string, v uint64) {
		if v == 0 {
			return
		}
		if out[rule] == nil {
			out[rule] = map[string]uint64{}
		}
		out[rule][metric] = v
	}
	put("k7_e", "actors", st.LikeCount)
	put("k6_16_e", "actors", st.RepostCount)
	put("k1_1111_e_reply", "sources", st.ReplyCount)
	put("k9735_e", "value_total", st.SatsZapped)
	return out
}

// envelopeEvents indexes an envelope's embedded events by id.
func envelopeEvents(env Envelope) map[string]FeedEvent {
	out := make(map[string]FeedEvent, len(env.Events))
	for _, event := range env.Events {
		out[event.ID] = event
	}
	return out
}

// envelopeProfile returns the kind-0 event for a pubkey, if embedded.
func envelopeProfile(env Envelope, pubkey string) (FeedEvent, bool) {
	for _, event := range env.Events {
		if event.Kind == 0 && event.PubKey == pubkey {
			return event, true
		}
	}
	return FeedEvent{}, false
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
	profile        vertex.ProfileResult
	profileErr     error
	profileCalls   *int
	refreshCalls   *int
	search         []vertex.SearchResult
	searchErr      error
	lastSearchArgs *vertex.SearchArgs
}

func (v fakeVertex) Search(_ context.Context, args vertex.SearchArgs) ([]vertex.SearchResult, bool, error) {
	if v.lastSearchArgs != nil {
		*v.lastSearchArgs = args
	}
	if v.searchErr != nil {
		return nil, false, v.searchErr
	}
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
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
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
	// A viewer-anchored feed (no explicit pubkeys) serves the authors the
	// viewer's latest kind-3 references — never the viewer's own events.
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
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
	if len(store.authors) != 1 || len(store.authors[0]) != 1 || store.authors[0][0] != fakeFollowedAuthor {
		t.Fatalf("authors = %+v, want the viewer's referenced authors, not the viewer", store.authors)
	}
}

func TestViewerAnchoredFeedServesReferencedAuthors(t *testing.T) {
	// POST spec with only a viewer pubkey: the author set is the viewer's
	// latest kind-3 reference set (the reported bug: it used to be the
	// viewer themselves, so "Following" showed the viewer's own events).
	followed := map[string]struct{}{
		"1111111111111111111111111111111111111111111111111111111111111111": {},
		"2222222222222222222222222222222222222222222222222222222222222222": {},
	}
	store := &sequencedFeedStore{
		fakeStore: fakeStore{
			profiles:        map[string]chstore.K0Row{},
			followsByViewer: map[string]map[string]struct{}{testPubkey: followed},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"following-recent\",\"pubkey\":\"`+testPubkey+`\"}","limit":5}`),
	)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(store.authors) != 1 || len(store.authors[0]) != 2 {
		t.Fatalf("authors = %+v, want the 2 referenced authors", store.authors)
	}
	for _, author := range store.authors[0] {
		if _, ok := followed[author]; !ok {
			t.Fatalf("unexpected author %q (viewer leaked into the author set?)", author)
		}
	}
}

func TestFeedWithoutAuthorsReturnsEmpty(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
	}
	handler := New(
		store,
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodPost,
		"/nostr/feed",
		strings.NewReader(`{"spec":"{\"id\":\"some-feed\"}","limit":5}`),
	)
	handler.feed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if len(store.authors) != 0 {
		t.Fatalf("authors = %+v", store.authors)
	}
}

func TestConfiguredViewerPubkeyFallsBackForFollows(t *testing.T) {
	store := &followCountSpyStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
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
		fakeStore{profiles: map[string]chstore.K0Row{}},
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
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	events := envelopeEvents(response)
	if len(response.Order) != 1 || events[response.Order[0]].Content != "hello" {
		t.Fatalf("order = %v events = %v", response.Order, response.Events)
	}
}

func TestUserFeedHydrationReturnsIndexedDataWhenHydrationIsSlow(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Order) != 0 {
		t.Fatalf("order = %v, want stale empty response", response.Order)
	}
}

func TestUserFeedWaitsThenReturnsBackfilledPosts(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
		feeds: [][]chstore.EventView{
			nil,
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "backfilled",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	// completed=true models the synchronous UserFeedWait latching onto the
	// finished backfill job, so the handler re-queries and serves the posts.
	backfiller := &fakeHydratingUserBackfiller{completed: true}
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
	if backfiller.hydrated != 1 {
		t.Fatalf("hydrated = %d, want 1", backfiller.hydrated)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2 (cold + requery)", store.calls)
	}
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	events := envelopeEvents(response)
	if len(response.Order) != 1 || events[response.Order[0]].Content != "backfilled" {
		t.Fatalf("order = %v events = %v, want the backfilled post", response.Order, response.Events)
	}
}

func TestUserFeedWarmSkipsBackfill(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
		feeds: [][]chstore.EventView{
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "already indexed",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeHydratingUserBackfiller{completed: true}
	handler := New(
		store,
		WithUserFeedBackfill(backfiller),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	// limit=1 and one indexed event: the first page is full, so it is warm.
	req := httptest.NewRequest(http.MethodGet, "/nostr/feed/user?pubkey="+testPubkey+"&limit=1", nil)
	handler.userFeed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if backfiller.hydrated != 0 || backfiller.calls != 0 {
		t.Fatalf("warm profile must not backfill: %+v", backfiller)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (no requery for a warm profile)", store.calls)
	}
}

func TestUserFeedPaginationSkipsBackfill(t *testing.T) {
	store := &sequencedFeedStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
		feeds:     [][]chstore.EventView{nil},
	}
	backfiller := &fakeHydratingUserBackfiller{completed: true}
	handler := New(
		store,
		WithUserFeedBackfill(backfiller),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	// A paginated request (until>0) is a deliberate older-page fetch, never a
	// cold first load, so it must not trigger relay backfill even when empty.
	req := httptest.NewRequest(http.MethodGet, "/nostr/feed/user?pubkey="+testPubkey+"&limit=5&until=1700000000", nil)
	handler.userFeed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if backfiller.hydrated != 0 || backfiller.calls != 0 {
		t.Fatalf("paginated request must not backfill: %+v", backfiller)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1 (no requery on pagination)", store.calls)
	}
}

func TestDMEnvelopesHydratesViewerInbox(t *testing.T) {
	store := &sequencedEventStore{fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}}}
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
	if len(backfiller.kinds) != 2 || backfiller.kinds[0] != 4 || backfiller.kinds[1] != 1059 {
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

// TestDMEnvelopesReturnsEncryptedContentVerbatim asserts nagg is zero-knowledge
// for DMs: the envelopes path returns the raw encrypted Content exactly as it
// came from the store, without any decryption or rewriting. The client is the
// only party that holds the key and decrypts.
func TestDMEnvelopesReturnsEncryptedContentVerbatim(t *testing.T) {
	const ciphertext = "AdseMQ8a8b1cKpQ9z3==?iv=Z3VhcmRfdGVzdF9pdg==" // opaque NIP-04 ciphertext
	envelope := chstore.EventView{
		ID:        strings.Repeat("c", 64),
		PubKey:    testPubkey,
		Kind:      4,
		CreatedAt: time.Unix(1_710_000_000, 0),
		Content:   ciphertext,
		Tags:      [][]string{{"p", testPubkey}},
		Sig:       strings.Repeat("c", 128),
	}
	store := &sequencedEventStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
		// First call is the authored query, second is the received query.
		events: [][]chstore.EventView{{envelope}, nil},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/dm/envelopes?viewer="+testPubkey+"&limit=25", nil)
	handler.dmEnvelopes(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response Envelope
	if err := json.Unmarshal(rec.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if len(response.Events) != 1 {
		t.Fatalf("events = %+v, want exactly one", response.Events)
	}
	if got := response.Events[0].Content; got != ciphertext {
		t.Fatalf("envelope content = %q, want encrypted ciphertext verbatim %q", got, ciphertext)
	}
	if response.Cursor == nil {
		t.Fatalf("cursor = nil, want a cursor for a non-empty page")
	}
	// PRIVACY: DM responses must never enrich — no aggregates, no profiles.
	if len(response.Aggregates) != 0 {
		t.Fatalf("aggregates = %+v, want empty on the DM path", response.Aggregates)
	}
	for _, event := range response.Events {
		if event.Kind == 0 {
			t.Fatalf("profile hydration leaked into the DM path: %+v", event)
		}
	}
}

func TestEventsEndpointBackfillsMissingEventsBeforeEnrichment(t *testing.T) {
	const eventID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	store := &sequencedEventStore{
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	events := envelopeEvents(response)
	if len(response.Order) != 1 || response.Order[0] != eventID || events[eventID].Content != "quoted" {
		t.Fatalf("order = %v events = %v", response.Order, response.Events)
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
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Order) != 2 || response.Order[0] != rootID || response.Order[1] != replyID {
		t.Fatalf("order = %v, want [root reply]", response.Order)
	}
	events := envelopeEvents(response)
	if _, ok := events[rootID]; !ok {
		t.Fatalf("root missing from events: %v", response.Events)
	}
	if _, ok := events[replyID]; !ok {
		t.Fatalf("reply missing from events: %v", response.Events)
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
			profiles: map[string]chstore.K0Row{
				rootPubkey:  {PubKey: rootPubkey, EventID: strings.Repeat("e", 64), CreatedAt: time.Unix(1_700_000_000, 0), DisplayName: "Root Author", RawJSON: `{"display_name":"Root Author"}`},
				testPubkey:  {PubKey: testPubkey, EventID: strings.Repeat("f", 64), CreatedAt: time.Unix(1_700_000_001, 0), DisplayName: "Reply Author", RawJSON: `{"display_name":"Reply Author"}`},
				quotePubkey: {PubKey: quotePubkey, EventID: strings.Repeat("0", 63) + "1", CreatedAt: time.Unix(1_700_000_002, 0), DisplayName: "Quote Author", RawJSON: `{"display_name":"Quote Author"}`},
			},
		},
		feed:   []chstore.EventView{reply},
		events: map[string]chstore.EventView{rootID: root, quoteID: quote},
		stats: map[string]testCounts{
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Order) != 1 || response.Order[0] != replyID {
		t.Fatalf("order = %v", response.Order)
	}
	events := envelopeEvents(response)
	if _, ok := events[replyID]; !ok {
		t.Fatalf("reply missing from events: %v", response.Events)
	}
	if _, ok := events[rootID]; !ok {
		t.Fatalf("resolved root must be embedded: %v", response.Events)
	}
	if events[quoteID].Content != quote.Content {
		t.Fatalf("quoted event missing: %+v", events[quoteID])
	}
	if _, ok := events[nestedQuoteID]; ok {
		t.Fatalf("nested quote should not be hydrated: %v", response.Events)
	}
	if response.Aggregates[rootID]["k9735_e"]["value_total"] != 5 ||
		response.Aggregates[replyID]["k7_e"]["actors"] != 6 ||
		response.Aggregates[quoteID]["k1_1111_e_reply"]["sources"] != 12 {
		t.Fatalf("aggregates = %+v", response.Aggregates)
	}
	if profile, ok := envelopeProfile(response, rootPubkey); !ok || !strings.Contains(profile.Content, "Root Author") {
		t.Fatalf("root author profile event missing: %v", response.Events)
	}
	if profile, ok := envelopeProfile(response, quotePubkey); !ok || !strings.Contains(profile.Content, "Quote Author") {
		t.Fatalf("quote author profile event missing: %v", response.Events)
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
		fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
		feed:      []chstore.EventView{reply},
		events:    map[string]chstore.EventView{},
		stats:     map[string]testCounts{replyID: {LikeCount: 1}},
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Order) != 1 || response.Order[0] != replyID {
		t.Fatalf("order = %v", response.Order)
	}
	events := envelopeEvents(response)
	if _, ok := events[replyID]; !ok {
		t.Fatalf("reply missing from events: %v", response.Events)
	}
	// The root is unavailable, so it cannot be embedded; the client derives
	// the pointer from the reply's own root marker tag.
	if _, ok := events[rootID]; ok {
		t.Fatalf("unavailable root must not be embedded: %v", response.Events)
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
			profiles: map[string]chstore.K0Row{
				rootPubkey: {PubKey: rootPubkey, EventID: strings.Repeat("e", 64), CreatedAt: time.Unix(1_700_000_000, 0), DisplayName: "Root Author", RawJSON: `{"display_name":"Root Author"}`},
				testPubkey: {PubKey: testPubkey, EventID: strings.Repeat("f", 64), CreatedAt: time.Unix(1_700_000_001, 0), DisplayName: "Reply Author", RawJSON: `{"display_name":"Reply Author"}`},
			},
		},
		feed:   []chstore.EventView{reply},
		events: map[string]chstore.EventView{rootID: root, parentID: parent},
		stats: map[string]testCounts{
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Order) != 1 || response.Order[0] != replyID {
		t.Fatalf("order = %v", response.Order)
	}
	events := envelopeEvents(response)
	if _, ok := events[rootID]; !ok {
		t.Fatalf("upstream root must be embedded: %v", response.Events)
	}
	if _, ok := events[parentID]; ok {
		t.Fatalf("intermediate parent should not be embedded as displayed root: %v", response.Events)
	}
	if response.Aggregates[rootID]["k9735_e"]["value_total"] != 5 {
		t.Fatalf("aggregates = %+v", response.Aggregates)
	}
	if profile, ok := envelopeProfile(response, rootPubkey); !ok || !strings.Contains(profile.Content, "Root Author") {
		t.Fatalf("root author profile event missing: %v", response.Events)
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
			profiles: map[string]chstore.K0Row{
				rootPubkey:     {PubKey: rootPubkey, EventID: strings.Repeat("e", 64), CreatedAt: time.Unix(1_700_000_000, 0), DisplayName: "Root Author", RawJSON: `{"display_name":"Root Author"}`},
				originalPubkey: {PubKey: originalPubkey, EventID: strings.Repeat("f", 64), CreatedAt: time.Unix(1_700_000_001, 0), DisplayName: "Original Author", RawJSON: `{"display_name":"Original Author"}`},
				testPubkey:     {PubKey: testPubkey, EventID: strings.Repeat("0", 63) + "1", CreatedAt: time.Unix(1_700_000_002, 0), DisplayName: "Reposter", RawJSON: `{"display_name":"Reposter"}`},
			},
		},
		feed:   []chstore.EventView{repost},
		events: map[string]chstore.EventView{rootID: root, originalID: original},
		stats: map[string]testCounts{
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	// A kind-6 entry anchors on the referenced (original) event id.
	if len(response.Order) != 1 || response.Order[0] != originalID {
		t.Fatalf("order = %v, want anchored on the original", response.Order)
	}
	events := envelopeEvents(response)
	if _, ok := events[repostID]; !ok {
		t.Fatalf("repost event missing: %v", response.Events)
	}
	if _, ok := events[originalID]; !ok {
		t.Fatalf("original event missing: %v", response.Events)
	}
	if _, ok := events[rootID]; !ok {
		t.Fatalf("original's root missing: %v", response.Events)
	}
	if response.Aggregates[rootID]["k9735_e"]["value_total"] != 5 ||
		response.Aggregates[originalID]["k7_e"]["actors"] != 6 {
		t.Fatalf("aggregates = %+v", response.Aggregates)
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
			profiles: map[string]chstore.K0Row{
				testPubkey: {
					PubKey:  testPubkey,
					EventID: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
					Picture: "https://example.test/avatar.png",
					RawJSON: `{"picture":"https://example.test/avatar.png"}`,
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
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Events) != 1 || response.Events[0].Kind != 0 {
		t.Fatalf("events = %+v, want one kind-0", response.Events)
	}
	if !strings.Contains(response.Events[0].Content, "avatar.png") {
		t.Fatalf("profile event content = %q", response.Events[0].Content)
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
			profiles: map[string]chstore.K0Row{
				testPubkey: {
					PubKey:      testPubkey,
					CreatedAt:   localCreatedAt,
					Name:        "sovran",
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
				},
			},
			counts:       chstore.PubkeyStats{Follows: 7, Followers: 499},
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
	var response ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if refreshCalls != 0 {
		t.Fatalf("vertex refresh calls = %d, want 0", refreshCalls)
	}
	if store.cacheCalls != 0 {
		t.Fatalf("cache calls = %d, want 0", store.cacheCalls)
	}
	if v := vertexOf(response, testPubkey); v != nil && (v["score"] != nil || v["rank"] != float64(0)) {
		t.Fatalf("vertex fields leaked into low-follower response: %+v", v)
	}
	if response.FromCache {
		t.Fatal("fromCache leaked into low-follower response")
	}
	followers, follows := followerCounts(response, testPubkey)
	if followers != 499 || follows != 7 {
		t.Fatalf("counts = followers %d follows %d", followers, follows)
	}
	if got := firstEventAtOf(response, testPubkey); got == nil || *got != firstEventAt.Unix() {
		t.Fatalf("firstEventAt = %v", got)
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
			profiles: map[string]chstore.K0Row{
				testPubkey: {
					PubKey:      testPubkey,
					Name:        "sovran",
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
				},
			},
			counts:       chstore.PubkeyStats{Follows: 30, Followers: 500},
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
	var response ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Pubkeys) != 1 || response.Pubkeys[0] != testPubkey {
		t.Fatalf("pubkeys = %v", response.Pubkeys)
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
	followers, follows := followerCounts(response, testPubkey)
	if followers != 500 || follows != 30 {
		t.Fatalf("counts = followers %d follows %d", followers, follows)
	}
	v := vertexOf(response, testPubkey)
	if v == nil || v["score"] != score {
		t.Fatalf("vertex score = %+v", v)
	}
	if v["nodes"] != float64(nodes) {
		t.Fatalf("vertex nodes = %v", v["nodes"])
	}
	if got := firstEventAtOf(response, testPubkey); got == nil || *got != firstEventAt.Unix() {
		t.Fatalf("firstEventAt = %v", got)
	}
}

func TestProfileFallsBackToLocalCreatedAtWhenVertexMissing(t *testing.T) {
	createdAt := time.Unix(1_710_000_000, 0)
	handler := New(
		fakeStore{
			profiles: map[string]chstore.K0Row{
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
	var response ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if got := firstEventAtOf(response, testPubkey); got == nil || *got != createdAt.Unix() {
		t.Fatalf("firstEventAt = %v", got)
	}
}

func TestProfileFallsBackToCachedVertexProfileWhenRefreshFails(t *testing.T) {
	score := 55.5
	refreshCalls := 0
	store := &profilePolicySpyStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.K0Row{
				testPubkey: {
					PubKey:      testPubkey,
					Name:        "sovran",
					DisplayName: "Sovran",
				},
			},
			counts: chstore.PubkeyStats{Follows: 30, Followers: 500},
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
	var response ProvidersEnvelope
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
	if v := vertexOf(response, testPubkey); v == nil || v["score"] != score {
		t.Fatalf("vertex payload = %+v", v)
	}
	followers, follows := followerCounts(response, testPubkey)
	if followers != 500 || follows != 30 {
		t.Fatalf("counts = followers %d follows %d", followers, follows)
	}
}

func TestProfileReturnsLocalOnlyWhenVertexAndCacheMiss(t *testing.T) {
	refreshCalls := 0
	store := &profilePolicySpyStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.K0Row{
				testPubkey: {
					PubKey:    testPubkey,
					CreatedAt: time.Unix(1_710_000_000, 0),
				},
			},
			counts: chstore.PubkeyStats{Follows: 30, Followers: 500},
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
	var response ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if refreshCalls != 1 {
		t.Fatalf("vertex refresh calls = %d, want 1", refreshCalls)
	}
	if store.cacheCalls != 1 {
		t.Fatalf("cache calls = %d, want 1", store.cacheCalls)
	}
	if v := vertexOf(response, testPubkey); response.FromCache || (v != nil && (v["score"] != nil || v["rank"] != float64(0))) {
		t.Fatalf("expected local-only response, got fromCache=%v vertex=%+v", response.FromCache, v)
	}
	followers, follows := followerCounts(response, testPubkey)
	if followers != 500 || follows != 30 {
		t.Fatalf("counts = followers %d follows %d", followers, follows)
	}
}

func TestSearchEnrichesRowsWithLocalProfiles(t *testing.T) {
	rank := 0.01
	score := 42.5
	handler := New(
		fakeStore{
			profiles: map[string]chstore.K0Row{
				testPubkey: {
					PubKey:      testPubkey,
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
					EventID:     strings.Repeat("f", 64),
					RawJSON:     `{"display_name":"Sovran"}`,
				},
			},
		},
		WithProfileSearch(fakeVertex{
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
	var response ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if !response.FromCache {
		t.Fatalf("fromCache = false, want cached vertex path")
	}
	if len(response.Pubkeys) != 1 || response.Pubkeys[0] != testPubkey {
		t.Fatalf("pubkeys = %v", response.Pubkeys)
	}
	if len(response.Events) != 1 || response.Events[0].Kind != 0 || !strings.Contains(response.Events[0].Content, "Sovran") {
		t.Fatalf("events = %+v, want the local kind-0", response.Events)
	}
	if len(response.Order) != 1 || response.Order[0] != strings.Repeat("f", 64) {
		t.Fatalf("order = %v, want the kind-0 event id", response.Order)
	}
	if v := vertexOf(response, testPubkey); v == nil || v["score"] != 42.5 {
		t.Fatalf("vertex payload = %+v", v)
	}
}

// When the Vertex DVM fails (e.g. exhausted credits), search must degrade to
// the locally-indexed profiles and still return 200 — not surface the DVM's
// error as a 502. This is the parity the REST endpoint was missing relative to
// the GraphQL profileSearch resolver.
func TestSearchFallsBackToLocalIndexWhenVertexErrors(t *testing.T) {
	rank := 7.0
	score := 88.0
	handler := New(
		fakeStore{
			profiles: map[string]chstore.K0Row{
				testPubkey: {
					PubKey:      testPubkey,
					DisplayName: "Sovran",
					Picture:     "https://example.test/avatar.png",
				},
			},
			profileSearch: []chstore.ProfileSearchRow{{
				Profile: chstore.K0Row{PubKey: testPubkey, DisplayName: "Sovran"},
				Rank:    rank,
				Score:   score,
			}},
		},
		WithProfileSearch(fakeVertex{
			searchErr: errors.New("DVM 5315 error: you don't have enough credits to fulfil the request"),
		}),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/search?query=sovran&limit=5", nil)
	handler.search(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.FromCache {
		t.Fatalf("fromCache should be false when DVM errored, got %+v", response)
	}
	if len(response.Pubkeys) != 1 || response.Pubkeys[0] != testPubkey {
		t.Fatalf("pubkeys = %v, want the local result even without a kind-0 event", response.Pubkeys)
	}
	if v := vertexOf(response, testPubkey); v == nil || v["rank"] != 7.0 {
		t.Fatalf("vertex payload = %+v", v)
	}
}

// Search must preserve the provider's Vertex-pagerank ordering verbatim (the
// quality the REST endpoint was losing by serving the local index instead). The
// provider returns Bob before Alice; the response must too.
func TestSearchHonorsProviderOrdering(t *testing.T) {
	const pubA = testPubkey
	const pubB = "1111111111111111111111111111111111111111111111111111111111111111"
	rA, rB := 0.1, 0.9
	handler := New(
		fakeStore{profiles: map[string]chstore.K0Row{
			pubA: {PubKey: pubA, DisplayName: "Alice", EventID: strings.Repeat("a", 64), RawJSON: `{"display_name":"Alice"}`},
			pubB: {PubKey: pubB, DisplayName: "Bob", EventID: strings.Repeat("b", 64), RawJSON: `{"display_name":"Bob"}`},
		}},
		WithProfileSearch(fakeVertex{search: []vertex.SearchResult{
			{PubKey: pubB, Npub: vertex.Npub(pubB), Rank: &rB},
			{PubKey: pubA, Npub: vertex.Npub(pubA), Rank: &rA},
		}}),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/search?query=team&limit=5", nil)
	handler.search(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var response ProvidersEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Pubkeys) != 2 || response.Pubkeys[0] != pubB || response.Pubkeys[1] != pubA {
		t.Fatalf("pubkeys = %v want [%s %s]", response.Pubkeys, pubB, pubA)
	}
	// Order anchors the kind-0 events in the same provider ranking.
	if len(response.Order) != 2 || response.Order[0] != strings.Repeat("b", 64) || response.Order[1] != strings.Repeat("a", 64) {
		t.Fatalf("order = %v", response.Order)
	}
}

// limit must be clamped before it reaches the provider and the local-fallback
// math, matching vertex.NormalizeSearchArgs, so REST and GraphQL agree on counts.
func TestSearchClampsLimitAboveCeiling(t *testing.T) {
	var captured vertex.SearchArgs
	handler := New(
		fakeStore{profiles: map[string]chstore.K0Row{
			testPubkey: {PubKey: testPubkey, DisplayName: "Sovran"},
		}},
		WithProfileSearch(fakeVertex{
			lastSearchArgs: &captured,
			search:         []vertex.SearchResult{{PubKey: testPubkey, Npub: vertex.Npub(testPubkey)}},
		}),
		WithNIP05Validation(false),
	)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/search?query=sovran&limit=999", nil)
	handler.search(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if captured.Limit != 100 {
		t.Fatalf("provider limit = %d, want clamped 100", captured.Limit)
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

type fakeRanker struct {
	events []chstore.EventView
	err    error
	calls  int
	last   any
}

func (f *fakeRanker) RankedEventViews(_ context.Context, input any) ([]chstore.EventView, error) {
	f.calls++
	f.last = input
	if f.err != nil {
		return nil, f.err
	}
	return f.events, nil
}

func TestRankedFeedPreservesRankingOrderAndEnriches(t *testing.T) {
	const firstID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const secondID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	const authorPubkey = "1111111111111111111111111111111111111111111111111111111111111111"

	// Ranked order is [first, second]; createdAt is intentionally inverted so a
	// passthrough of ranking (not chronological order) is what we assert.
	first := chstore.EventView{
		ID:        firstID,
		PubKey:    authorPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_000, 0),
		Content:   "ranked first",
		Tags:      [][]string{},
	}
	second := chstore.EventView{
		ID:        secondID,
		PubKey:    authorPubkey,
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_500, 0),
		Content:   "ranked second",
		Tags:      [][]string{},
	}
	store := &appViewHydrationStore{
		fakeStore: fakeStore{
			profiles: map[string]chstore.K0Row{
				authorPubkey: {PubKey: authorPubkey, EventID: strings.Repeat("e", 64), CreatedAt: time.Unix(1_700_000_000, 0), DisplayName: "Ranked Author", RawJSON: `{"display_name":"Ranked Author"}`},
			},
		},
		events: map[string]chstore.EventView{},
		stats: map[string]testCounts{
			firstID:  {LikeCount: 1},
			secondID: {LikeCount: 2},
		},
	}
	ranker := &fakeRanker{events: []chstore.EventView{first, second}}
	handler := New(store, WithRankedFeed(ranker), WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	body := `{"references":{"kinds":[7]},"via":{"key":"e"},"limit":10}`
	req := httptest.NewRequest(http.MethodPost, "/nostr/feed/ranked", strings.NewReader(body))
	handler.rankedFeed(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if ranker.calls != 1 {
		t.Fatalf("ranker calls = %d, want 1", ranker.calls)
	}
	if _, ok := ranker.last.(map[string]any); !ok {
		t.Fatalf("ranker input = %T, want map[string]any (same shape as GraphQL input)", ranker.last)
	}
	var response Envelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.OrderBy != orderByRank {
		t.Fatalf("orderBy = %q, want %q", response.OrderBy, orderByRank)
	}
	if len(response.Order) != 2 || response.Order[0] != firstID || response.Order[1] != secondID {
		t.Fatalf("order = %v, want ranking preserved [%s %s]", response.Order, firstID, secondID)
	}
	if response.Aggregates[firstID]["k7_e"]["actors"] != 1 || response.Aggregates[secondID]["k7_e"]["actors"] != 2 {
		t.Fatalf("aggregates = %+v", response.Aggregates)
	}
	if profile, ok := envelopeProfile(response, authorPubkey); !ok || !strings.Contains(profile.Content, "Ranked Author") {
		t.Fatalf("author profile event missing: %v", response.Events)
	}
}

func TestRankedFeedWithoutProviderReturnsServiceUnavailable(t *testing.T) {
	handler := New(fakeStore{profiles: map[string]chstore.K0Row{}}, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/nostr/feed/ranked", strings.NewReader(`{}`))
	handler.rankedFeed(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
}

func TestRankedFeedRejectsNonPost(t *testing.T) {
	ranker := &fakeRanker{}
	handler := New(fakeStore{profiles: map[string]chstore.K0Row{}}, WithRankedFeed(ranker), WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/feed/ranked", nil)
	handler.rankedFeed(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if ranker.calls != 0 {
		t.Fatalf("ranker should not be called for GET, calls = %d", ranker.calls)
	}
}

type notificationStore struct {
	appViewHydrationStore
	rows      []chstore.ViewerFeedRow
	lastInput chstore.ViewerFeedInput
}

func (s *notificationStore) ViewerFeed(_ context.Context, input chstore.ViewerFeedInput) ([]chstore.ViewerFeedRow, error) {
	// Record the primary (non-kind-3) call; the grouped handler also makes a
	// small kind-3-only sub-call we don't want to clobber the mirrored input.
	if len(input.Kinds) == 0 {
		s.lastInput = input
	}
	// Honor the kind include/exclude filters the way ClickHouse does, so the
	// grouped handler's separate kind-3 / other windows behave realistically.
	include := map[int64]bool{}
	for _, kind := range input.Kinds {
		include[kind] = true
	}
	exclude := map[int64]bool{}
	for _, kind := range input.ExcludeKinds {
		exclude[kind] = true
	}
	out := make([]chstore.ViewerFeedRow, 0, len(s.rows))
	for _, row := range s.rows {
		if len(include) > 0 && !include[int64(row.Kind)] {
			continue
		}
		if exclude[int64(row.Kind)] {
			continue
		}
		out = append(out, row)
	}
	return out, nil
}

func TestNotificationsEnrichesEventsAndMirrorsInput(t *testing.T) {
	const eventID = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const actorPubkey = "1111111111111111111111111111111111111111111111111111111111111111"

	event := chstore.EventView{
		ID:        eventID,
		PubKey:    actorPubkey,
		Kind:      7,
		CreatedAt: time.Unix(1_710_000_000, 0),
		Content:   "+",
		Tags:      [][]string{{"p", testPubkey}},
	}
	store := &notificationStore{
		appViewHydrationStore: appViewHydrationStore{
			fakeStore: fakeStore{
				profiles: map[string]chstore.K0Row{
					actorPubkey: {PubKey: actorPubkey, EventID: eventID, DisplayName: "Reactor"},
				},
			},
			events: map[string]chstore.EventView{},
			stats:  map[string]testCounts{eventID: {LikeCount: 3}},
		},
		rows: []chstore.ViewerFeedRow{{
			Event:            event,
			Kind:             7,
			ActorVertexScore: 0.42,
		}},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(
		http.MethodGet,
		"/nostr/notifications?pubkey="+testPubkey+"&policy=relaxed&replyScope=direct&until=1710000000&limit=25&grouped=false",
		nil,
	)
	handler.notifications(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if store.lastInput.Viewer != testPubkey {
		t.Fatalf("viewer = %q, want %q", store.lastInput.Viewer, testPubkey)
	}
	if store.lastInput.Policy != "RELAXED" || store.lastInput.ReplyScope != "DIRECT" {
		t.Fatalf("policy/replyScope = %q/%q", store.lastInput.Policy, store.lastInput.ReplyScope)
	}
	if store.lastInput.Until != 1_710_000_000 || store.lastInput.Limit != 25 {
		t.Fatalf("until/limit = %d/%d", store.lastInput.Until, store.lastInput.Limit)
	}
	var response NotificationsEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if len(response.Entries) != 1 {
		t.Fatalf("entries = %+v", response.Entries)
	}
	got := response.Entries[0]
	if got.ID != eventID || got.Kind != 7 || got.Actor == "" && len(got.Actors) == 0 {
		t.Fatalf("entry = %+v", got)
	}
	if len(got.Actors) != 1 || got.Actors[0].ActorVertexScore != 0.42 {
		t.Fatalf("entry actors = %+v", got.Actors)
	}
	// No reason strings anywhere in the payload — kinds only.
	if strings.Contains(rec.Body.String(), "REACTION") || strings.Contains(rec.Body.String(), "reaction") {
		t.Fatalf("reason string leaked into response: %s", rec.Body.String())
	}
	if response.Aggregates[eventID]["k7_e"]["actors"] != 3 {
		t.Fatalf("aggregates = %+v", response.Aggregates)
	}
	if response.Order[0] != eventID {
		t.Fatalf("order = %v", response.Order)
	}
	if response.Cursor == nil {
		t.Fatalf("cursor = nil, want a cursor for a non-empty page")
	}
}

func TestNotificationsDefaultsAndViewerFallback(t *testing.T) {
	store := &notificationStore{
		appViewHydrationStore: appViewHydrationStore{
			fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
			events:    map[string]chstore.EventView{},
			stats:     map[string]testCounts{},
		},
	}
	handler := New(store, WithViewerPubkey(testPubkey), WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	// grouped=false exercises the pass-through path so we can assert the parsed
	// defaults reach the store verbatim (the grouped path intentionally fans the
	// limit into a wide grouping window + a small follow window).
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications?grouped=false", nil)
	handler.notifications(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if store.lastInput.Viewer != testPubkey {
		t.Fatalf("viewer fallback = %q, want %q", store.lastInput.Viewer, testPubkey)
	}
	if store.lastInput.Tab != "ALL" || store.lastInput.Policy != "STRICT" || store.lastInput.ReplyScope != "THREAD" {
		t.Fatalf("defaults = tab %q policy %q replyScope %q", store.lastInput.Tab, store.lastInput.Policy, store.lastInput.ReplyScope)
	}
	if store.lastInput.Limit != 50 {
		t.Fatalf("default limit = %d, want 50", store.lastInput.Limit)
	}
}

func TestNotificationsAcceptsFollowsPolicy(t *testing.T) {
	store := &notificationStore{
		appViewHydrationStore: appViewHydrationStore{
			fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
			events:    map[string]chstore.EventView{},
			stats:     map[string]testCounts{},
		},
	}
	handler := New(store, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications?pubkey="+testPubkey+"&policy=follows&grouped=false", nil)
	handler.notifications(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	if store.lastInput.Policy != "FOLLOWS" {
		t.Fatalf("policy = %q, want FOLLOWS", store.lastInput.Policy)
	}
}

func TestNotificationsRequiresViewer(t *testing.T) {
	store := &notificationStore{
		appViewHydrationStore: appViewHydrationStore{
			fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
			events:    map[string]chstore.EventView{},
			stats:     map[string]testCounts{},
		},
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications", nil)
	handler.notifications(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d body = %s, want 400", rec.Code, rec.Body.String())
	}
}

func TestNotificationsGroupsFollowsReactionsAndKeepsRepliesSingle(t *testing.T) {
	hex := func(c byte) string { return strings.Repeat(string(c), 64) }
	target := hex('f')
	mk := func(id, actor string, kind int, at int64, tags [][]string) chstore.ViewerFeedRow {
		return chstore.ViewerFeedRow{
			Event:        chstore.EventView{ID: id, PubKey: actor, Kind: kind, CreatedAt: time.Unix(at, 0), Tags: tags},
			Kind:         kind,
			ActorPubKey:  actor,
			RefCreatedAt: time.Unix(at, 0),
		}
	}
	reactionTags := [][]string{{"e", target}, {"p", testPubkey}}
	// Newest-first, as the store returns them.
	rows := []chstore.ViewerFeedRow{
		mk(hex('a'), hex('1'), 7, 300, reactionTags),
		mk(hex('b'), hex('2'), 7, 290, reactionTags),
		mk(hex('c'), hex('3'), 3, 280, nil),
		mk(hex('d'), hex('4'), 3, 270, nil),
		mk(hex('e'), hex('5'), 3, 260, nil),
		mk(hex('9'), hex('6'), 1, 250, [][]string{{"e", target, "", "reply"}}),
	}
	store := &notificationStore{
		appViewHydrationStore: appViewHydrationStore{
			fakeStore: fakeStore{
				profiles: map[string]chstore.K0Row{
					hex('1'): {PubKey: hex('1'), DisplayName: "A1", EventID: hex('7'), RawJSON: `{"display_name":"A1"}`},
					hex('2'): {PubKey: hex('2'), DisplayName: "A2", EventID: hex('8'), RawJSON: `{"display_name":"A2"}`},
				},
				counts: chstore.PubkeyStats{Followers: 100},
			},
			events: map[string]chstore.EventView{target: {ID: target, PubKey: testPubkey, Kind: 1, Content: "my post"}},
			stats:  map[string]testCounts{},
		},
		rows: rows,
	}
	handler := New(store, WithNIP05Validation(false))

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications?pubkey="+testPubkey+"&limit=30", nil)
	handler.notifications(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp NotificationsEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	entries := resp.Entries
	if len(entries) != 3 {
		t.Fatalf("entries = %d, want 3 (kind-7 group, kind-3 group, kind-1 single): %+v", len(entries), entries)
	}
	// entry[0]: kind-7 group on the shared target.
	if entries[0].Kind != 7 || entries[0].Total != 2 || entries[0].Target != target {
		t.Fatalf("kind-7 group = %+v", entries[0])
	}
	if len(entries[0].Actors) != 2 {
		t.Fatalf("kind-7 group actors = %+v", entries[0].Actors)
	}
	// The target event is embedded in the envelope.
	targetEmbedded := false
	for _, event := range resp.Events {
		if event.ID == target {
			targetEmbedded = true
		}
	}
	if !targetEmbedded {
		t.Fatalf("target event not embedded: %v", resp.Order)
	}
	// entry[1]: kind-3 group with the exact count from PubkeyStats.
	if entries[1].Kind != 3 || entries[1].Total != 100 || entries[1].TotalCapped {
		t.Fatalf("kind-3 group = %+v", entries[1])
	}
	if len(entries[1].Actors) != 3 {
		t.Fatalf("kind-3 sample actors = %d, want 3", len(entries[1].Actors))
	}
	// entry[2]: the kind-1 stays single (no Total).
	if entries[2].Kind != 1 || entries[2].Total != 0 {
		t.Fatalf("kind-1 single = %+v", entries[2])
	}
	// Order anchors the representative ids.
	if len(resp.Order) != 3 || resp.Order[0] != entries[0].ID {
		t.Fatalf("order = %v", resp.Order)
	}
	// Sample-actor kind-0 profile events are embedded even though their member
	// events collapsed.
	actorProfileEmbedded := false
	for _, event := range resp.Events {
		if event.Kind == 0 && event.PubKey == hex('2') {
			actorProfileEmbedded = true
		}
	}
	if !actorProfileEmbedded {
		t.Fatalf("sample actor profile event missing")
	}
	// No reason vocabulary in the payload.
	for _, word := range []string{"reaction", "follow", "reply"} {
		if strings.Contains(rec.Body.String(), "\"reason\":") || strings.Contains(rec.Body.String(), "\""+word+"\"") {
			t.Fatalf("reason string %q leaked into response", word)
		}
	}
}

func TestNotificationsGroupedFalseReturnsRawSingles(t *testing.T) {
	hex := func(c byte) string { return strings.Repeat(string(c), 64) }
	rows := []chstore.ViewerFeedRow{
		{Event: chstore.EventView{ID: hex('a'), PubKey: hex('1'), Kind: 3, CreatedAt: time.Unix(300, 0)}, Kind: 3, ActorPubKey: hex('1'), RefCreatedAt: time.Unix(300, 0)},
		{Event: chstore.EventView{ID: hex('b'), PubKey: hex('2'), Kind: 3, CreatedAt: time.Unix(290, 0)}, Kind: 3, ActorPubKey: hex('2'), RefCreatedAt: time.Unix(290, 0)},
	}
	store := &notificationStore{
		appViewHydrationStore: appViewHydrationStore{
			fakeStore: fakeStore{profiles: map[string]chstore.K0Row{}},
			events:    map[string]chstore.EventView{},
			stats:     map[string]testCounts{},
		},
		rows: rows,
	}
	handler := New(store, WithNIP05Validation(false))
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/nostr/notifications?pubkey="+testPubkey+"&grouped=false", nil)
	handler.notifications(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d body = %s", rec.Code, rec.Body.String())
	}
	var resp NotificationsEnvelope
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if len(resp.Entries) != 2 {
		t.Fatalf("grouped=false should return raw rows, got %d", len(resp.Entries))
	}
	for _, entry := range resp.Entries {
		if entry.Total != 0 {
			t.Fatalf("grouped=false entry must be a raw single: %+v", entry)
		}
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

// vertexOf returns the vertex provider payload for a pubkey, or nil.
func vertexOf(resp ProvidersEnvelope, pubkey string) map[string]any {
	if resp.Providers == nil || resp.Providers[pubkey] == nil {
		return nil
	}
	payload, _ := resp.Providers[pubkey]["vertex"].(map[string]any)
	return payload
}

// followerCounts reads the follower/following aggregates for a pubkey.
func followerCounts(resp ProvidersEnvelope, pubkey string) (followers, follows uint64) {
	return resp.Aggregates[pubkey]["k3_p_latest"]["actors"],
		resp.Aggregates[pubkey]["k3_author_latest"]["sources"]
}

// firstEventAtOf reads the nagg provider first-indexed timestamp, or nil.
func firstEventAtOf(resp ProvidersEnvelope, pubkey string) *int64 {
	if resp.Providers == nil || resp.Providers[pubkey] == nil {
		return nil
	}
	payload, _ := resp.Providers[pubkey]["nagg"].(map[string]any)
	if payload == nil {
		return nil
	}
	v, ok := payload["firstEventAt"].(float64)
	if !ok {
		return nil
	}
	ts := int64(v)
	return &ts
}
