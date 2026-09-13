package appview

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/modules"
	"github.com/vertex-lab/nagg/internal/vertex"
)

type relayTestStore struct {
	fakeStore
	savedProfile *vertex.ProfileResult
	savedSearch  []vertex.SearchResult
	searchArgs   vertex.SearchArgs
}

func (s *relayTestStore) SaveVertexProfile(_ context.Context, p vertex.ProfileResult) error {
	s.savedProfile = &p
	return nil
}
func (s *relayTestStore) SaveVertexSearch(_ context.Context, a vertex.SearchArgs, r []vertex.SearchResult) error {
	s.searchArgs = a
	s.savedSearch = r
	return nil
}
func (s *relayTestStore) CachedVertexSearch(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, time.Time, bool, error) {
	return s.savedSearch, time.Now(), len(s.savedSearch) > 0, nil
}

// Panic makes accidental social-table queries fail even if their errors would
// otherwise be swallowed by a best-effort enrichment path.
func (s *relayTestStore) PubkeyStats(context.Context, string) (chstore.PubkeyStats, error) {
	panic("nostr table queried")
}
func (s *relayTestStore) BatchPubkeyStats(context.Context, []string) (map[string]chstore.PubkeyStats, error) {
	panic("nostr table queried")
}

type relayTestClient struct {
	events []nostr.Event
	err    error
}

func (c *relayTestClient) RelaySigned(_ context.Context, e nostr.Event) (vertex.SignedResult, error) {
	c.events = append(c.events, e)
	if c.err != nil {
		return vertex.SignedResult{}, c.err
	}
	stamp := time.Now().Unix()
	rank := 0.1
	if e.Kind == vertex.ProfileRequestKind {
		return vertex.SignedResult{Kind: "profile", Profile: &vertex.ProfileResult{PubKey: testPubkey, Rank: rank, FetchedAt: &stamp}, FetchedAt: stamp}, nil
	}
	kind := "search"
	if e.Kind == vertex.RecommendRequestKind {
		kind = "recommend"
	}
	return vertex.SignedResult{Kind: kind, Results: []vertex.SearchResult{{PubKey: testPubkey, Rank: &rank, FetchedAt: &stamp}}, FetchedAt: stamp}, nil
}
func relayRequestEvent(t *testing.T, kind int, tags nostr.Tags) nostr.Event {
	t.Helper()
	e := nostr.Event{Kind: kind, Tags: tags, CreatedAt: nostr.Now()}
	if err := e.Sign(nostr.GeneratePrivateKey()); err != nil {
		t.Fatal(err)
	}
	return e
}
func relayTestHandler(store *relayTestStore, client *relayTestClient, limit int) *Handler {
	return New(store, WithModules(modules.Set{modules.Mint: {}, modules.Vertex: {}}), WithNIP05Validation(false), WithVertexRelay(client, true, limit, false))
}
func postRelay(t *testing.T, h *Handler, e nostr.Event) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	h.vertexRelayRequest(w, httptest.NewRequest(http.MethodPost, "/nostr/vertex/relay", bytes.NewReader(body)))
	return w
}
func TestVertexRelayValidation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(*nostr.Event)
	}{
		{"bad signature", func(e *nostr.Event) { e.Sig = strings.Repeat("0", 128) }},
		{"noncanonical signer", func(e *nostr.Event) { e.PubKey = strings.ToUpper(e.PubKey) }},
		{"wrong kind", func(e *nostr.Event) { e.Kind = 1 }},
		{"stale", func(e *nostr.Event) { e.CreatedAt -= 301 }},
		{"future", func(e *nostr.Event) { e.CreatedAt += 301 }},
		{"personalized", func(e *nostr.Event) {
			e.Tags = append(e.Tags, nostr.Tag{"param", "sort", "personalizedPagerank"}, nostr.Tag{"param", "source", testPubkey})
			_ = e.Sign(nostr.GeneratePrivateKey())
		}},
		{"duplicate", func(e *nostr.Event) {
			e.Tags = append(e.Tags, nostr.Tag{"param", "target", testPubkey})
			_ = e.Sign(nostr.GeneratePrivateKey())
		}},
		{"large content", func(e *nostr.Event) { e.Content = strings.Repeat("x", 1025) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &relayTestClient{}
			h := relayTestHandler(&relayTestStore{}, client, 10)
			e := relayRequestEvent(t, vertex.ProfileRequestKind, nostr.Tags{{"param", "target", testPubkey}})
			tc.mutate(&e)
			w := postRelay(t, h, e)
			if w.Code != 400 || len(client.events) != 0 {
				t.Fatalf("status=%d body=%s calls=%d", w.Code, w.Body, len(client.events))
			}
		})
	}
}
func TestVertexRelayHappyPathAndRateLimit(t *testing.T) {
	store, client := &relayTestStore{}, &relayTestClient{}
	h := relayTestHandler(store, client, 10)
	e := relayRequestEvent(t, vertex.ProfileRequestKind, nostr.Tags{{"param", "target", testPubkey}})
	for i := 0; i < 11; i++ {
		w := postRelay(t, h, e)
		want := 200
		if i == 10 {
			want = 429
		}
		if w.Code != want {
			t.Fatalf("request %d status=%d %s", i, w.Code, w.Body)
		}
	}
	if store.savedProfile == nil || store.savedProfile.PubKey != testPubkey || len(client.events) != 10 {
		t.Fatalf("store=%#v calls=%d", store, len(client.events))
	}
	if !reflect.DeepEqual(client.events[0], e) {
		t.Fatal("event changed")
	}
	// A different signed identity has an independent allowance.
	if w := postRelay(t, h, relayRequestEvent(t, vertex.ProfileRequestKind, e.Tags)); w.Code != 200 {
		t.Fatalf("other pubkey status=%d", w.Code)
	}
}
func TestVertexRelaySearchAndRecommendCache(t *testing.T) {
	for _, kind := range []int{vertex.SearchRequestKind, vertex.RecommendRequestKind} {
		store, client := &relayTestStore{}, &relayTestClient{}
		h := relayTestHandler(store, client, 10)
		tags := nostr.Tags{{"param", "limit", "5"}}
		if kind == vertex.SearchRequestKind {
			tags = append(tags, nostr.Tag{"param", "search", "alice"})
		}
		w := postRelay(t, h, relayRequestEvent(t, kind, tags))
		if w.Code != 200 {
			t.Fatalf("%s", w.Body)
		}
		var body struct {
			OK     bool `json:"ok"`
			Cached bool `json:"cached"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if !body.OK || body.Cached != (kind == vertex.SearchRequestKind) {
			t.Fatalf("%s", w.Body)
		}
		if kind == vertex.SearchRequestKind && (len(store.savedSearch) != 1 || store.searchArgs.Query != "alice") {
			t.Fatal("search not saved")
		}
	}
}
func TestVertexRelayErrors(t *testing.T) {
	for _, tc := range []struct {
		err    error
		status int
		reason string
	}{
		{vertex.ErrInsufficientCredits, 200, "insufficient_credits"},
		{&vertex.ErrDVMRejected{Message: "private data"}, 200, "rejected"},
		{context.DeadlineExceeded, 504, "timeout"},
		{errors.New("internal private data"), 502, "unavailable"},
	} {
		h := relayTestHandler(&relayTestStore{}, &relayTestClient{err: tc.err}, 10)
		w := postRelay(t, h, relayRequestEvent(t, vertex.ProfileRequestKind, nostr.Tags{{"param", "target", testPubkey}}))
		if w.Code != tc.status || !strings.Contains(w.Body.String(), tc.reason) || strings.Contains(w.Body.String(), "private") {
			t.Fatalf("%d %s", w.Code, w.Body)
		}
	}
}
func TestVertexSearchProfilePiggyback(t *testing.T) {
	for _, kind := range []int{vertex.SearchRequestKind, vertex.ProfileRequestKind} {
		for _, method := range []string{http.MethodGet, http.MethodPost} {
			if kind == vertex.ProfileRequestKind && method == http.MethodPost {
				continue
			}
			store, client := &relayTestStore{}, &relayTestClient{}
			h := relayTestHandler(store, client, 10)
			path := "/nostr/search?query=alice"
			tags := nostr.Tags{{"param", "search", "alice"}}
			if kind == vertex.ProfileRequestKind {
				path = "/nostr/profile?pubkey=" + testPubkey
				tags = nostr.Tags{{"param", "target", testPubkey}}
			}
			event := relayRequestEvent(t, kind, tags)
			data, _ := json.Marshal(event)
			path += "&svr=" + base64.RawURLEncoding.EncodeToString(data)
			w := httptest.NewRecorder()
			req := httptest.NewRequest(method, path, strings.NewReader(`{"query":"alice"}`))
			if kind == vertex.SearchRequestKind {
				h.search(w, req)
			} else {
				h.profile(w, req)
			}
			if w.Code != 200 || len(client.events) != 1 || !reflect.DeepEqual(client.events[0], event) {
				t.Fatalf("%s %d %s calls=%d", method, w.Code, w.Body, len(client.events))
			}
			var body ProvidersEnvelope
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if len(body.Pubkeys) != 1 || vertexOf(body, testPubkey)["vertexFetchedAt"] == nil {
				t.Fatalf("missing fresh result: %s", w.Body)
			}
			if kind == vertex.SearchRequestKind && !body.VertexFresh {
				t.Fatalf("not fresh: %s", w.Body)
			}
		}
	}
}
func TestVertexCacheOnlyReadsWithoutNostr(t *testing.T) {
	stamp := time.Now().Unix()
	store := &relayTestStore{fakeStore: fakeStore{cachedVertex: vertex.ProfileResult{PubKey: testPubkey, Rank: 0.1, FetchedAt: &stamp}, cachedVertexOK: true}}
	client := &relayTestClient{}
	h := relayTestHandler(store, client, 10)
	for _, path := range []string{"/nostr/profile?pubkey=" + testPubkey, "/nostr/search?query=alice"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if strings.Contains(path, "/profile") {
			h.profile(w, req)
		} else {
			h.search(w, req)
		}
		if w.Code != 200 || len(client.events) != 0 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body)
		}
	}
}
func TestVertexPiggybackMismatch(t *testing.T) {
	client := &relayTestClient{}
	h := relayTestHandler(&relayTestStore{}, client, 10)
	e := relayRequestEvent(t, vertex.SearchRequestKind, nostr.Tags{{"param", "search", "bob"}})
	data, _ := json.Marshal(e)
	w := httptest.NewRecorder()
	h.search(w, httptest.NewRequest(http.MethodGet, "/nostr/search?query=alice&svr="+base64.RawURLEncoding.EncodeToString(data), nil))
	if w.Code != 400 || len(client.events) != 0 {
		t.Fatalf("%d %s", w.Code, w.Body)
	}
}

func TestVertexSearchCacheFreshness(t *testing.T) {
	for _, age := range []time.Duration{time.Hour, 8 * 24 * time.Hour} {
		store := &relayTestStore{}
		h := relayTestHandler(store, &relayTestClient{}, 10)
		cache := &readSearchCache{fetched: time.Now().Add(-age)}
		h.profileSearcher = vertex.NewSearchProvider(cache, nil, vertex.SearchProviderConfig{MaxAge: vertex.NewPlugin().Policy().CacheTTL}, nil)
		w := httptest.NewRecorder()
		h.search(w, httptest.NewRequest(http.MethodGet, "/nostr/search?query=alice", nil))
		var body ProvidersEnvelope
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || !body.FromCache || body.VertexFresh != (age < vertex.NewPlugin().Policy().CacheTTL) {
			t.Fatalf("age=%s: %d %s", age, w.Code, w.Body)
		}
		if got := vertexOf(body, testPubkey)["vertexFetchedAt"]; got != float64(cache.fetched.Unix()) {
			t.Fatalf("timestamp=%v", got)
		}
	}
}

type readSearchCache struct{ fetched time.Time }

func (s *readSearchCache) CachedVertexSearch(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, time.Time, bool, error) {
	rank := 0.1
	return []vertex.SearchResult{{PubKey: testPubkey, Rank: &rank}}, s.fetched, true, nil
}
func (s *readSearchCache) SaveVertexSearch(context.Context, vertex.SearchArgs, []vertex.SearchResult) error {
	panic("cache-only search tried to write")
}

func TestVertexPersonalizedNeverOverwritesGlobalProfile(t *testing.T) {
	store, client := &relayTestStore{}, &relayTestClient{}
	h := relayTestHandler(store, client, 10)
	h.vertexAllowPersonalized = true
	event := relayRequestEvent(t, vertex.ProfileRequestKind, nostr.Tags{{"param", "target", testPubkey}, {"param", "sort", "personalizedPagerank"}, {"param", "source", testPubkey}})
	w := postRelay(t, h, event)
	if w.Code != 200 || store.savedProfile != nil || !strings.Contains(w.Body.String(), `"cached":false`) {
		t.Fatalf("%d %s saved=%#v", w.Code, w.Body, store.savedProfile)
	}
}
func TestVertexRelayDisabled(t *testing.T) {
	client := &relayTestClient{}
	h := relayTestHandler(&relayTestStore{}, client, 10)
	h.vertexRelayEnabled = false
	w := postRelay(t, h, relayRequestEvent(t, vertex.ProfileRequestKind, nostr.Tags{{"param", "target", testPubkey}}))
	if w.Code != 503 || len(client.events) != 0 {
		t.Fatalf("%d calls=%d", w.Code, len(client.events))
	}
}
