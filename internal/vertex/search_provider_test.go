package vertex

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeSearchCacheStore struct {
	cached   []SearchResult
	cachedAt time.Time
	cacheOK  bool
	cacheErr error
	saved    [][]SearchResult
}

func (s *fakeSearchCacheStore) CachedVertexSearch(context.Context, SearchArgs) ([]SearchResult, time.Time, bool, error) {
	return s.cached, s.cachedAt, s.cacheOK, s.cacheErr
}

func (s *fakeSearchCacheStore) SaveVertexSearch(_ context.Context, _ SearchArgs, rows []SearchResult) error {
	s.saved = append(s.saved, rows)
	return nil
}

type fakeSearchRefreshClient struct {
	rows  []SearchResult
	err   error
	calls int
}

func (c *fakeSearchRefreshClient) SearchRefresh(context.Context, SearchArgs) ([]SearchResult, error) {
	c.calls++
	return c.rows, c.err
}

// A provider with no refresh client (how cmd/api wires the no-Vertex-key case)
// must degrade to ErrUnavailable on a cache miss, never panic on a nil-pointer
// method call. Regression guard for the typed-nil-interface trap: cmd/api assigns
// vertexClient through the SearchRefreshClient interface only when non-nil so the
// p.client == nil guard fires here instead of dereferencing a nil *Client.
func TestSearchProviderNilClientCacheMissReturnsUnavailable(t *testing.T) {
	store := &fakeSearchCacheStore{cacheOK: false}
	provider := NewSearchProvider(store, nil, SearchProviderConfig{}, nil)

	rows, fromCache, err := provider.Search(context.Background(), SearchArgs{Query: "cal", Limit: 5})
	if !errors.Is(err, ErrUnavailable) {
		t.Fatalf("err = %v, want ErrUnavailable", err)
	}
	if rows != nil || fromCache {
		t.Fatalf("rows = %v fromCache = %v, want nil/false", rows, fromCache)
	}
}

// Even with no refresh client, a cache hit must still serve the cached
// Vertex-pagerank rows — this is what keeps search good when the live DVM is down.
func TestSearchProviderServesCacheWhenClientNil(t *testing.T) {
	rank := 0.5
	store := &fakeSearchCacheStore{
		cached:   []SearchResult{{PubKey: "p", Rank: &rank}},
		cachedAt: time.Now(),
		cacheOK:  true,
	}
	provider := NewSearchProvider(store, nil, SearchProviderConfig{MaxAge: time.Hour}, nil)

	rows, fromCache, err := provider.Search(context.Background(), SearchArgs{Query: "cal", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if !fromCache || len(rows) != 1 {
		t.Fatalf("rows = %v fromCache = %v, want 1 cached/true", rows, fromCache)
	}
}

// On a cache miss with a healthy client the provider refreshes live and writes
// the result back to the cache (fromCache=false on the synchronous miss).
func TestSearchProviderRefreshesAndCachesOnMiss(t *testing.T) {
	rank := 0.9
	store := &fakeSearchCacheStore{cacheOK: false}
	client := &fakeSearchRefreshClient{rows: []SearchResult{{PubKey: "p", Rank: &rank}}}
	provider := NewSearchProvider(store, client, SearchProviderConfig{}, nil)

	rows, fromCache, err := provider.Search(context.Background(), SearchArgs{Query: "cal", Limit: 5})
	if err != nil {
		t.Fatal(err)
	}
	if fromCache || len(rows) != 1 {
		t.Fatalf("rows = %v fromCache = %v, want 1 fresh/false", rows, fromCache)
	}
	if client.calls != 1 || len(store.saved) != 1 {
		t.Fatalf("client calls = %d saved = %d, want refresh and save", client.calls, len(store.saved))
	}
}
