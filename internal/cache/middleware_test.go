package cache

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// memCache is a minimal in-memory Cache for exercising the middleware without
// Redis. It ignores TTL: freshness in serveSWR is derived from the stored
// envelope timestamp, not the backend expiry, so tests stay deterministic.
type memCache struct {
	mu   sync.Mutex
	m    map[string][]byte
	sets int32
}

func newMemCache() *memCache { return &memCache{m: map[string][]byte{}} }

func (c *memCache) Get(_ context.Context, key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	v, ok := c.m[key]
	return v, ok
}

func (c *memCache) Set(_ context.Context, key string, value []byte, _ time.Duration) {
	c.mu.Lock()
	c.m[key] = value
	c.mu.Unlock()
	atomic.AddInt32(&c.sets, 1)
}

func (c *memCache) Enabled() bool { return true }

func postGraphQL(t *testing.T, h http.HandlerFunc, query string) (string, string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"`+query+`"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	h(w, r)
	return w.Header().Get("X-Nagg-Cache"), w.Body.String()
}

func TestWrapGraphQLMissThenHit(t *testing.T) {
	var calls int32
	next := func(w http.ResponseWriter, _ *http.Request) {
		atomic.AddInt32(&calls, 1)
		_, _ = w.Write([]byte(`{"data":{"ok":true}}`))
	}
	h := WrapGraphQL(next, newMemCache(), 20*time.Second, 5*time.Minute)

	if state, _ := postGraphQL(t, h, "{ ok }"); state != "miss" {
		t.Fatalf("first request: got %q, want miss", state)
	}
	if state, body := postGraphQL(t, h, "{ ok }"); state != "hit" || body != `{"data":{"ok":true}}` {
		t.Fatalf("second request: got state %q body %q, want hit", state, body)
	}
	if got := atomic.LoadInt32(&calls); got != 1 {
		t.Fatalf("handler called %d times, want 1 (second served from cache)", got)
	}
}

func TestWrapGraphQLStaleServesAndRevalidates(t *testing.T) {
	mc := newMemCache()
	key := GraphQLKey("{ ok }", "", nil, "")
	// Seed a stale entry (stored an hour ago, well past the 20s fresh TTL).
	mc.Set(context.Background(), key, encodeEnvelope(time.Now().Add(-time.Hour), []byte(`{"data":"old"}`)), time.Minute)

	revalidated := make(chan struct{}, 1)
	next := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":"new"}`))
		select {
		case revalidated <- struct{}{}:
		default:
		}
	}
	h := WrapGraphQL(next, mc, 20*time.Second, 5*time.Minute)

	state, body := postGraphQL(t, h, "{ ok }")
	if state != "stale" || body != `{"data":"old"}` {
		t.Fatalf("got state %q body %q, want stale + old body served instantly", state, body)
	}

	select {
	case <-revalidated:
	case <-time.After(2 * time.Second):
		t.Fatal("background revalidation did not run")
	}
	// The revalidated value should land in the cache as a fresh envelope.
	waitFor(t, func() bool {
		if raw, ok := mc.Get(context.Background(), key); ok {
			_, b, decoded := decodeEnvelope(raw)
			return decoded && string(b) == `{"data":"new"}`
		}
		return false
	})
}

func TestWrapGraphQLRefreshServesStaleNotBlocking(t *testing.T) {
	mc := newMemCache()
	key := GraphQLKey("{ ok }", "", nil, "")
	mc.Set(context.Background(), key, encodeEnvelope(time.Now(), []byte(`{"data":"cached"}`)), time.Minute)

	next := func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(500 * time.Millisecond) // a slow recompute must NOT block the response
		_, _ = w.Write([]byte(`{"data":"fresh"}`))
	}
	h := WrapGraphQL(next, mc, 20*time.Second, 5*time.Minute)

	r := httptest.NewRequest(http.MethodPost, "/graphql?refresh=1", strings.NewReader(`{"query":"{ ok }"}`))
	r.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	start := time.Now()
	h(w, r)
	if elapsed := time.Since(start); elapsed > 200*time.Millisecond {
		t.Fatalf("refresh blocked for %s; expected instant stale serve", elapsed)
	}
	if state := w.Header().Get("X-Nagg-Cache"); state != "stale" {
		t.Fatalf("got %q, want stale on refresh", state)
	}
	if body := w.Body.String(); body != `{"data":"cached"}` {
		t.Fatalf("got body %q, want cached value served instantly", body)
	}
}

func TestCachePolicyPerType(t *testing.T) {
	// DMs must never serve stale: a refresh recomputes so the newest message is
	// always shown. Feed tolerates a generous stale window.
	if fresh, stale := graphqlCachePolicy("{ dmEnvelopes { id } }", 30*time.Second, 5*time.Minute); stale != 0 || fresh > 10*time.Second {
		t.Fatalf("DM policy = (%s, %s), want short fresh + zero stale", fresh, stale)
	}
	if _, stale := graphqlCachePolicy("{ rankedEvents { nodes { id } } }", 30*time.Second, 5*time.Minute); stale == 0 {
		t.Fatal("feed policy should allow stale serving")
	}
	if _, stale := restCachePolicy("/v1/nostr/dm/abc", 30*time.Second, 5*time.Minute); stale != 0 {
		t.Fatal("REST DM policy should never serve stale")
	}
}

// TestWrapGraphQLDMRecomputesWhenStale proves a DM query past its fresh TTL is
// recomputed (current data) rather than served stale, because its stale window
// is zero.
func TestWrapGraphQLDMRecomputesWhenStale(t *testing.T) {
	mc := newMemCache()
	dmQuery := "{ dmConversation { id } }"
	key := GraphQLKey(dmQuery, "", nil, "")
	// Seed an entry older than the DM fresh TTL.
	mc.Set(context.Background(), key, encodeEnvelope(time.Now().Add(-time.Minute), []byte(`{"data":"old"}`)), time.Hour)

	next := func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"data":"new"}`))
	}
	h := WrapGraphQL(next, mc, 30*time.Second, 5*time.Minute)

	state, body := postGraphQL(t, h, dmQuery)
	if state != "miss" || body != `{"data":"new"}` {
		t.Fatalf("stale DM read = (%q, %q), want a blocking recompute to current data", state, body)
	}
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}

func TestAppCachePolicy(t *testing.T) {
	for _, path := range []string{"/app/latest-version", "/app/ai-lineup", "/app/rates", "/v1/app/latest-version", "/v1/app/ai-lineup", "/v1/app/rates"} {
		fresh, stale := restCachePolicy(path, time.Second, time.Second)
		if fresh != time.Minute || stale != 24*time.Hour {
			t.Fatalf("%s policy = (%s, %s)", path, fresh, stale)
		}
	}
	if fresh, stale := restCachePolicy("/other", time.Second, 2*time.Second); fresh != time.Second || stale != 2*time.Second {
		t.Fatalf("fallback policy = (%s, %s)", fresh, stale)
	}
}

func TestAppCacheHeaders(t *testing.T) {
	for _, state := range []string{"miss", "hit", "stale", "error"} {
		t.Run(state, func(t *testing.T) {
			c := newMemCache()
			path := "/app/latest-version"
			if state == "hit" || state == "stale" {
				storedAt := time.Now()
				if state == "stale" {
					storedAt = storedAt.Add(-2 * time.Minute)
				}
				c.Set(context.Background(), RESTKey(http.MethodGet, path, "", ""), encodeEnvelope(storedAt, []byte(`{"version":"0.1.3"}`)), 25*time.Hour)
			}
			h := WrapREST(func(w http.ResponseWriter, r *http.Request) {
				if state == "error" {
					http.Error(w, "unavailable", http.StatusServiceUnavailable)
					return
				}
				_, _ = w.Write([]byte(`{"version":"0.1.3"}`))
			}, c, time.Second, time.Second)
			rec := httptest.NewRecorder()
			h(rec, httptest.NewRequest(http.MethodGet, path, nil))
			if state == "error" {
				if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Cache-Control") != "" {
					t.Fatalf("error response: %d %v", rec.Code, rec.Header())
				}
				return
			}
			if rec.Header().Get("Cache-Control") != "public, max-age=60" || rec.Header().Get("X-Nagg-Cache") != state {
				t.Fatalf("headers = %v", rec.Header())
			}
		})
	}
}
