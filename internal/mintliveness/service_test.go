package mintliveness

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// lister is a fixed work-list.
type lister []string

func (l lister) MintURLs(context.Context) ([]string, error) { return []string(l), nil }

// mint is one fake mint whose /v1/info answer is switchable.
type mint struct {
	*httptest.Server
	status atomic.Int32
	body   atomic.Pointer[string]
	delay  time.Duration
}

func newMint(t *testing.T, delay time.Duration) *mint {
	t.Helper()
	m := &mint{delay: delay}
	m.status.Store(http.StatusOK)
	body := `{"name":"Test mint","nuts":{"4":{"methods":[]}}}`
	m.body.Store(&body)
	m.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/info" {
			http.NotFound(w, r)
			return
		}
		time.Sleep(m.delay)
		w.WriteHeader(int(m.status.Load()))
		fmt.Fprint(w, *m.body.Load())
	}))
	t.Cleanup(m.Close)
	return m
}

func TestReachableMintIsOnlineWithLatencyAndCheckedAt(t *testing.T) {
	m := newMint(t, 20*time.Millisecond)
	s := New(Config{}, nil, lister{m.URL + "/"}, nil)
	before := time.Now().Add(-time.Second)
	s.RunOnce(context.Background())

	// The caller's own spelling is the map key, however it is cased or slashed.
	for _, key := range []string{m.URL, m.URL + "/", Key(m.URL)} {
		st := s.Liveness([]string{key})[key]
		if st.Status != StatusOnline {
			t.Fatalf("key %q: status = %+v, want online", key, st)
		}
		if st.CheckedAt == nil || st.CheckedAt.Before(before.Truncate(time.Second)) {
			t.Fatalf("key %q: checkedAt = %v, want a recent time", key, st.CheckedAt)
		}
		if st.LatencyMs < 20 {
			t.Fatalf("key %q: latencyMs = %d, want the 20ms the mint took", key, st.LatencyMs)
		}
	}
}

func TestServerErrorIsOfflineWithCheckedAtAndNoLatency(t *testing.T) {
	m := newMint(t, 0)
	m.status.Store(http.StatusInternalServerError)
	s := New(Config{}, nil, lister{m.URL}, nil)
	s.RunOnce(context.Background())

	st := s.Liveness([]string{m.URL})[m.URL]
	if st.Status != StatusOffline || st.CheckedAt == nil || st.LatencyMs != 0 {
		t.Fatalf("status = %+v, want offline with checkedAt and no latency", st)
	}
}

func TestNonObjectBodyAndDeadSocketAreOffline(t *testing.T) {
	m := newMint(t, 0)
	html := `<html>parked</html>`
	m.body.Store(&html)
	dead := httptest.NewServer(http.NotFoundHandler())
	dead.Close()

	s := New(Config{}, nil, lister{m.URL, dead.URL}, nil)
	s.RunOnce(context.Background())

	got := s.Liveness([]string{m.URL, dead.URL})
	if got[m.URL].Status != StatusOffline {
		t.Fatalf("html body: %+v, want offline", got[m.URL])
	}
	if got[dead.URL].Status != StatusOffline {
		t.Fatalf("dead socket: %+v, want offline", got[dead.URL])
	}
}

func TestNeverProbedIsUnknown(t *testing.T) {
	m := newMint(t, 0)
	s := New(Config{}, nil, lister{m.URL}, nil)

	// Listed but not yet swept, and not listed at all, read the same.
	got := s.Liveness([]string{m.URL, "https://nobody.example"})
	for key, st := range got {
		if st.Status != StatusUnknown || st.CheckedAt != nil || st.LatencyMs != 0 {
			t.Fatalf("%q before any sweep = %+v, want bare unknown", key, st)
		}
	}
}

func TestProbeOlderThanMaxAgeIsUnknown(t *testing.T) {
	m := newMint(t, 0)
	clock := time.Unix(1_790_000_000, 0).UTC()
	s := New(Config{MaxAge: time.Hour}, nil, lister{m.URL}, nil)
	s.now = func() time.Time { return clock }
	s.RunOnce(context.Background())

	if st := s.Liveness([]string{m.URL})[m.URL]; st.Status != StatusOnline || st.CheckedAt == nil || !st.CheckedAt.Equal(clock) {
		t.Fatalf("fresh probe = %+v, want online at %v", st, clock)
	}
	clock = clock.Add(time.Hour + time.Second)
	if st := s.Liveness([]string{m.URL})[m.URL]; st.Status != StatusUnknown || st.CheckedAt != nil || st.LatencyMs != 0 {
		t.Fatalf("aged probe = %+v, want bare unknown", st)
	}
}

func TestOutageKeepsTheRecordAndItsLastLatency(t *testing.T) {
	m := newMint(t, 0)
	s := New(Config{}, nil, lister{m.URL}, nil)
	s.RunOnce(context.Background())
	m.status.Store(http.StatusBadGateway)
	s.RunOnce(context.Background())

	st := s.Liveness([]string{m.URL})[m.URL]
	// Offline, not gone; and no latency is published for a probe that failed.
	if st.Status != StatusOffline || st.CheckedAt == nil || st.LatencyMs != 0 {
		t.Fatalf("after outage = %+v, want offline with checkedAt and no latency", st)
	}
}

func TestKeyMatchesAppviewDedupKey(t *testing.T) {
	// appview.normalizeMintURL is lower(trimRight(trimSpace(url), "/")); the
	// service key must land on the same string so the handler's key finds the
	// record the sweep filed under the path-preserving fetch target.
	raw := " https://Mint.Minibits.cash/Bitcoin/ "
	if Key(raw) != "https://mint.minibits.cash/bitcoin" {
		t.Fatalf("Key(%q) = %q", raw, Key(raw))
	}
	if Key(Key(raw)) != Key(raw) {
		t.Fatalf("Key is not idempotent: %q", Key(Key(raw)))
	}
}
