package rates

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

type fakeNostr func(context.Context, map[string]any, time.Duration) ([]relayquery.Event, error)

func (f fakeNostr) Query(ctx context.Context, filter map[string]any, timeout time.Duration) ([]relayquery.Event, error) {
	return f(ctx, filter, timeout)
}

type fakeHTTP func(context.Context, string) ([]byte, error)

func (f fakeHTTP) Fetch(ctx context.Context, url string) ([]byte, error) { return f(ctx, url) }
func quietLogger() *slog.Logger                                          { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func TestRatesPassLogSanitized(t *testing.T) {
	var log bytes.Buffer
	s := NewService(Config{Sources: []Source{{ID: "test", Currency: "USD", Kind: HTTPJSON, URL: "https://example.com/?token=secret", JSONKey: "USD"}}}, nil,
		fakeHTTP(func(context.Context, string) ([]byte, error) { return nil, errors.New("secret upstream response") }),
		slog.New(slog.NewTextHandler(&log, nil)))
	s.RunOnce(context.Background())
	got := log.String()
	if !strings.Contains(got, "rates.pass") || !strings.Contains(got, "sources.test.USD.ok=false") || strings.Contains(got, "secret") || strings.Contains(got, "example.com") {
		t.Fatalf("unexpected pass log: %s", got)
	}
}

func TestRatesServiceHTTPFallbackAndExpiry(t *testing.T) {
	now := time.Unix(1800000000, 0)
	start := now
	down := false
	calls := 0
	s := NewService(Config{Sources: LoadSources("", true, nil)}, fakeNostr(func(context.Context, map[string]any, time.Duration) ([]relayquery.Event, error) {
		return nil, errors.New("secret relay error")
	}), fakeHTTP(func(context.Context, string) ([]byte, error) {
		calls++
		if down {
			return nil, errors.New("secret URL")
		}
		return []byte(fmt.Sprintf(`{"time":%d,"USD":100,"EUR":90,"CHF":95,"GBP":80}`, now.Unix())), nil
	}), quietLogger())
	s.now = func() time.Time { return now }
	if _, ok := s.Snapshot(); ok {
		t.Fatal("warm before first pass")
	}
	s.RunOnce(context.Background())
	snap, ok := s.Snapshot()
	if !ok || !snap.Degraded || len(snap.Rates) != 4 || calls != 1 {
		t.Fatalf("snapshot %+v, calls %d", snap, calls)
	}
	for _, r := range snap.Rates {
		if r.Confidence != "single-source" || r.Samples != 1 || r.Sources[0] != "mempool" {
			t.Fatalf("rate %+v", r)
		}
	}
	for i := 0; i < 3; i++ {
		if snap.Sources[i].OK || snap.Sources[i].ConsecutiveFailures != 1 || snap.Sources[i].LastError != "Nostr fetch failed" {
			t.Fatalf("health %+v", snap.Sources[i])
		}
	}
	down = true
	for i := 0; i < 3; i++ {
		now = now.Add(time.Hour)
		s.RunOnce(context.Background())
	}
	snap, ok = s.Snapshot()
	if !ok || !snap.Degraded || snap.UpdatedAt != start.Unix() || snap.Rates["USD"].At != start.Unix() {
		t.Fatalf("stale snapshot %+v", snap)
	}
	if snap.Sources[3].ConsecutiveFailures != 3 || snap.Sources[3].LastError != "HTTP fetch failed" {
		t.Fatalf("health %+v", snap.Sources[3])
	}
	now = start.Add(24 * time.Hour)
	if _, ok := s.Snapshot(); !ok {
		t.Fatal("expired at inclusive boundary")
	}
	now = now.Add(time.Second)
	if _, ok := s.Snapshot(); ok {
		t.Fatal("served beyond StaleFor without another pass")
	}
	down = false
	s.RunOnce(context.Background())
	snap, ok = s.Snapshot()
	if !ok || !snap.Sources[3].OK || snap.Sources[3].ConsecutiveFailures != 0 || snap.Sources[3].LastError != "" || snap.Sources[3].LastErrorAt == nil {
		t.Fatalf("recovery %+v", snap)
	}
	// Copies must not allow callers to mutate shared state.
	snap.Rates["USD"].Sources[0] = "changed"
	*snap.Sources[3].LastSuccessAt = 1
	fresh, _ := s.Snapshot()
	if fresh.Rates["USD"].Sources[0] != "mempool" || *fresh.Sources[3].LastSuccessAt != now.Unix() {
		t.Fatal("snapshot aliases service state")
	}
}

func TestRatesServiceNotesValidateFilterDedupeAndCap(t *testing.T) {
	now := time.Unix(1800000000, 0)
	src := LoadSources("", false, nil)[0]
	var events []relayquery.Event
	for i := 0; i < 8; i++ {
		e := &nostr.Event{ID: fmt.Sprint(i), PubKey: src.Pubkey, Kind: 1, Content: "1 BTC = 100 USD", CreatedAt: nostr.Timestamp(now.Add(-time.Duration(i) * time.Minute).Unix())}
		events = append(events, relayquery.Event{Event: e}, relayquery.Event{Event: e})
	}
	for i, content := range []string{"garbage", "1 BTC = 100 EUR", "1 BTC = 100 USD"} {
		events = append(events, relayquery.Event{Event: &nostr.Event{ID: fmt.Sprint(100 + i), PubKey: "wrong", Kind: 2, Content: content, CreatedAt: nostr.Timestamp(now.Unix())}})
	}
	s := NewService(Config{Sources: []Source{src}}, fakeNostr(func(_ context.Context, filter map[string]any, timeout time.Duration) ([]relayquery.Event, error) {
		want := map[string]any{"kinds": []int{1}, "authors": []string{src.Pubkey}, "limit": 10}
		if !reflect.DeepEqual(filter, want) || timeout != 8*time.Second {
			t.Fatalf("filter=%v timeout=%v", filter, timeout)
		}
		return events, nil
	}), nil, quietLogger())
	s.now = func() time.Time { return now }
	s.RunOnce(context.Background())
	snap, ok := s.Snapshot()
	if !ok || snap.Rates["USD"].Samples != 1 || snap.Rates["USD"].Confidence != "single-source" || !snap.Degraded {
		t.Fatalf("got %+v", snap)
	}
	now = now.Add(7 * time.Hour)
	s.RunOnce(context.Background())
	snap, _ = s.Snapshot()
	if snap.Sources[0].OK || snap.Sources[0].LastError != "no fresh observations" {
		t.Fatalf("stale source %+v", snap.Sources[0])
	}
}

func TestRatesServiceHealthThresholdAndPlausibility(t *testing.T) {
	now := time.Unix(1800000000, 0)
	price := 100
	sources := []Source{
		{ID: "one", Currency: "USD", Kind: HTTPJSON, URL: "one", JSONKey: "USD"},
		{ID: "two", Currency: "USD", Kind: HTTPJSON, URL: "two", JSONKey: "USD"},
		{ID: "down", Currency: "USD", Kind: HTTPJSON, URL: "down", JSONKey: "USD"},
	}
	s := NewService(Config{Sources: sources}, nil, fakeHTTP(func(_ context.Context, url string) ([]byte, error) {
		if url == "down" {
			return nil, errors.New("secret")
		}
		return []byte(fmt.Sprintf(`{"USD":%d}`, price)), nil
	}), quietLogger())
	s.now = func() time.Time { return now }
	for i := 1; i <= 3; i++ {
		s.RunOnce(context.Background())
		snap, ok := s.Snapshot()
		if !ok || snap.Degraded != (i >= 3) {
			t.Fatalf("pass %d: %+v", i, snap)
		}
	}
	price = 200
	now = now.Add(time.Hour)
	s.RunOnce(context.Background())
	snap, _ := s.Snapshot()
	if snap.Rates["USD"].Price != 100 || !snap.Degraded {
		t.Fatalf("accepted jump: %+v", snap)
	}
	now = now.Add(24 * time.Hour)
	s.RunOnce(context.Background())
	snap, _ = s.Snapshot()
	if snap.Rates["USD"].Price != 200 {
		t.Fatalf("failed reanchor: %+v", snap)
	}
}

func TestRatesHTTPFetcherAndParsing(t *testing.T) {
	now := time.Unix(1800000000, 0)
	for _, tt := range []struct {
		name, body       string
		status           int
		fetchOK, parseOK bool
	}{
		{"valid", `{"time":1800000000,"USD":100}`, 200, true, true},
		{"no timestamp", `{"USD":100}`, 200, true, true},
		{"status", `{"USD":100}`, 503, false, false},
		{"oversize", strings.Repeat("x", maxHTTPBody+1), 200, false, false},
		{"garbage", `oops`, 200, true, false},
		{"missing price", `{}`, 200, true, false},
		{"negative", `{"USD":-1}`, 200, true, false},
		{"null timestamp", `{"USD":100,"time":null}`, 200, true, false},
		{"bad time", `{"USD":100,"time":"x"}`, 200, true, false},
		{"overflow", `{"USD":1e999}`, 200, true, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.status)
				_, _ = io.WriteString(w, tt.body)
			}))
			defer server.Close()
			f := NewHTTPFetcher()
			if f.client.Timeout != 8*time.Second {
				t.Fatal("unbounded timeout")
			}
			body, err := f.Fetch(context.Background(), server.URL)
			if (err == nil) != tt.fetchOK {
				t.Fatalf("fetch error %v", err)
			}
			_, ok := parseHTTP(body, Source{JSONKey: "USD"}, now)
			if ok != tt.parseOK {
				t.Fatalf("parse ok=%v", ok)
			}
		})
	}
}

func TestRatesRunImmediateCancelAndConcurrentSnapshots(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fetched := make(chan struct{}, 1)
	s := NewService(Config{Sources: []Source{{ID: "test", Currency: "USD", Kind: HTTPJSON, JSONKey: "USD"}}}, nil, fakeHTTP(func(context.Context, string) ([]byte, error) {
		fetched <- struct{}{}
		return []byte(`{"USD":100}`), nil
	}), quietLogger())
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	select {
	case <-fetched:
	case <-time.After(time.Second):
		t.Fatal("no immediate pass")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run ignored cancellation")
	}
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			for range 20 {
				s.Snapshot()
			}
		})
	}
	for range 3 {
		s.RunOnce(context.Background())
		<-fetched
	}
	wg.Wait()
}
