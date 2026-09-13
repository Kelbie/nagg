package auditor

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

type clientFunc func(context.Context) ([]Mint, error)

func (f clientFunc) Mints(ctx context.Context) ([]Mint, error) { return f(ctx) }

type probeClient struct {
	clientFunc
	err error
}

func (c probeClient) Probe(context.Context) error { return c.err }

func TestDualFallbackAndRecovery(t *testing.T) {
	for _, failure := range []string{"fetch", "empty", "probe"} {
		t.Run(failure, func(t *testing.T) {
			healthy := false
			primaryCalls, fallbackCalls := 0, 0
			primary := clientFunc(func(context.Context) ([]Mint, error) {
				primaryCalls++
				if healthy {
					return []Mint{{URL: "https://primary"}}, nil
				}
				if failure == "fetch" {
					return nil, errors.New("down")
				}
				return nil, nil
			})
			d := NewDual(primary, clientFunc(func(context.Context) ([]Mint, error) {
				fallbackCalls++
				return []Mint{{URL: "https://fallback"}}, nil
			}), time.Hour)
			if failure == "probe" {
				d.primary = probeClient{primary, errors.New("probe failed")}
			}
			if err := d.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			mints, err := d.Mints(context.Background())
			if err != nil || len(mints) != 1 || mints[0].Source != "8333" || mints[0].URL != "https://fallback" {
				t.Fatalf("fallback: %v %v", mints, err)
			}
			if failure == "probe" && primaryCalls != 0 {
				t.Fatal("fetched after failed probe")
			}
			healthy = true
			d.primary = primary
			if err := d.RunOnce(context.Background()); err != nil {
				t.Fatal(err)
			}
			mints, err = d.Mints(context.Background())
			if err != nil || mints[0].Source != "ucash" || mints[0].URL != "https://primary" || fallbackCalls != 1 {
				t.Fatalf("recovery: %v %v calls=%d", mints, err, fallbackCalls)
			}
		})
	}
}

func TestDualMintsNeverWaitsForPrimary(t *testing.T) {
	entered, release, done := make(chan struct{}), make(chan struct{}), make(chan struct{})
	d := NewDual(clientFunc(func(context.Context) ([]Mint, error) {
		close(entered)
		<-release
		return []Mint{{URL: "https://new"}}, nil
	}), nil, time.Hour)
	defer func() { close(release); <-done }()
	go func() { defer close(done); _ = d.RunOnce(context.Background()) }()
	<-entered
	result := make(chan error, 1)
	go func() { _, err := d.Mints(context.Background()); result <- err }()
	select {
	case err := <-result:
		if !errors.Is(err, ErrUnavailable) {
			t.Fatalf("cold read: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cold read blocked")
	}
	d.mu.Lock()
	d.cached = []Mint{{URL: "https://old"}}
	d.fetchedAt = time.Now().Add(-2 * time.Hour)
	d.mu.Unlock()
	go func() {
		m, err := d.Mints(context.Background())
		if err == nil && m[0].URL != "https://old" {
			err = errors.New("wrong snapshot")
		}
		result <- err
	}()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stale read blocked")
	}
}

func TestDualStaleExpiryAndSnapshotIsolation(t *testing.T) {
	uptime := 98.0
	d := NewDual(clientFunc(func(context.Context) ([]Mint, error) {
		return []Mint{{URL: "https://mint", Uptime24h: &uptime, Units: []string{"sat"}}}, nil
	}), nil, time.Hour)
	if err := d.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	mints, _ := d.Mints(context.Background())
	*mints[0].Uptime24h = 0
	mints[0].Units[0] = "bad"
	again, _ := d.Mints(context.Background())
	if *again[0].Uptime24h != 98 || again[0].Units[0] != "sat" {
		t.Fatal("caller mutated snapshot")
	}
	d.primary = clientFunc(func(context.Context) ([]Mint, error) { return nil, errors.New("down") })
	at := d.fetchedAt
	if err := d.RunOnce(context.Background()); err == nil {
		t.Fatal("expected failure")
	}
	if d.fetchedAt != at {
		t.Fatal("failed pass renewed stale data")
	}
	if _, err := d.Mints(context.Background()); err != nil {
		t.Fatal("lost stale snapshot")
	}
	d.fetchedAt = time.Now().Add(-24*time.Hour - time.Second)
	if _, err := d.Mints(context.Background()); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("expired: %v", err)
	}
}

func TestDualLegacyCacheCannotRenewStaleSnapshot(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls > 1 {
			w.WriteHeader(503)
			return
		}
		fmt.Fprint(w, `[{"url":"https://mint"}]`)
	}))
	defer server.Close()
	legacy := NewHTTPClient(server.URL)
	if _, err := legacy.Mints(context.Background()); err != nil {
		t.Fatal(err)
	}
	d := NewDual(nil, legacy, time.Hour)
	if err := d.RunOnce(context.Background()); err == nil {
		t.Fatal("legacy cache counted as a successful refresh")
	}
	if calls != 2 {
		t.Fatalf("fetch count = %d", calls)
	}
}

func TestDualRunWarmsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	calls := make(chan struct{}, 2)
	d := NewDual(clientFunc(func(context.Context) ([]Mint, error) { calls <- struct{}{}; return []Mint{{URL: "https://mint"}}, nil }), nil, 10*time.Millisecond)
	done := make(chan struct{})
	go func() { defer close(done); d.Run(ctx) }()
	defer cancel()
	for i := 0; i < 2; i++ {
		select {
		case <-calls:
		case <-time.After(time.Second):
			t.Fatal("Run did not refresh")
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not stop")
	}
}

func TestDualLogsSourceTransitionsOnce(t *testing.T) {
	var logs bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logs, nil)))
	defer slog.SetDefault(previous)
	healthy := false
	d := NewDual(clientFunc(func(context.Context) ([]Mint, error) {
		if healthy {
			return []Mint{{URL: "https://primary"}}, nil
		}
		return nil, errors.New("down")
	}), clientFunc(func(context.Context) ([]Mint, error) { return []Mint{{URL: "https://fallback"}}, nil }), time.Hour)
	for i := 0; i < 4; i++ {
		healthy = i >= 2
		if err := d.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if got := strings.Count(logs.String(), `"msg":"auditor.source.changed"`); got != 2 {
		t.Fatalf("transitions=%d logs=%s", got, logs.String())
	}
	if got := strings.Count(logs.String(), `"msg":"auditor.refresh"`); got != 4 {
		t.Fatalf("refreshes=%d", got)
	}
	if !strings.Contains(logs.String(), `"uptimeEnriched":0`) || !strings.Contains(logs.String(), `"mints":1`) {
		t.Fatalf("missing refresh fields: %s", logs.String())
	}
}
