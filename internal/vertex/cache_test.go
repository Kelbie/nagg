package vertex

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

func TestCachedCallSingleFlight(t *testing.T) {
	var calls atomic.Int32
	started := make(chan struct{}, 1)
	release := make(chan struct{})

	cache, err := newCachedCall(
		func(ctx context.Context, key string) (string, error) {
			calls.Add(1)
			started <- struct{}{}
			select {
			case <-release:
				return "value:" + key, nil
			case <-ctx.Done():
				return "", ctx.Err()
			}
		},
		func(key string) string { return key },
		func(value string) bool { return value != "" },
		time.Hour,
		24*time.Hour,
		10,
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first := make(chan string, 1)
	second := make(chan string, 1)
	errs := make(chan error, 2)

	go func() {
		value, _, err := cache.Get(ctx, "same")
		if err != nil {
			errs <- err
			return
		}
		first <- value
	}()
	<-started
	go func() {
		value, _, err := cache.Get(ctx, "same")
		if err != nil {
			errs <- err
			return
		}
		second <- value
	}()

	time.Sleep(10 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("calls before release = %d, want 1", calls.Load())
	}
	close(release)

	if got := <-first; got != "value:same" {
		t.Fatalf("first = %q", got)
	}
	if got := <-second; got != "value:same" {
		t.Fatalf("second = %q", got)
	}
	select {
	case err := <-errs:
		t.Fatal(err)
	default:
	}
	if calls.Load() != 1 {
		t.Fatalf("calls = %d, want 1", calls.Load())
	}
}

func TestCachedCallStaleWhileRevalidate(t *testing.T) {
	var calls atomic.Int32
	cache, err := newCachedCall(
		func(context.Context, string) (string, error) {
			return fmt.Sprintf("value:%d", calls.Add(1)), nil
		},
		func(key string) string { return key },
		func(value string) bool { return value != "" },
		10*time.Millisecond,
		time.Hour,
		10,
	)
	if err != nil {
		t.Fatal(err)
	}

	value, fromCache, err := cache.Get(context.Background(), "same")
	if err != nil {
		t.Fatal(err)
	}
	if fromCache || value != "value:1" {
		t.Fatalf("initial value = %q fromCache=%v", value, fromCache)
	}

	time.Sleep(20 * time.Millisecond)
	value, fromCache, err = cache.Get(context.Background(), "same")
	if err != nil {
		t.Fatal(err)
	}
	if !fromCache || value != "value:1" {
		t.Fatalf("stale value = %q fromCache=%v", value, fromCache)
	}

	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if calls.Load() < 2 {
		t.Fatalf("background refresh calls = %d, want at least 2", calls.Load())
	}
}
