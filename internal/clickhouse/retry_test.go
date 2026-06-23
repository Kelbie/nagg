package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestRetryTransientReadRetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := retryTransientRead(context.Background(), 3, time.Millisecond, func() error {
		calls++
		if calls < 3 {
			return errors.New("read tcp 1.2.3.4:9000: connection reset by peer")
		}
		return nil
	})
	if err != nil {
		t.Fatalf("err = %v, want nil after recovery", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3 (2 transient + 1 success)", calls)
	}
}

func TestRetryTransientReadGivesUpAfterAttempts(t *testing.T) {
	calls := 0
	transient := errors.New("broken pipe")
	err := retryTransientRead(context.Background(), 3, time.Millisecond, func() error {
		calls++
		return transient
	})
	if !errors.Is(err, transient) {
		t.Fatalf("err = %v, want the transient error surfaced after exhausting retries", err)
	}
	if calls != 3 {
		t.Fatalf("calls = %d, want 3 (capped attempts)", calls)
	}
}

func TestRetryTransientReadStopsOnNonTransient(t *testing.T) {
	calls := 0
	fatal := errors.New("DB::Exception: syntax error")
	err := retryTransientRead(context.Background(), 3, time.Millisecond, func() error {
		calls++
		return fatal
	})
	if !errors.Is(err, fatal) {
		t.Fatalf("err = %v, want the non-transient error returned immediately", err)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (no retry on a non-transient error)", calls)
	}
}

func TestRetryTransientReadStopsWhenContextCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	// A long backoff would make this hang if the context weren't honoured.
	err := retryTransientRead(ctx, 3, time.Hour, func() error {
		calls++
		return errors.New("unexpected EOF")
	})
	if err == nil {
		t.Fatal("err = nil, want the last transient error")
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1 (a cancelled context must not schedule another attempt)", calls)
	}
}
