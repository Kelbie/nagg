package clickhouse

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestOpenWithRetryRetriesUntilSuccess(t *testing.T) {
	attempts := 0
	store, err := openWithRetry(
		context.Background(),
		Config{},
		openRetryConfig{Attempts: 3, InitialDelay: time.Nanosecond, MaxDelay: time.Nanosecond},
		nil,
		func(context.Context, Config) (*Store, error) {
			attempts++
			if attempts < 3 {
				return nil, errors.New("connection reset by peer")
			}
			return &Store{}, nil
		},
	)
	if err != nil {
		t.Fatalf("openWithRetry returned error: %v", err)
	}
	if store == nil {
		t.Fatal("openWithRetry returned nil store")
	}
	if attempts != 3 {
		t.Fatalf("attempts = %d, want 3", attempts)
	}
}

func TestOpenWithRetryReturnsLastError(t *testing.T) {
	wantErr := errors.New("clickhouse still unavailable")
	attempts := 0
	store, err := openWithRetry(
		context.Background(),
		Config{},
		openRetryConfig{Attempts: 2, InitialDelay: time.Nanosecond, MaxDelay: time.Nanosecond},
		nil,
		func(context.Context, Config) (*Store, error) {
			attempts++
			return nil, wantErr
		},
	)
	if store != nil {
		t.Fatalf("store = %+v, want nil", store)
	}
	if !errors.Is(err, wantErr) {
		t.Fatalf("error = %v, want %v", err, wantErr)
	}
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2", attempts)
	}
}

func TestOpenWithRetryStopsWhenContextIsCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	attempts := 0
	store, err := openWithRetry(
		ctx,
		Config{},
		openRetryConfig{Attempts: 3, InitialDelay: time.Hour, MaxDelay: time.Hour},
		nil,
		func(context.Context, Config) (*Store, error) {
			attempts++
			cancel()
			return nil, errors.New("connection reset by peer")
		},
	)
	if store != nil {
		t.Fatalf("store = %+v, want nil", store)
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if attempts != 1 {
		t.Fatalf("attempts = %d, want 1", attempts)
	}
}
