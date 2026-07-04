package relevance

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

type mockStore struct {
	mu        sync.Mutex
	touched   []string
	exempt    []string
	exemptErr error
}

func (m *mockStore) TouchKnownViewer(_ context.Context, pubkey string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.touched = append(m.touched, pubkey)
	return nil
}

func (m *mockStore) ExemptPubkeys(_ context.Context) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.exempt, m.exemptErr
}

func (m *mockStore) touchedCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.touched)
}

func TestExemptFailsOpenBeforeFirstRefresh(t *testing.T) {
	tracker := NewTracker(&mockStore{}, nil)
	if !tracker.Exempt(strings.Repeat("a", 64)) {
		t.Fatal("Exempt must fail OPEN (true) before the first successful refresh")
	}
}

func TestExemptAfterRefreshMatchesSetOnly(t *testing.T) {
	known := strings.Repeat("a", 64)
	store := &mockStore{exempt: []string{known}}
	tracker := NewTracker(store, nil)
	tracker.refresh(context.Background())

	if !tracker.Exempt(known) {
		t.Fatal("known pubkey should be exempt")
	}
	if tracker.Exempt(strings.Repeat("b", 64)) {
		t.Fatal("unknown pubkey should not be exempt after a successful refresh")
	}
}

func TestRefreshFailureKeepsPreviousSnapshot(t *testing.T) {
	known := strings.Repeat("a", 64)
	store := &mockStore{exempt: []string{known}}
	tracker := NewTracker(store, nil)
	tracker.refresh(context.Background())

	store.mu.Lock()
	store.exemptErr = errors.New("clickhouse down")
	store.mu.Unlock()
	tracker.refresh(context.Background())

	if !tracker.Exempt(known) {
		t.Fatal("a failed refresh must keep the previous snapshot")
	}
	if tracker.Exempt(strings.Repeat("b", 64)) {
		t.Fatal("a failed refresh must not fall back to fail-open")
	}
}

func TestTouchThrottlesAndValidates(t *testing.T) {
	store := &mockStore{}
	tracker := NewTracker(store, nil)

	// Invalid input never reaches the store.
	tracker.Touch("npub1notheix")
	tracker.Touch("")

	pubkey := strings.Repeat("c", 64)
	for i := 0; i < 5; i++ {
		tracker.Touch(pubkey)
	}
	// Touch inserts asynchronously; wait for the (single) insert to land.
	deadline := time.Now().Add(2 * time.Second)
	for store.touchedCount() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := store.touchedCount(); got != 1 {
		t.Fatalf("5 touches within the throttle window should insert once; got %d", got)
	}
}
