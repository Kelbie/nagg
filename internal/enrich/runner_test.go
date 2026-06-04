package enrich

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestRunnerRunOnceWritesAnnotationsAndAdvancesPastEventErrors(t *testing.T) {
	events := []Event{
		{
			ID:        hexID("1"),
			PubKey:    hexID("a"),
			Kind:      1,
			CreatedAt: time.Unix(10, 0).UTC(),
			Content:   "bitcoin nostr",
		},
		{
			ID:        hexID("2"),
			PubKey:    hexID("b"),
			Kind:      1,
			CreatedAt: time.Unix(11, 0).UTC(),
			Content:   "bad",
		},
	}
	store := &fakeStore{events: events}
	processor := fakeProcessor{
		task: TaskTopics,
		results: []ProcessResult{
			{Annotation: Annotation{Tags: []Tag{{Key: "topic", Value: "crypto.bitcoin"}}, ModelVersion: "test-v1"}},
			{Err: errors.New("model refused event")},
		},
	}
	runner := NewRunner(store, []Processor{processor}, RunnerConfig{BatchSize: 2}, discardLogger())
	now := time.Unix(100, 0).UTC()
	runner.now = func() time.Time { return now }

	processed, err := runner.RunOnce(context.Background(), processor)
	if err != nil {
		t.Fatal(err)
	}
	if processed != 2 {
		t.Fatalf("processed = %d, want 2", processed)
	}
	if store.fetchLimit != 2 {
		t.Fatalf("fetch limit = %d, want 2", store.fetchLimit)
	}
	if len(store.written) != 1 {
		t.Fatalf("written annotations = %d, want 1", len(store.written))
	}
	written := store.written[0]
	if written.Event.ID != events[0].ID {
		t.Fatalf("written event id = %q, want %q", written.Event.ID, events[0].ID)
	}
	if !written.ComputedAt.Equal(now) {
		t.Fatalf("computed_at = %s, want %s", written.ComputedAt, now)
	}
	if store.saved.Task != TaskTopics {
		t.Fatalf("saved task = %q", store.saved.Task)
	}
	if store.saved.Cursor.EventID != events[1].ID || !store.saved.Cursor.CreatedAt.Equal(events[1].CreatedAt) {
		t.Fatalf("saved cursor = %+v, want second event", store.saved.Cursor)
	}
	if store.saved.Processed != 1 || store.saved.Failed != 1 {
		t.Fatalf("saved counts processed=%d failed=%d, want 1/1", store.saved.Processed, store.saved.Failed)
	}
}

func TestRunnerRunOnceDoesNotSaveWatermarkWhenWriteFails(t *testing.T) {
	events := []Event{{
		ID:        hexID("1"),
		PubKey:    hexID("a"),
		Kind:      1,
		CreatedAt: time.Unix(10, 0).UTC(),
		Content:   "bitcoin",
	}}
	store := &fakeStore{events: events, writeErr: errors.New("clickhouse unavailable")}
	processor := fakeProcessor{
		task:    TaskTopics,
		results: []ProcessResult{{Annotation: Annotation{Tags: []Tag{{Key: "topic", Value: "crypto.bitcoin"}}}}},
	}
	runner := NewRunner(store, []Processor{processor}, RunnerConfig{BatchSize: 1}, discardLogger())

	_, err := runner.RunOnce(context.Background(), processor)
	if err == nil {
		t.Fatal("expected error")
	}
	if store.saveCalls != 0 {
		t.Fatalf("save calls = %d, want 0", store.saveCalls)
	}
}

type fakeStore struct {
	state      State
	stateOK    bool
	events     []Event
	fetchFrom  Cursor
	fetchLimit int
	written    []Annotation
	writeErr   error
	saved      State
	saveCalls  int
}

func (s *fakeStore) LoadEnrichmentState(context.Context, string) (State, bool, error) {
	return s.state, s.stateOK, nil
}

func (s *fakeStore) FetchEnrichmentEvents(_ context.Context, cursor Cursor, limit int) ([]Event, error) {
	s.fetchFrom = cursor
	s.fetchLimit = limit
	return s.events, nil
}

func (s *fakeStore) WriteEnrichmentAnnotations(_ context.Context, annotations []Annotation) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.written = append(s.written, annotations...)
	return nil
}

func (s *fakeStore) SaveEnrichmentState(_ context.Context, state State) error {
	s.saved = state
	s.saveCalls++
	return nil
}

type fakeProcessor struct {
	task    string
	results []ProcessResult
}

func (p fakeProcessor) Task() string {
	return p.task
}

func (p fakeProcessor) ProcessBatch(context.Context, []Event) ([]ProcessResult, error) {
	return p.results, nil
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func hexID(suffix string) string {
	return "000000000000000000000000000000000000000000000000000000000000000" + suffix
}
