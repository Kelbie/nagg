package ingest

import (
	"context"
	"log/slog"
	"testing"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

type seedStoreFake struct {
	counts  map[int]uint64
	inserts int
}

func (s *seedStoreFake) CountEventsOfKind(_ context.Context, kind int) (uint64, error) {
	return s.counts[kind], nil
}
func (s *seedStoreFake) InsertEvents(_ context.Context, records []chstore.EventRecord) error {
	s.inserts += len(records)
	return nil
}

func TestSeedFetchSkipsPopulatedKindsAndEmptyConfig(t *testing.T) {
	store := &seedStoreFake{counts: map[int]uint64{38000: 12}}
	// Populated kind: no relay round-trips happen because the page loop is
	// never entered — with no relays configured the whole call is a no-op too.
	SeedFetch(context.Background(), store, nil, []int{38000}, slog.Default())
	SeedFetch(context.Background(), store, []string{"wss://x"}, nil, slog.Default())
	if store.inserts != 0 {
		t.Fatalf("inserts = %d, want 0", store.inserts)
	}
}
