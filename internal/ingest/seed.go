package ingest

import (
	"context"
	"log/slog"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

// Seed fetch: a live firehose only ever sees NEW publications, so kinds whose
// events are long-lived and rarely republished (parameterized-replaceable app
// data being the canonical case) would otherwise take months to accumulate.
// For each configured kind whose local store is EMPTY, fetch the existing
// history from the relay set once, paginating by created_at. The empty-store
// gate makes restarts cheap and re-seeding automatic only when a kind is
// genuinely missing; after the seed, the firehose keeps it current.
//
// Inserts go through Store.InsertEvents directly — like every on-demand
// backfill, seeded history is definitionally relevant and bypasses the
// per-author ingest caps (which target firehose flooding, not history).

const (
	seedPageLimit = 500
	seedMaxPages  = 40 // hard cap per kind: 20k events, far above any expected seed
	seedQueryTime = 15 * time.Second
	seedPagePause = 500 * time.Millisecond
)

// SeedStore is the store subset the seed fetch needs.
type SeedStore interface {
	CountEventsOfKind(ctx context.Context, kind int) (uint64, error)
	InsertEvents(ctx context.Context, records []chstore.EventRecord) error
}

// SeedFetch seeds each empty kind from the relay set. Errors are logged, not
// fatal — a failed seed retries on the next boot (the store is still empty).
func SeedFetch(ctx context.Context, store SeedStore, relays []string, kinds []int, logger *slog.Logger) {
	if len(kinds) == 0 || len(relays) == 0 {
		return
	}
	client := relayquery.Client{Relays: relays, Health: relayquery.NewRelayHealth()}

	for _, kind := range kinds {
		count, err := store.CountEventsOfKind(ctx, kind)
		if err != nil {
			logger.Warn("seed fetch: count failed", "kind", kind, "error", err)
			continue
		}
		if count > 0 {
			logger.Info("seed fetch: kind already populated", "kind", kind, "count", count)
			continue
		}

		total := 0
		until := time.Now().Unix() + 60
		for page := 0; page < seedMaxPages; page++ {
			if ctx.Err() != nil {
				return
			}
			events, err := client.Query(ctx, map[string]any{
				"kinds": []int{kind},
				"limit": seedPageLimit,
				"until": until,
			}, seedQueryTime)
			if err != nil {
				logger.Warn("seed fetch: relay query failed", "kind", kind, "error", err)
			}
			if len(events) == 0 {
				break
			}

			records := make([]chstore.EventRecord, 0, len(events))
			oldest := until
			for _, ev := range events {
				if ev.Event == nil {
					continue
				}
				created := int64(ev.Event.CreatedAt)
				if created < oldest {
					oldest = created
				}
				records = append(records, chstore.EventRecord{
					Event: ev.Event,
					Relay: ev.Relay,
					Seen:  time.Now().UTC(),
				})
			}
			if len(records) == 0 {
				break
			}
			if err := store.InsertEvents(ctx, records); err != nil {
				logger.Warn("seed fetch: insert failed", "kind", kind, "error", err)
				break
			}
			total += len(records)

			if oldest >= until || len(events) < seedPageLimit/2 {
				break // exhausted (relays returned a short/unprogressing page)
			}
			until = oldest - 1
			select {
			case <-ctx.Done():
				return
			case <-time.After(seedPagePause):
			}
		}
		logger.Info("seed fetch: kind seeded", "kind", kind, "events", total)
	}
}
