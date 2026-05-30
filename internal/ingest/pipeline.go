package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/firehose"
)

type Config struct {
	BatchSize     int
	FlushInterval time.Duration
	QueueSize     int
	VerifyEvents  bool
}

type Store interface {
	InsertEvents(context.Context, []clickhouse.EventRecord) error
}

type Pipeline struct {
	store Store
	cfg   Config
	batch []clickhouse.EventRecord
}

func New(store Store, cfg Config) *Pipeline {
	return &Pipeline{
		store: store,
		cfg:   cfg,
		batch: make([]clickhouse.EventRecord, 0, cfg.BatchSize),
	}
}

func (p *Pipeline) Run(ctx context.Context, in <-chan firehose.RelayEvent) error {
	ticker := time.NewTicker(p.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return p.Flush(context.Background())
		case <-ticker.C:
			if err := p.Flush(ctx); err != nil {
				return err
			}
		case relayEvent, ok := <-in:
			if !ok {
				return p.Flush(ctx)
			}
			if err := p.add(relayEvent); err != nil {
				slog.Debug("event rejected", "relay", relayEvent.Relay, "error", err)
				continue
			}
			if len(p.batch) >= p.cfg.BatchSize {
				if err := p.Flush(ctx); err != nil {
					return err
				}
			}
		}
	}
}

func (p *Pipeline) Flush(ctx context.Context) error {
	if len(p.batch) == 0 {
		return nil
	}

	start := time.Now()
	if err := p.store.InsertEvents(ctx, p.batch); err != nil {
		return err
	}
	slog.Info("inserted event batch", "events", len(p.batch), "duration", time.Since(start))
	p.batch = p.batch[:0]
	return nil
}

func (p *Pipeline) add(relayEvent firehose.RelayEvent) error {
	event := relayEvent.Event
	if event == nil {
		return fmt.Errorf("nil event")
	}
	if err := validateShape(event); err != nil {
		return err
	}
	if p.cfg.VerifyEvents {
		if !event.CheckID() {
			return fmt.Errorf("invalid id")
		}
		ok, err := event.CheckSignature()
		if err != nil {
			return fmt.Errorf("signature check failed: %w", err)
		}
		if !ok {
			return fmt.Errorf("invalid signature")
		}
	}

	p.batch = append(p.batch, clickhouse.EventRecord{
		Event: event,
		Relay: relayEvent.Relay,
		Seen:  time.Now().UTC(),
	})
	return nil
}

func validateShape(e *nostr.Event) error {
	if len(e.ID) != 64 {
		return fmt.Errorf("invalid id length")
	}
	if len(e.PubKey) != 64 {
		return fmt.Errorf("invalid pubkey length")
	}
	if len(e.Sig) != 128 {
		return fmt.Errorf("invalid signature length")
	}
	if len(e.Tags) > 50_000 {
		return fmt.Errorf("too many tags")
	}
	if len(e.Content) > 1_000_000 {
		return fmt.Errorf("content too large")
	}
	return nil
}
