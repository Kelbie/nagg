package ingest

import (
	"context"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/safego"
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

	// verifyOnce/verified: the signature-verification stage is created ONCE per
	// Pipeline and reused across Run calls — the callers restart Run after
	// insert-failure bursts, and rebuilding the stage per call would strand the
	// old workers on the shared input channel, silently swallowing events.
	verifyOnce sync.Once
	verified   <-chan firehose.RelayEvent
}

func New(store Store, cfg Config) *Pipeline {
	return &Pipeline{
		store: store,
		cfg:   cfg,
		batch: make([]clickhouse.EventRecord, 0, cfg.BatchSize),
	}
}

func (p *Pipeline) Run(ctx context.Context, in <-chan firehose.RelayEvent) error {
	// Signature verification is secp256k1 work per event; done inline on this
	// single consumer goroutine it was the ingest bottleneck — and when inserts
	// slowed, the combination stalled the channel, backpressured the firehose
	// off its relays, and triggered the reconnect replay storm. A small worker
	// pool keeps verification off the batching goroutine.
	source := in
	if p.cfg.VerifyEvents {
		p.verifyOnce.Do(func() {
			p.verified = verifyStage(ctx, in, verifyWorkerCount())
		})
		source = p.verified
	}

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
		case relayEvent, ok := <-source:
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

func verifyWorkerCount() int {
	workers := runtime.GOMAXPROCS(0) - 1
	if workers < 1 {
		workers = 1
	}
	if workers > 4 {
		workers = 4
	}
	return workers
}

// verifyStage fans events across `workers` goroutines that drop shape-invalid
// and signature-invalid events, forwarding the rest. Event order is not
// preserved — inserts have no ordering dependency (dedup is by id downstream).
// The output channel closes when the input closes and all workers drain.
func verifyStage(ctx context.Context, in <-chan firehose.RelayEvent, workers int) <-chan firehose.RelayEvent {
	out := make(chan firehose.RelayEvent, workers*2)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer safego.Recover("ingest.verify")
			defer wg.Done()
			for relayEvent := range in {
				// Per-EVENT recovery: a panic inside signature verification
				// (malformed relay data) must poison only that event. If it
				// killed the worker, the pool would drain one panic at a time
				// until out closed and Run returned nil — which the restart
				// loops treat as terminal, silently wedging ingestion.
				if err := verifyEventSafe(relayEvent.Event); err != nil {
					slog.Debug("event rejected", "relay", relayEvent.Relay, "error", err)
					continue
				}
				select {
				case out <- relayEvent:
				case <-ctx.Done():
					return
				}
			}
		}()
	}
	go func() {
		defer safego.Recover("ingest.verify_close")
		wg.Wait()
		close(out)
	}()
	return out
}

// verifyEventSafe converts a verification panic into an error so one poisoned
// event can never take a verify worker down.
func verifyEventSafe(event *nostr.Event) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("verification panicked: %v", r)
		}
	}()
	return verifyEvent(event)
}

func verifyEvent(event *nostr.Event) error {
	if event == nil {
		return fmt.Errorf("nil event")
	}
	if err := validateShape(event); err != nil {
		return err
	}
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
	return nil
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
	if !p.cfg.VerifyEvents {
		// The verify stage already shape-checked when enabled.
		if err := validateShape(event); err != nil {
			return err
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
