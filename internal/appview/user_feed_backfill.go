package appview

import (
	"context"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

type UserFeedBackfiller interface {
	BackfillUserFeed(context.Context, string, uint64) error
}

type eventInserter interface {
	InsertEvents(context.Context, []chstore.EventRecord) error
}

type UserFeedBackfillConfig struct {
	Relays          []string
	ReadLimit       int64
	Cooldown        time.Duration
	Timeout         time.Duration
	AuthorLimit     int
	EngagementLimit int
}

type RelayUserFeedBackfiller struct {
	store eventInserter
	query relayquery.Client
	cfg   UserFeedBackfillConfig

	mu       sync.Mutex
	attempts map[string]time.Time
}

func NewRelayUserFeedBackfiller(store eventInserter, cfg UserFeedBackfillConfig) *RelayUserFeedBackfiller {
	if cfg.Cooldown <= 0 {
		cfg.Cooldown = 5 * time.Minute
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.AuthorLimit <= 0 {
		cfg.AuthorLimit = 100
	}
	if cfg.EngagementLimit <= 0 {
		cfg.EngagementLimit = 1000
	}
	return &RelayUserFeedBackfiller{
		store: store,
		query: relayquery.Client{
			Relays:    cfg.Relays,
			ReadLimit: cfg.ReadLimit,
		},
		cfg:      cfg,
		attempts: map[string]time.Time{},
	}
}

func (b *RelayUserFeedBackfiller) BackfillUserFeed(ctx context.Context, pubkey string, limit uint64) error {
	if b == nil || b.store == nil || pubkey == "" || !b.shouldAttempt(pubkey) {
		return nil
	}
	timeout := b.cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), timeout)
	defer cancel()

	records := map[string]chstore.EventRecord{}
	add := func(events []relayquery.Event) {
		seen := time.Now().UTC()
		for _, event := range events {
			if event.Event == nil {
				continue
			}
			if _, ok := records[event.Event.ID]; ok {
				continue
			}
			records[event.Event.ID] = chstore.EventRecord{
				Event: event.Event,
				Relay: event.Relay,
				Seen:  seen,
			}
		}
	}

	authorLimit := maxInt(b.cfg.AuthorLimit, int(limit))
	authorEvents, err := b.query.Query(ctx, map[string]any{
		"authors": []string{pubkey},
		"kinds":   []int{0, 1, 6, 16},
		"limit":   authorLimit,
	}, timeout)
	if err != nil {
		return err
	}
	add(authorEvents)

	targetIDs := targetEventIDs(records)
	if len(targetIDs) > 0 {
		for _, batch := range chunks(targetIDs, 80) {
			originals, err := b.query.Query(ctx, map[string]any{
				"ids":   batch,
				"limit": len(batch) * 2,
			}, timeout)
			if err != nil {
				slog.Debug("user feed original backfill failed", "error", err)
			}
			add(originals)

			engagement, err := b.query.Query(ctx, map[string]any{
				"#e":    batch,
				"kinds": []int{1, 6, 7, 16, 9735},
				"limit": b.cfg.EngagementLimit,
			}, timeout)
			if err != nil {
				slog.Debug("user feed engagement backfill failed", "error", err)
			}
			add(engagement)
		}
	}

	for _, batch := range chunks(profilePubkeys(records), 80) {
		profiles, err := b.query.Query(ctx, map[string]any{
			"authors": batch,
			"kinds":   []int{0},
			"limit":   len(batch) * 3,
		}, timeout)
		if err != nil {
			slog.Debug("user feed profile backfill failed", "error", err)
		}
		add(profiles)
	}

	if len(records) == 0 {
		return nil
	}
	out := make([]chstore.EventRecord, 0, len(records))
	for _, record := range records {
		out = append(out, record)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Event.CreatedAt < out[j].Event.CreatedAt
	})
	if err := b.store.InsertEvents(ctx, out); err != nil {
		return err
	}
	slog.Info("user feed backfill inserted", "pubkey", pubkey, "events", len(out))
	return nil
}

func (b *RelayUserFeedBackfiller) shouldAttempt(pubkey string) bool {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if last, ok := b.attempts[pubkey]; ok && now.Sub(last) < b.cfg.Cooldown {
		return false
	}
	b.attempts[pubkey] = now
	return true
}

func targetEventIDs(records map[string]chstore.EventRecord) []string {
	seen := map[string]struct{}{}
	for _, record := range records {
		event := record.Event
		if event == nil {
			continue
		}
		switch event.Kind {
		case 1:
			seen[event.ID] = struct{}{}
		case 6, 16:
			if id := firstHexTag(event.Tags, "e"); id != "" {
				seen[id] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

func profilePubkeys(records map[string]chstore.EventRecord) []string {
	seen := map[string]struct{}{}
	for _, record := range records {
		event := record.Event
		if event == nil {
			continue
		}
		if nostr.IsValidPublicKey(event.PubKey) {
			seen[event.PubKey] = struct{}{}
		}
		for _, tag := range event.Tags {
			if len(tag) >= 2 && tag[0] == "p" && nostr.IsValidPublicKey(tag[1]) {
				seen[tag[1]] = struct{}{}
			}
		}
	}
	return sortedKeys(seen)
}

func firstHexTag(tags nostr.Tags, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func chunks[T any](items []T, size int) [][]T {
	if len(items) == 0 {
		return nil
	}
	if size <= 0 {
		size = len(items)
	}
	out := make([][]T, 0, (len(items)+size-1)/size)
	for len(items) > 0 {
		n := size
		if len(items) < n {
			n = len(items)
		}
		out = append(out, items[:n])
		items = items[n:]
	}
	return out
}

func sortedKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
