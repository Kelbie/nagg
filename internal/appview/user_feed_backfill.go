package appview

import (
	"context"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/relayquery"
)

type UserFeedBackfiller interface {
	BackfillUserFeed(context.Context, string, uint64) error
}

type EventBackfiller interface {
	BackfillEvents(context.Context, []string) error
}

type ProfileBackfiller interface {
	BackfillProfiles(context.Context, []string) error
}

type EngagementBackfiller interface {
	BackfillEngagement(context.Context, []string) error
}

type ThreadBackfiller interface {
	BackfillThread(context.Context, string, int) error
}

type FollowBackfiller interface {
	BackfillFollows(context.Context, string) error
}

type AppViewBackfiller interface {
	UserFeedBackfiller
	EventBackfiller
	ProfileBackfiller
	EngagementBackfiller
	ThreadBackfiller
	FollowBackfiller
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
	ThreadLimit     int
	FollowLimit     int
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
	if cfg.ThreadLimit <= 0 {
		cfg.ThreadLimit = 1000
	}
	if cfg.FollowLimit <= 0 {
		cfg.FollowLimit = 1000
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
	if b == nil || b.store == nil || pubkey == "" || !b.shouldAttempt("feed:"+pubkey) {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	authorLimit := maxInt(b.cfg.AuthorLimit, int(limit))
	authorEvents, err := b.query.Query(queryCtx, map[string]any{
		"authors": []string{pubkey},
		"kinds":   []int{0, 1, 6, 16},
		"limit":   authorLimit,
	}, timeout)
	if err != nil {
		return err
	}
	collector.add(authorEvents)

	b.queryEventsByID(queryCtx, collector, b.attemptValues("events", targetEventIDs(collector.records)), timeout)
	b.queryEngagement(queryCtx, collector, b.attemptValues("engagement", targetEventIDs(collector.records)), timeout)
	b.queryProfiles(queryCtx, collector, b.attemptValues("profiles", profilePubkeys(collector.records)), timeout)
	return b.insertCollected(baseCtx, "user feed backfill inserted", collector.records, "pubkey", pubkey)
}

func (b *RelayUserFeedBackfiller) BackfillEvents(ctx context.Context, ids []string) error {
	if b == nil || b.store == nil {
		return nil
	}
	ids = b.attemptValues("events", validHexIDs(ids))
	if len(ids) == 0 {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryEventsByID(queryCtx, collector, ids, timeout)
	b.queryProfiles(queryCtx, collector, profilePubkeys(collector.records), timeout)
	return b.insertCollected(baseCtx, "event backfill inserted", collector.records, "ids", len(ids))
}

func (b *RelayUserFeedBackfiller) BackfillProfiles(ctx context.Context, pubkeys []string) error {
	if b == nil || b.store == nil {
		return nil
	}
	pubkeys = b.attemptValues("profiles", validPubkeys(pubkeys))
	if len(pubkeys) == 0 {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryProfiles(queryCtx, collector, pubkeys, timeout)
	return b.insertCollected(baseCtx, "profile backfill inserted", collector.records, "pubkeys", len(pubkeys))
}

func (b *RelayUserFeedBackfiller) BackfillEngagement(ctx context.Context, ids []string) error {
	if b == nil || b.store == nil {
		return nil
	}
	ids = b.attemptValues("engagement", validHexIDs(ids))
	if len(ids) == 0 {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryEngagement(queryCtx, collector, ids, timeout)
	b.queryProfiles(queryCtx, collector, profilePubkeys(collector.records), timeout)
	return b.insertCollected(baseCtx, "engagement backfill inserted", collector.records, "ids", len(ids))
}

func (b *RelayUserFeedBackfiller) BackfillThread(ctx context.Context, id string, limit int) error {
	ids := validHexIDs([]string{id})
	if len(ids) != 1 {
		return nil
	}
	id = ids[0]
	if b == nil || b.store == nil || !b.shouldAttempt("thread:"+id) {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	b.queryEventsByID(queryCtx, collector, b.attemptValues("events", []string{id}), timeout)
	b.queryThreadReferences(queryCtx, collector, id, threadLimit(limit, b.cfg.ThreadLimit), timeout)
	threadIDs := targetEventIDs(collector.records)
	if len(threadIDs) == 0 {
		threadIDs = []string{id}
	}
	b.queryEngagement(queryCtx, collector, b.attemptValues("engagement", threadIDs), timeout)
	b.queryProfiles(queryCtx, collector, b.attemptValues("profiles", profilePubkeys(collector.records)), timeout)
	return b.insertCollected(baseCtx, "thread backfill inserted", collector.records, "root", id)
}

func (b *RelayUserFeedBackfiller) BackfillFollows(ctx context.Context, pubkey string) error {
	pubkeys := validPubkeys([]string{pubkey})
	if len(pubkeys) != 1 {
		return nil
	}
	pubkey = pubkeys[0]
	if b == nil || b.store == nil || !b.shouldAttempt("follows:"+pubkey) {
		return nil
	}
	queryCtx, baseCtx, cancelQuery, timeout := b.queryContext(ctx)
	defer cancelQuery()

	collector := newEventCollector()
	contactLists, err := b.query.Query(queryCtx, map[string]any{
		"authors": []string{pubkey},
		"kinds":   []int{3},
		"limit":   3,
	}, timeout)
	if err != nil {
		slog.Debug("follow contact-list backfill failed", "pubkey", pubkey, "error", err)
	}
	collector.add(contactLists)

	followers, err := b.query.Query(queryCtx, map[string]any{
		"#p":    []string{pubkey},
		"kinds": []int{3},
		"limit": b.cfg.FollowLimit,
	}, timeout)
	if err != nil {
		slog.Debug("follower backfill failed", "pubkey", pubkey, "error", err)
	}
	collector.add(followers)
	b.queryProfiles(queryCtx, collector, b.attemptValues("profiles", profilePubkeys(collector.records)), timeout)
	return b.insertCollected(baseCtx, "follow graph backfill inserted", collector.records, "pubkey", pubkey)
}

func (b *RelayUserFeedBackfiller) queryContext(ctx context.Context) (context.Context, context.Context, context.CancelFunc, time.Duration) {
	timeout := b.cfg.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	baseCtx := context.WithoutCancel(ctx)
	queryCtx, cancelQuery := context.WithTimeout(baseCtx, timeout)
	return queryCtx, baseCtx, cancelQuery, timeout
}

func (b *RelayUserFeedBackfiller) queryEventsByID(ctx context.Context, collector *eventCollector, ids []string, timeout time.Duration) {
	for _, batch := range chunks(validHexIDs(ids), 80) {
		if ctx.Err() != nil {
			return
		}
		events, err := b.query.Query(ctx, map[string]any{
			"ids":   batch,
			"limit": len(batch) * 2,
		}, timeout)
		if err != nil {
			slog.Debug("event id backfill failed", "ids", len(batch), "error", err)
		}
		collector.add(events)
	}
}

func (b *RelayUserFeedBackfiller) queryEngagement(ctx context.Context, collector *eventCollector, ids []string, timeout time.Duration) {
	for _, batch := range chunks(validHexIDs(ids), 80) {
		if ctx.Err() != nil {
			return
		}
		events, err := b.query.Query(ctx, map[string]any{
			"#e":    batch,
			"kinds": []int{1, 6, 7, 16, 9735},
			"limit": b.cfg.EngagementLimit,
		}, timeout)
		if err != nil {
			slog.Debug("engagement backfill failed", "ids", len(batch), "error", err)
		}
		collector.add(events)
	}
}

func (b *RelayUserFeedBackfiller) queryProfiles(ctx context.Context, collector *eventCollector, pubkeys []string, timeout time.Duration) {
	for _, batch := range chunks(validPubkeys(pubkeys), 80) {
		if ctx.Err() != nil {
			return
		}
		profiles, err := b.query.Query(ctx, map[string]any{
			"authors": batch,
			"kinds":   []int{0},
			"limit":   len(batch) * 3,
		}, timeout)
		if err != nil {
			slog.Debug("profile backfill failed", "pubkeys", len(batch), "error", err)
		}
		collector.add(profiles)
	}
}

func (b *RelayUserFeedBackfiller) queryThreadReferences(ctx context.Context, collector *eventCollector, rootID string, limit int, timeout time.Duration) {
	visited := map[string]struct{}{}
	frontier := []string{rootID}
	for depth := 0; depth < 8 && len(frontier) > 0 && len(collector.records) < limit; depth++ {
		batch := takeUnvisited(visited, frontier, 80)
		if len(batch) == 0 || ctx.Err() != nil {
			return
		}
		remaining := limit - len(collector.records)
		if remaining <= 0 {
			return
		}
		events, err := b.query.Query(ctx, map[string]any{
			"#e":    batch,
			"kinds": []int{1, 1111},
			"limit": min(maxInt(remaining, 100), 500),
		}, timeout)
		if err != nil {
			slog.Debug("thread reference backfill failed", "depth", depth, "ids", len(batch), "error", err)
		}

		newIDs := collector.add(events)
		frontier = frontier[:0]
		frontier = append(frontier, newIDs...)
	}
}

func (b *RelayUserFeedBackfiller) insertCollected(ctx context.Context, message string, records map[string]chstore.EventRecord, attrs ...any) error {
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
	insertCtx, cancelInsert := context.WithTimeout(ctx, 10*time.Second)
	defer cancelInsert()
	if err := b.store.InsertEvents(insertCtx, out); err != nil {
		return err
	}
	attrs = append(attrs, "events", len(out))
	slog.Info(message, attrs...)
	return nil
}

func (b *RelayUserFeedBackfiller) shouldAttempt(key string) bool {
	now := time.Now()
	b.mu.Lock()
	defer b.mu.Unlock()
	if last, ok := b.attempts[key]; ok && now.Sub(last) < b.cfg.Cooldown {
		return false
	}
	b.attempts[key] = now
	return true
}

func (b *RelayUserFeedBackfiller) attemptValues(prefix string, values []string) []string {
	now := time.Now()
	out := make([]string, 0, len(values))
	b.mu.Lock()
	defer b.mu.Unlock()
	for _, value := range values {
		key := prefix + ":" + value
		if last, ok := b.attempts[key]; ok && now.Sub(last) < b.cfg.Cooldown {
			continue
		}
		b.attempts[key] = now
		out = append(out, value)
	}
	return out
}

type eventCollector struct {
	records map[string]chstore.EventRecord
}

func newEventCollector() *eventCollector {
	return &eventCollector{records: map[string]chstore.EventRecord{}}
}

func (c *eventCollector) add(events []relayquery.Event) []string {
	seen := time.Now().UTC()
	newIDs := make([]string, 0, len(events))
	for _, event := range events {
		if event.Event == nil {
			continue
		}
		if _, ok := c.records[event.Event.ID]; ok {
			continue
		}
		c.records[event.Event.ID] = chstore.EventRecord{
			Event: event.Event,
			Relay: event.Relay,
			Seen:  seen,
		}
		newIDs = append(newIDs, event.Event.ID)
	}
	return newIDs
}

func targetEventIDs(records map[string]chstore.EventRecord) []string {
	seen := map[string]struct{}{}
	for _, record := range records {
		event := record.Event
		if event == nil {
			continue
		}
		switch event.Kind {
		case 1, 1111:
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

func validHexIDs(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !nostr.IsValid32ByteHex(value) {
			continue
		}
		seen[value] = struct{}{}
	}
	return sortedKeys(seen)
}

func validPubkeys(values []string) []string {
	seen := map[string]struct{}{}
	for _, value := range values {
		value = strings.TrimSpace(value)
		if !nostr.IsValidPublicKey(value) {
			continue
		}
		seen[value] = struct{}{}
	}
	return sortedKeys(seen)
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

func takeUnvisited(visited map[string]struct{}, ids []string, max int) []string {
	out := make([]string, 0, max)
	for _, id := range ids {
		if _, ok := visited[id]; ok {
			continue
		}
		visited[id] = struct{}{}
		out = append(out, id)
		if len(out) == max {
			break
		}
	}
	return out
}

func threadLimit(requested int, configured int) int {
	if requested <= 0 || requested > 2000 {
		requested = 1000
	}
	if configured <= 0 {
		return requested
	}
	return min(requested, configured)
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}
