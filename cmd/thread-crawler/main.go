package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/config"
)

type crawler struct {
	relay  string
	events map[string]*nostr.Event
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	target := env("NAGG_THREAD_TARGET", "")
	if len(os.Args) > 1 {
		target = os.Args[1]
	}
	if target == "" {
		slog.Error("missing thread target", "usage", "go run ./cmd/thread-crawler <nevent-or-note-or-event-id>")
		os.Exit(1)
	}

	rootID, err := decodeEventID(target)
	if err != nil {
		slog.Error("target decode failed", "error", err)
		os.Exit(1)
	}

	cfg, err := config.Load()
	if err != nil {
		slog.Error("configuration failed", "error", err)
		os.Exit(1)
	}

	relay := env("NAGG_THREAD_RELAY", "")
	if relay == "" && len(cfg.Firehose.Relays) > 0 {
		relay = cfg.Firehose.Relays[0]
	}
	if relay == "" {
		slog.Error("missing thread relay", "usage", "set NAGG_THREAD_RELAY or NAGG_RELAYS")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := chstore.OpenWithRetry(ctx, cfg.ClickHouse, logger)
	if err != nil {
		slog.Error("clickhouse connection failed", "error", err)
		os.Exit(1)
	}
	defer store.Close()
	if err := store.Migrate(ctx); err != nil {
		slog.Error("clickhouse migration failed", "error", err)
		os.Exit(1)
	}

	c := &crawler{
		relay:  relay,
		events: map[string]*nostr.Event{},
	}
	if err := c.crawl(ctx, rootID); err != nil {
		slog.Error("thread crawl failed", "error", err)
		os.Exit(1)
	}

	records := make([]chstore.EventRecord, 0, len(c.events))
	seen := time.Now().UTC()
	for _, event := range c.events {
		records = append(records, chstore.EventRecord{Event: event, Relay: relay, Seen: seen})
	}
	sort.Slice(records, func(i, j int) bool {
		return records[i].Event.CreatedAt < records[j].Event.CreatedAt
	})

	if err := store.InsertEvents(ctx, records); err != nil {
		slog.Error("clickhouse insert failed", "error", err)
		os.Exit(1)
	}

	stats := c.stats(rootID)
	slog.Info("thread crawl inserted",
		"relay", relay,
		"root", rootID,
		"events", len(records),
		"authors", stats.authors,
		"kind0", stats.kind0,
		"kind1", stats.kind1,
		"kind7", stats.kind7,
		"kind9735", stats.kind9735,
		"referencing_root", stats.referencesRoot,
	)
}

func (c *crawler) crawl(ctx context.Context, rootID string) error {
	rootEvents, err := c.query(ctx, "root", map[string]any{"ids": []string{rootID}, "limit": 10}, 12*time.Second)
	if err != nil {
		return err
	}
	if len(rootEvents) == 0 {
		return fmt.Errorf("root event %s not found on %s", rootID, c.relay)
	}
	c.add(rootEvents)

	if err := c.crawlReferences(ctx, rootID); err != nil {
		return err
	}

	primaryRelay := c.relay
	for _, relay := range splitCSV(env("NAGG_THREAD_EXTRA_RELAYS", "")) {
		c.relay = relay
		if err := c.crawlReferences(ctx, rootID); err != nil {
			slog.Warn("extra relay reference crawl failed", "relay", relay, "error", err)
		}
	}
	c.relay = primaryRelay

	if err := c.crawlEngagement(ctx); err != nil {
		slog.Warn("engagement crawl failed", "relay", c.relay, "error", err)
	}

	authors := c.pubkeys()
	for _, batch := range chunks(authors, 80) {
		events, err := c.query(ctx, "profiles", map[string]any{"kinds": []int{0}, "authors": batch, "limit": len(batch) * 3}, 15*time.Second)
		if err != nil {
			return err
		}
		c.add(events)
		slog.Info("profile batch fetched", "authors", len(batch), "profiles", len(events), "new_total", len(c.events))
	}
	return nil
}

func (c *crawler) crawlEngagement(ctx context.Context) error {
	eventIDs := c.eventIDs()
	for batchIndex, batch := range chunks(eventIDs, 80) {
		events, err := c.query(ctx, fmt.Sprintf("engagement-%d", batchIndex), map[string]any{
			"kinds": []int{6, 7, 16, 9735},
			"#e":    batch,
			"limit": 5000,
		}, 20*time.Second)
		if err != nil {
			return err
		}
		before := len(c.events)
		c.add(events)
		slog.Info("engagement batch fetched", "relay", c.relay, "targets", len(batch), "events", len(events), "new_total", len(c.events), "new_events", len(c.events)-before)
	}
	return nil
}

func (c *crawler) crawlReferences(ctx context.Context, rootID string) error {
	visited := map[string]struct{}{}
	frontier := []string{rootID}
	for _, event := range c.events {
		frontier = append(frontier, event.ID)
	}
	for depth := 0; depth < 12 && len(frontier) > 0; depth++ {
		batch := takeUnvisited(visited, frontier, 80)
		if len(batch) == 0 {
			break
		}
		filter := map[string]any{"#e": batch, "limit": 5000}
		events, err := c.query(ctx, fmt.Sprintf("refs-%d", depth), filter, 20*time.Second)
		if err != nil {
			return err
		}

		before := len(c.events)
		c.add(events)
		slog.Info("thread crawl layer", "relay", c.relay, "depth", depth, "queried_ids", len(batch), "fetched", len(events), "new_total", len(c.events))
		if len(c.events) == before {
			continue
		}

		frontier = frontier[:0]
		for _, event := range events {
			frontier = append(frontier, event.ID)
		}
	}
	return nil
}

func (c *crawler) query(ctx context.Context, subID string, filter map[string]any, timeout time.Duration) ([]*nostr.Event, error) {
	return c.queryEnvelope(ctx, subID, filter, timeout)
}

func (c *crawler) queryEnvelope(ctx context.Context, subID string, filter map[string]any, timeout time.Duration) ([]*nostr.Event, error) {
	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	conn, _, err := websocket.DefaultDialer.DialContext(qctx, c.relay, http.Header{})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	conn.SetReadLimit(8 << 20)

	if err := conn.WriteJSON([]any{"REQ", "nagg-" + subID, filter}); err != nil {
		return nil, err
	}
	defer conn.WriteJSON([]any{"CLOSE", "nagg-" + subID})

	var out []*nostr.Event
	for {
		select {
		case <-qctx.Done():
			if errors.Is(qctx.Err(), context.DeadlineExceeded) {
				return out, nil
			}
			return out, qctx.Err()
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		_, data, err := conn.ReadMessage()
		if err != nil {
			if strings.Contains(err.Error(), "i/o timeout") {
				return out, nil
			}
			if websocket.IsCloseError(err, websocket.CloseNoStatusReceived, websocket.CloseNormalClosure, websocket.CloseGoingAway) {
				return out, nil
			}
			if len(out) > 0 {
				return out, nil
			}
			return out, err
		}
		event, eose, err := parseRelayMessage(data)
		if err != nil {
			slog.Debug("relay message ignored", "error", err)
			continue
		}
		if event != nil && validate(event) == nil {
			out = append(out, event)
		}
		if eose {
			return out, nil
		}
	}
}

func parseRelayMessage(data []byte) (*nostr.Event, bool, error) {
	var frame []json.RawMessage
	if err := json.Unmarshal(data, &frame); err != nil {
		return nil, false, err
	}
	if len(frame) == 0 {
		return nil, false, nil
	}
	var typ string
	if err := json.Unmarshal(frame[0], &typ); err != nil {
		return nil, false, err
	}
	switch typ {
	case "EVENT":
		if len(frame) < 3 {
			return nil, false, nil
		}
		var event nostr.Event
		if err := json.Unmarshal(frame[2], &event); err != nil {
			return nil, false, err
		}
		return &event, false, nil
	case "EOSE":
		return nil, true, nil
	default:
		return nil, false, nil
	}
}

func validate(event *nostr.Event) error {
	if len(event.ID) != 64 || len(event.PubKey) != 64 || len(event.Sig) != 128 {
		return fmt.Errorf("invalid event shape")
	}
	if !event.CheckID() {
		return fmt.Errorf("invalid id")
	}
	ok, err := event.CheckSignature()
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("invalid signature")
	}
	return nil
}

func (c *crawler) add(events []*nostr.Event) {
	for _, event := range events {
		if event != nil {
			c.events[event.ID] = event
		}
	}
}

func (c *crawler) pubkeys() []string {
	seen := map[string]struct{}{}
	for _, event := range c.events {
		if event.PubKey != "" {
			seen[event.PubKey] = struct{}{}
		}
		for _, tag := range event.Tags {
			if len(tag) >= 2 && tag[0] == "p" && nostr.IsValidPublicKey(tag[1]) {
				seen[tag[1]] = struct{}{}
			}
		}
	}
	out := make([]string, 0, len(seen))
	for pubkey := range seen {
		out = append(out, pubkey)
	}
	sort.Strings(out)
	return out
}

func (c *crawler) eventIDs() []string {
	out := make([]string, 0, len(c.events))
	for id := range c.events {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}

type crawlStats struct {
	authors        int
	kind0          int
	kind1          int
	kind7          int
	kind9735       int
	referencesRoot int
}

func (c *crawler) stats(rootID string) crawlStats {
	authors := map[string]struct{}{}
	var stats crawlStats
	for _, event := range c.events {
		authors[event.PubKey] = struct{}{}
		switch event.Kind {
		case 0:
			stats.kind0++
		case 1:
			stats.kind1++
		case 7:
			stats.kind7++
		case 9735:
			stats.kind9735++
		}
		for _, tag := range event.Tags {
			if len(tag) >= 2 && tag[0] == "e" && tag[1] == rootID {
				stats.referencesRoot++
				break
			}
		}
	}
	stats.authors = len(authors)
	return stats
}

func takeUnvisited(visited map[string]struct{}, ids []string, max int) []string {
	out := make([]string, 0, min(len(ids), max))
	for _, id := range ids {
		if _, ok := visited[id]; ok {
			continue
		}
		visited[id] = struct{}{}
		out = append(out, id)
		if len(out) >= max {
			break
		}
	}
	return out
}

func chunks(values []string, size int) [][]string {
	var out [][]string
	for start := 0; start < len(values); start += size {
		end := min(start+size, len(values))
		out = append(out, values[start:end])
	}
	return out
}

func decodeEventID(input string) (string, error) {
	if nostr.IsValid32ByteHex(input) {
		return input, nil
	}
	prefix, value, err := nip19.Decode(input)
	if err != nil {
		return "", err
	}
	switch prefix {
	case "note":
		return value.(string), nil
	case "nevent":
		return value.(nostr.EventPointer).ID, nil
	default:
		return "", fmt.Errorf("unsupported target prefix %q", prefix)
	}
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := map[string]struct{}{}
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}
