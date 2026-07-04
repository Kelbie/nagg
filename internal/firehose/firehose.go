package firehose

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/vertex-lab/nagg/internal/safego"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/nbd-wtf/go-nostr"
)

type Config struct {
	Relays        []string
	Kinds         []int
	Since         *time.Duration
	RelayRetry    time.Duration
	SeenCacheSize int
	ReadLimit     int64
	SubID         string
}

type RelayEvent struct {
	Relay string
	Event *nostr.Event
}

type Client struct {
	cfg  Config
	seen *lru.Cache[string, struct{}]
}

func New(cfg Config) (*Client, error) {
	seen, err := lru.New[string, struct{}](cfg.SeenCacheSize)
	if err != nil {
		return nil, fmt.Errorf("create seen cache: %w", err)
	}
	return &Client{cfg: cfg, seen: seen}, nil
}

func (c *Client) Run(ctx context.Context, out chan<- RelayEvent) {
	var wg sync.WaitGroup
	for _, relay := range c.cfg.Relays {
		relay := relay
		wg.Add(1)
		go func() {
			defer safego.Recover("firehose.relay")
			defer wg.Done()
			c.runRelay(ctx, relay, out)
		}()
	}
	<-ctx.Done()
	wg.Wait()
}

func (c *Client) runRelay(ctx context.Context, relay string, out chan<- RelayEvent) {
	retries := 0
	for ctx.Err() == nil {
		err := c.consumeRelay(ctx, relay, out)
		if ctx.Err() != nil {
			return
		}

		delay := backoff(c.cfg.RelayRetry, retries)
		retries++
		slog.Warn("relay disconnected; retrying", "relay", relay, "delay", delay, "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

func (c *Client) consumeRelay(ctx context.Context, relay string, out chan<- RelayEvent) error {
	u, err := url.Parse(relay)
	if err != nil {
		return err
	}
	if u.Scheme != "wss" && u.Scheme != "ws" {
		return fmt.Errorf("relay URL must use ws or wss: %s", relay)
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: 15 * time.Second,
		Proxy:            http.ProxyFromEnvironment,
	}
	conn, _, err := dialer.DialContext(ctx, relay, nil)
	if err != nil {
		return err
	}
	defer conn.Close()
	if c.cfg.ReadLimit > 0 {
		conn.SetReadLimit(c.cfg.ReadLimit)
	}

	req := []any{"REQ", c.cfg.SubID, c.filter()}
	if err := conn.WriteJSON(req); err != nil {
		return fmt.Errorf("send REQ: %w", err)
	}
	slog.Info("relay subscription opened", "relay", relay, "kinds", c.cfg.Kinds)

	for {
		select {
		case <-ctx.Done():
			_ = conn.WriteJSON([]any{"CLOSE", c.cfg.SubID})
			return ctx.Err()
		default:
		}

		_, data, err := conn.ReadMessage()
		if err != nil {
			return err
		}

		event, ok, err := parseEventMessage(data)
		if err != nil {
			slog.Debug("relay message ignored", "relay", relay, "error", err)
			continue
		}
		if !ok {
			continue
		}
		if c.seen.Contains(event.ID) {
			continue
		}
		c.seen.Add(event.ID, struct{}{})

		select {
		case <-ctx.Done():
			return ctx.Err()
		case out <- RelayEvent{Relay: relay, Event: event}:
		}
	}
}

func (c *Client) filter() map[string]any {
	filter := make(map[string]any, 2)
	if len(c.cfg.Kinds) > 0 {
		filter["kinds"] = c.cfg.Kinds
	}
	if c.cfg.Since != nil {
		filter["since"] = time.Now().Add(-*c.cfg.Since).Unix()
	}
	return filter
}

func parseEventMessage(data []byte) (*nostr.Event, bool, error) {
	var frame []json.RawMessage
	if err := json.Unmarshal(data, &frame); err != nil {
		return nil, false, err
	}
	if len(frame) < 3 {
		return nil, false, nil
	}

	var typ string
	if err := json.Unmarshal(frame[0], &typ); err != nil {
		return nil, false, err
	}
	if typ != "EVENT" {
		return nil, false, nil
	}

	var event nostr.Event
	if err := json.Unmarshal(frame[2], &event); err != nil {
		return nil, false, err
	}
	return &event, true, nil
}

func backoff(base time.Duration, attempts int) time.Duration {
	if base <= 0 {
		base = 30 * time.Second
	}
	if attempts <= 0 {
		return time.Second
	}
	delay := base * time.Duration(attempts*attempts)
	if delay > 5*time.Minute {
		delay = 5 * time.Minute
	}
	jitter := time.Duration(rand.Int64N(int64(delay / 4)))
	return delay + jitter
}
