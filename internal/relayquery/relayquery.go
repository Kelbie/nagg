package relayquery

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/nbd-wtf/go-nostr"
)

type Event struct {
	Relay string
	Event *nostr.Event
}

type Client struct {
	Relays          []string
	ReadLimit       int64
	DialTimeout     time.Duration
	ReadIdleTimeout time.Duration
}

func (c Client) Query(ctx context.Context, filter map[string]any, timeout time.Duration) ([]Event, error) {
	relays := uniqueRelays(c.Relays)
	if len(relays) == 0 {
		return nil, nil
	}
	if timeout <= 0 {
		timeout = 5 * time.Second
	}

	qctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	subID := fmt.Sprintf("nagg-demand-%d", time.Now().UnixNano())
	var wg sync.WaitGroup
	var mu sync.Mutex
	var out []Event
	var errs []error

	for _, relay := range relays {
		relay := relay
		wg.Add(1)
		go func() {
			defer wg.Done()
			events, err := c.queryRelay(qctx, relay, subID, filter)
			mu.Lock()
			defer mu.Unlock()
			out = append(out, events...)
			if err != nil {
				errs = append(errs, fmt.Errorf("%s: %w", relay, err))
			}
		}()
	}
	wg.Wait()

	if len(out) > 0 {
		return out, nil
	}
	if len(errs) > 0 {
		return nil, errors.Join(errs...)
	}
	return nil, nil
}

func (c Client) queryRelay(ctx context.Context, relay string, subID string, filter map[string]any) ([]Event, error) {
	dialTimeout := c.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = 5 * time.Second
	}
	idleTimeout := c.ReadIdleTimeout
	if idleTimeout <= 0 {
		idleTimeout = 2 * time.Second
	}

	dialer := websocket.Dialer{
		HandshakeTimeout: dialTimeout,
		Proxy:            http.ProxyFromEnvironment,
	}
	conn, _, err := dialer.DialContext(ctx, relay, http.Header{})
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if c.ReadLimit > 0 {
		conn.SetReadLimit(c.ReadLimit)
	} else {
		conn.SetReadLimit(8 << 20)
	}

	if err := conn.WriteJSON([]any{"REQ", subID, filter}); err != nil {
		return nil, fmt.Errorf("send REQ: %w", err)
	}
	defer conn.WriteJSON([]any{"CLOSE", subID})

	var out []Event
	for {
		select {
		case <-ctx.Done():
			if errors.Is(ctx.Err(), context.DeadlineExceeded) {
				return out, nil
			}
			return out, ctx.Err()
		default:
		}

		_ = conn.SetReadDeadline(time.Now().Add(idleTimeout))
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
			continue
		}
		if event != nil && validateEvent(event) == nil {
			out = append(out, Event{Relay: relay, Event: event})
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

func validateEvent(event *nostr.Event) error {
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

func uniqueRelays(relays []string) []string {
	out := make([]string, 0, len(relays))
	seen := map[string]struct{}{}
	for _, relay := range relays {
		relay = strings.TrimSpace(relay)
		if relay == "" {
			continue
		}
		if _, ok := seen[relay]; ok {
			continue
		}
		seen[relay] = struct{}{}
		out = append(out, relay)
	}
	return out
}
