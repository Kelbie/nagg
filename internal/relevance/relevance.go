// Package relevance answers one question for the ingest post cap: "is this
// author relevant to a Sovran user?" — with no external reputation lookups.
//
// The exemption set is (known Sovran viewers) ∪ (everyone they follow). Viewers
// become known on their FIRST app request (the appview touches them on
// notifications / DM / thread reads), so a brand-new profile is never treated
// like a firehose bot. Follows come from latest_k3, which the
// app-view already maintains.
package relevance

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/nbd-wtf/go-nostr"
	"github.com/vertex-lab/nagg/internal/safego"
)

type Store interface {
	TouchKnownViewer(ctx context.Context, pubkey string) error
	ExemptPubkeys(ctx context.Context) ([]string, error)
}

const (
	defaultRefreshInterval = 15 * time.Minute
	touchThrottle          = time.Hour
	touchTimeout           = 5 * time.Second
	// touchThrottleMaxEntries bounds the per-viewer throttle map. Overflow just
	// clears it — the worst case is a redundant insert per viewer, which the
	// ReplacingMergeTree collapses anyway.
	touchThrottleMaxEntries = 100_000
)

// Tracker records known viewers and serves the exemption set.
type Tracker struct {
	store           Store
	logger          *slog.Logger
	refreshInterval time.Duration

	mu sync.RWMutex
	// set is nil until the first successful refresh. Nil FAILS OPEN: Exempt
	// returns true for everyone, so an unreachable ClickHouse can never make
	// the ingest cap drop events it shouldn't.
	set map[string]struct{}

	touchMu   sync.Mutex
	lastTouch map[string]time.Time
}

func NewTracker(store Store, logger *slog.Logger) *Tracker {
	if logger == nil {
		logger = slog.Default()
	}
	return &Tracker{
		store:           store,
		logger:          logger,
		refreshInterval: defaultRefreshInterval,
		lastTouch:       map[string]time.Time{},
	}
}

// Exempt reports whether an author is exempt from the ingest post cap.
func (t *Tracker) Exempt(pubkey string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.set == nil {
		return true
	}
	_, ok := t.set[pubkey]
	return ok
}

// Touch records a viewer sighting, best-effort and asynchronously: it must
// never block or fail the request that carried the viewer. Throttled to one
// insert per viewer per hour.
func (t *Tracker) Touch(pubkey string) {
	pubkey = strings.ToLower(strings.TrimSpace(pubkey))
	if !nostr.IsValid32ByteHex(pubkey) {
		return
	}

	t.touchMu.Lock()
	if len(t.lastTouch) >= touchThrottleMaxEntries {
		t.lastTouch = map[string]time.Time{}
	}
	now := time.Now()
	if last, ok := t.lastTouch[pubkey]; ok && now.Sub(last) < touchThrottle {
		t.touchMu.Unlock()
		return
	}
	t.lastTouch[pubkey] = now
	t.touchMu.Unlock()

	safego.Go("relevance.touch", func() {
		ctx, cancel := context.WithTimeout(context.Background(), touchTimeout)
		defer cancel()
		if err := t.store.TouchKnownViewer(ctx, pubkey); err != nil {
			t.logger.Warn("known viewer touch failed", "error", err)
		}
	})
}

// Run refreshes the exemption set until ctx is done: once immediately, then
// every refreshInterval. Refresh failures keep the previous snapshot (or stay
// fail-open before the first success).
func (t *Tracker) Run(ctx context.Context) {
	for {
		t.refresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(t.refreshInterval):
		}
	}
}

func (t *Tracker) refresh(ctx context.Context) {
	pubkeys, err := t.store.ExemptPubkeys(ctx)
	if err != nil {
		if ctx.Err() == nil {
			t.logger.Warn("relevance exempt set refresh failed", "error", err)
		}
		return
	}
	set := make(map[string]struct{}, len(pubkeys))
	for _, pubkey := range pubkeys {
		set[pubkey] = struct{}{}
	}
	t.mu.Lock()
	first := t.set == nil
	t.set = set
	t.mu.Unlock()
	if first {
		t.logger.Info("relevance exempt set loaded", "pubkeys", len(set))
	}
}
