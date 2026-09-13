package wallpapers

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/vertex-lab/nagg/internal/relayquery"
)

type Fetcher interface {
	Query(context.Context, map[string]any, time.Duration) ([]relayquery.Event, error)
}

type Config struct {
	AdminPubkey string
	Interval    time.Duration
}

type Service struct {
	cfg       Config
	fetcher   Fetcher
	logger    *slog.Logger
	now       func() time.Time
	passMu    sync.Mutex
	mu        sync.RWMutex
	body      []byte
	updatedAt time.Time
}

func NewService(cfg Config, fetcher Fetcher, logger *slog.Logger) *Service {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Hour
	}
	if cfg.AdminPubkey == "" {
		cfg.AdminPubkey = DefaultAdminPubkey
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Service{cfg: cfg, fetcher: fetcher, logger: logger, now: time.Now}
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.Interval)
	defer ticker.Stop()
	for {
		if ctx.Err() != nil {
			return
		}
		s.RunOnce(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

func (s *Service) RunOnce(ctx context.Context) {
	s.passMu.Lock()
	defer s.passMu.Unlock()
	if s.fetcher == nil || ctx.Err() != nil {
		return
	}
	var events []relayquery.Event
	for _, filter := range []map[string]any{
		{"authors": []string{s.cfg.AdminPubkey}, "kinds": []int{30078}, "#d": []string{catalogTag}, "limit": 1},
		{"authors": []string{s.cfg.AdminPubkey}, "kinds": []int{1063}, "#t": []string{"wallpaper"}, "limit": 500},
	} {
		result, err := s.fetcher.Query(ctx, filter, 8*time.Second)
		if err != nil {
			s.logger.Warn("wallpapers.refresh.failed", "reason", "relay query failed")
			return
		}
		events = append(events, result...)
	}
	if ctx.Err() != nil {
		return
	}
	now := s.now()
	catalog, ok := buildCatalog(events, s.cfg.AdminPubkey, now.Unix())
	if !ok {
		s.logger.Warn("wallpapers.refresh.failed", "reason", "no usable catalog")
		return
	}
	catalog.LastUpdated = now.UnixMilli()
	body, err := json.Marshal(catalog)
	if err != nil {
		s.logger.Warn("wallpapers.refresh.failed", "reason", "encoding failed")
		return
	}
	s.mu.Lock()
	s.body, s.updatedAt = body, now
	s.mu.Unlock()
	s.logger.Info("wallpapers.refresh", "wallpapers", len(catalog.Wallpapers), "albums", len(catalog.Albums))
}

// Snapshot copies immutable JSON and checks expiry on every read, including
// after the worker stops. A failed refresh never extends this 24-hour window.
func (s *Service) Snapshot() (json.RawMessage, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if len(s.body) == 0 || s.now().Sub(s.updatedAt) >= 24*time.Hour {
		return nil, false
	}
	return append(json.RawMessage(nil), s.body...), true
}
