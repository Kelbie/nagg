package vertex

import (
	"context"
	"log/slog"
	"time"
)

type SyncStore interface {
	RecentAuthorPubkeysByFollowers(ctx context.Context, minFollowers uint64, staleAfter time.Duration, limit int) ([]string, error)
	SaveVertexProfile(context.Context, ProfileResult) error
}

type SyncClient interface {
	ProfileRefresh(context.Context, string) (ProfileResult, error)
}

type SyncConfig struct {
	MinFollowers uint64
	BatchSize    int
	// StaleAfter is the score cache TTL: pubkeys whose stored values are
	// older than this get refetched (the plugin policy's CacheTTL).
	StaleAfter time.Duration
	Interval   time.Duration
	// Throttle is the minimum delay between upstream Vertex profile fetches. It
	// protects the credit-limited upstream API from a burst when a large backlog
	// of stale/unscored authors accumulates. Zero disables throttling.
	Throttle time.Duration
}

type Syncer struct {
	store  SyncStore
	client SyncClient
	config SyncConfig
	logger *slog.Logger
}

func NewSyncer(store SyncStore, client SyncClient, cfg SyncConfig, logger *slog.Logger) *Syncer {
	if cfg.BatchSize <= 0 {
		cfg.BatchSize = 200
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 30 * time.Minute
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Syncer{
		store:  store,
		client: client,
		config: cfg,
		logger: logger,
	}
}

func (s *Syncer) Run(ctx context.Context) {
	if s == nil || s.store == nil || s.client == nil {
		return
	}
	for {
		refreshed, failed, err := s.RunOnce(ctx)
		if err != nil {
			s.logger.Error("vertex score sync failed", "error", err)
		} else {
			s.logger.Info("vertex score sync completed", "refreshed", refreshed, "failed", failed)
		}
		if err := sleep(ctx, s.config.Interval); err != nil {
			return
		}
	}
}

func (s *Syncer) RunOnce(ctx context.Context) (int, int, error) {
	pubkeys, err := s.store.RecentAuthorPubkeysByFollowers(ctx, s.config.MinFollowers, s.config.StaleAfter, s.config.BatchSize)
	if err != nil {
		return 0, 0, err
	}
	var refreshed int
	var failed int
	var scoreAvailable int
	for i, pubkey := range pubkeys {
		select {
		case <-ctx.Done():
			return refreshed, failed, ctx.Err()
		default:
		}
		// Throttle upstream fetches (after the first) to stay within Vertex credit
		// limits when the candidate backlog is large.
		if i > 0 && s.config.Throttle > 0 {
			if err := sleep(ctx, s.config.Throttle); err != nil {
				return refreshed, failed, err
			}
		}
		profile, err := s.client.ProfileRefresh(ctx, pubkey)
		if err != nil {
			failed++
			s.logger.Warn("vertex profile sync refresh failed", "pubkey", pubkey, "error", err)
			continue
		}
		if profile.Score != nil {
			scoreAvailable++
		}
		if err := s.store.SaveVertexProfile(ctx, profile); err != nil {
			failed++
			s.logger.Warn("vertex profile sync save failed", "pubkey", pubkey, "error", err)
			continue
		}
		refreshed++
	}
	s.logger.Info(
		"vertex score sync batch metrics",
		"candidates", len(pubkeys),
		"refreshed", refreshed,
		"failed", failed,
		"score_available", scoreAvailable,
		"score_unavailable", refreshed-scoreAvailable,
		"score_available_ratio", ratio(scoreAvailable, refreshed),
	)
	return refreshed, failed, nil
}

func ratio(numerator int, denominator int) float64 {
	if denominator <= 0 {
		return 0
	}
	return float64(numerator) / float64(denominator)
}

func sleep(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
