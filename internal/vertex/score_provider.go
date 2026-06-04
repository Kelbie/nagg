package vertex

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
)

var ErrFollowerCounterUnavailable = errors.New("vertex follower counter unavailable")

type ScoreProfileStore interface {
	CachedVertexProfile(context.Context, string) (ProfileResult, bool, error)
	SaveVertexProfile(context.Context, ProfileResult) error
}

type ScoreProfileClient interface {
	ProfileRefresh(context.Context, string) (ProfileResult, error)
}

type FollowerCounter interface {
	FollowerCount(context.Context, string) (uint64, error)
}

type ScoreProvider struct {
	store        ScoreProfileStore
	client       ScoreProfileClient
	counter      FollowerCounter
	minFollowers uint64
	logger       *slog.Logger
}

type ScoreProviderOption func(*ScoreProvider)

func WithFollowerCounter(counter FollowerCounter) ScoreProviderOption {
	return func(p *ScoreProvider) {
		p.counter = counter
	}
}

func WithScoreProviderLogger(logger *slog.Logger) ScoreProviderOption {
	return func(p *ScoreProvider) {
		if logger != nil {
			p.logger = logger
		}
	}
}

func NewScoreProvider(
	store ScoreProfileStore,
	client ScoreProfileClient,
	minFollowers uint64,
	opts ...ScoreProviderOption,
) *ScoreProvider {
	p := &ScoreProvider{
		store:        store,
		client:       client,
		minFollowers: minFollowers,
		logger:       slog.Default(),
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func (p *ScoreProvider) AuthorScore(ctx context.Context, pubkey string) (float64, bool, error) {
	if p.counter == nil {
		return 0, false, ErrFollowerCounterUnavailable
	}
	followers, err := p.counter.FollowerCount(ctx, pubkey)
	if err != nil {
		return 0, false, err
	}
	return p.AuthorScoreWithFollowers(ctx, pubkey, followers)
}

func (p *ScoreProvider) AuthorScoreWithFollowers(ctx context.Context, pubkey string, followers uint64) (float64, bool, error) {
	profile, _, err := p.AuthorProfileWithFollowers(ctx, pubkey, followers)
	if err != nil {
		return 0, false, err
	}
	if profile.Score == nil {
		return 0, false, nil
	}
	return *profile.Score, true, nil
}

func (p *ScoreProvider) AuthorProfileWithFollowers(
	ctx context.Context,
	pubkey string,
	followers uint64,
) (ProfileResult, bool, error) {
	normalized, ok := NormalizePubkey(pubkey)
	if !ok {
		return ProfileResult{}, false, fmt.Errorf("invalid pubkey")
	}
	if followers < p.minFollowers {
		return ProfileResult{}, false, nil
	}
	if p.client != nil {
		profile, err := p.client.ProfileRefresh(ctx, normalized)
		if err == nil {
			if p.store != nil {
				if saveErr := p.store.SaveVertexProfile(ctx, profile); saveErr != nil {
					p.logger.Warn("vertex profile cache save failed", "pubkey", normalized, "error", saveErr)
				}
			}
			return profile, false, nil
		}
		p.logger.Warn("vertex profile refresh failed", "pubkey", normalized, "error", err)
	}
	if p.store == nil {
		return ProfileResult{}, false, nil
	}
	profile, fromCache, err := p.store.CachedVertexProfile(ctx, normalized)
	if err != nil {
		return ProfileResult{}, false, err
	}
	if !fromCache {
		return ProfileResult{}, false, nil
	}
	return profile, true, nil
}
