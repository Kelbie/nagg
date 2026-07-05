package vertex

import (
	"context"
	"fmt"
	"log/slog"
)

type ScoreProfileStore interface {
	CachedVertexProfile(context.Context, string) (ProfileResult, bool, error)
	SaveVertexProfile(context.Context, ProfileResult) error
}

type ScoreProfileClient interface {
	ProfileRefresh(context.Context, string) (ProfileResult, error)
}

type ScoreProvider struct {
	store        ScoreProfileStore
	client       ScoreProfileClient
	minFollowers uint64
	logger       *slog.Logger
}

type ScoreProviderOption func(*ScoreProvider)

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
