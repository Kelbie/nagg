package graphqlapi

import (
	"context"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// Ranker exposes the GraphQL ranked-feed ranking pipeline to non-GraphQL
// callers (e.g. the REST app-view) without duplicating any ranking logic. It
// wraps the same resolver and the same rankedEventViews core used by the
// GraphQL rankedEvents resolver, so both transports produce identical ranking
// for identical input.
type Ranker struct {
	resolver *resolver
}

// NewRanker builds a Ranker over the given store. It accepts the same Options as
// NewSchema (e.g. WithPubkeyScoreMinFollowers, WithUserFeedBackfill) so on-demand
// hydration and scoring behave identically to the GraphQL path.
func NewRanker(store Store, opts ...Option) *Ranker {
	r := &resolver{store: store, pubkeyScoreMinFollowers: defaultPubkeyScoreMinFollowers}
	for _, opt := range opts {
		opt(r)
	}
	return &Ranker{resolver: r}
}

// RankedEventViews parses the raw ranked-events input (the exact same map shape
// the GraphQL `rankedEvents(input: ...)` field accepts) and runs the shared
// ranking core, returning the ordered events. The REST handler enriches these
// into its FeedResponse; the GraphQL resolver wraps them in an event connection.
func (r *Ranker) RankedEventViews(ctx context.Context, raw any) ([]chstore.EventView, error) {
	input, err := r.resolver.parseRankedEventsInput(ctx, raw)
	if err != nil {
		return nil, err
	}
	return r.resolver.rankedEventViews(ctx, input)
}
