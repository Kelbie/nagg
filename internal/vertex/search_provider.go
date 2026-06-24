package vertex

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"golang.org/x/sync/singleflight"
)

type SearchCacheStore interface {
	CachedVertexSearch(context.Context, SearchArgs) ([]SearchResult, time.Time, bool, error)
	SaveVertexSearch(context.Context, SearchArgs, []SearchResult) error
}

type SearchRefreshClient interface {
	SearchRefresh(context.Context, SearchArgs) ([]SearchResult, error)
}

type SearchProviderConfig struct {
	MaxAge time.Duration
}

type SearchProvider struct {
	store  SearchCacheStore
	client SearchRefreshClient
	maxAge time.Duration
	logger *slog.Logger
	group  singleflight.Group
}

func NewSearchProvider(store SearchCacheStore, client SearchRefreshClient, cfg SearchProviderConfig, logger *slog.Logger) *SearchProvider {
	if cfg.MaxAge <= 0 {
		cfg.MaxAge = 7 * 24 * time.Hour
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &SearchProvider{
		store:  store,
		client: client,
		maxAge: cfg.MaxAge,
		logger: logger,
	}
}

func (p *SearchProvider) Search(ctx context.Context, args SearchArgs) ([]SearchResult, bool, error) {
	args = NormalizeSearchArgs(args)
	if len(args.Query) < 3 {
		return nil, false, fmt.Errorf("query must be at least 3 characters")
	}
	if p.store != nil {
		rows, fetchedAt, ok, err := p.store.CachedVertexSearch(ctx, args)
		if err != nil {
			return nil, false, err
		}
		if ok {
			if time.Since(fetchedAt) >= p.maxAge {
				p.refreshAsync(args)
			}
			return rows, true, nil
		}
	}
	rows, err := p.refresh(ctx, args)
	return rows, false, err
}

func (p *SearchProvider) refreshAsync(args SearchArgs) {
	if p.client == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if _, err := p.refresh(ctx, args); err != nil {
			p.logger.Warn("vertex search refresh failed", "query_len", len(args.Query), "sort", args.Sort, "source", args.Source, "error", err)
		}
	}()
}

func (p *SearchProvider) refresh(ctx context.Context, args SearchArgs) ([]SearchResult, error) {
	if p.client == nil {
		return nil, ErrUnavailable
	}
	value, err, _ := p.group.Do(SearchCacheKey(args), func() (any, error) {
		rows, err := p.client.SearchRefresh(ctx, args)
		if err != nil {
			return nil, err
		}
		if p.store != nil {
			if err := p.store.SaveVertexSearch(ctx, args, rows); err != nil {
				p.logger.Warn("vertex search cache save failed", "query_len", len(args.Query), "sort", args.Sort, "source", args.Source, "error", err)
			}
		}
		return rows, nil
	})
	if err != nil {
		return nil, err
	}
	rows, _ := value.([]SearchResult)
	return rows, nil
}
