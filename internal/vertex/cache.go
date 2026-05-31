package vertex

import (
	"context"
	"sync"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
)

type cachedCall[A any, R any] struct {
	fn       func(context.Context, A) (R, error)
	key      func(A) string
	validate func(R) bool
	ttl      time.Duration
	swr      time.Duration

	mu        sync.Mutex
	store     *lru.Cache[string, cacheEntry[R]]
	inflight  map[string]*cacheFlight[R]
	hits      uint64
	misses    uint64
	refreshes uint64
}

type cacheEntry[R any] struct {
	value     R
	fetchedAt time.Time
}

type cacheFlight[R any] struct {
	done  chan struct{}
	value R
	err   error
}

type CacheStats struct {
	Size      int    `json:"size"`
	Hits      uint64 `json:"hits"`
	Misses    uint64 `json:"misses"`
	Refreshes uint64 `json:"refreshes"`
}

func newCachedCall[A any, R any](
	fn func(context.Context, A) (R, error),
	key func(A) string,
	validate func(R) bool,
	ttl time.Duration,
	swr time.Duration,
	maxEntries int,
) (*cachedCall[A, R], error) {
	if maxEntries <= 0 {
		maxEntries = 10_000
	}
	store, err := lru.New[string, cacheEntry[R]](maxEntries)
	if err != nil {
		return nil, err
	}
	return &cachedCall[A, R]{
		fn:       fn,
		key:      key,
		validate: validate,
		ttl:      ttl,
		swr:      swr,
		store:    store,
		inflight: map[string]*cacheFlight[R]{},
	}, nil
}

func (c *cachedCall[A, R]) Get(ctx context.Context, args A) (R, bool, error) {
	key := c.key(args)
	now := time.Now()

	c.mu.Lock()
	if entry, ok := c.store.Get(key); ok {
		age := now.Sub(entry.fetchedAt)
		if age < c.ttl {
			c.hits++
			value := entry.value
			c.mu.Unlock()
			return value, true, nil
		}
		if age < c.swr {
			c.hits++
			value := entry.value
			if _, ok := c.inflight[key]; !ok {
				c.refreshes++
				flight := &cacheFlight[R]{done: make(chan struct{})}
				c.inflight[key] = flight
				go c.runFetch(context.Background(), key, args, flight)
			}
			c.mu.Unlock()
			return value, true, nil
		}
	}
	c.misses++
	c.mu.Unlock()

	value, err := c.fetchOnce(ctx, key, args)
	return value, false, err
}

func (c *cachedCall[A, R]) Peek(args A) (R, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	entry, ok := c.store.Peek(c.key(args))
	return entry.value, ok
}

func (c *cachedCall[A, R]) Values() []R {
	c.mu.Lock()
	defer c.mu.Unlock()
	entries := c.store.Values()
	out := make([]R, 0, len(entries))
	for _, entry := range entries {
		out = append(out, entry.value)
	}
	return out
}

func (c *cachedCall[A, R]) Stats() CacheStats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return CacheStats{
		Size:      c.store.Len(),
		Hits:      c.hits,
		Misses:    c.misses,
		Refreshes: c.refreshes,
	}
}

func (c *cachedCall[A, R]) fetchOnce(ctx context.Context, key string, args A) (R, error) {
	c.mu.Lock()
	if flight, ok := c.inflight[key]; ok {
		c.mu.Unlock()
		select {
		case <-flight.done:
			return flight.value, flight.err
		case <-ctx.Done():
			var zero R
			return zero, ctx.Err()
		}
	}
	flight := &cacheFlight[R]{done: make(chan struct{})}
	c.inflight[key] = flight
	c.mu.Unlock()

	return c.runFetch(ctx, key, args, flight)
}

func (c *cachedCall[A, R]) runFetch(ctx context.Context, key string, args A, flight *cacheFlight[R]) (R, error) {
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
	}
	value, err := c.fn(ctx, args)
	if err == nil && (c.validate == nil || c.validate(value)) {
		c.mu.Lock()
		c.store.Add(key, cacheEntry[R]{value: value, fetchedAt: time.Now()})
		c.mu.Unlock()
	}

	c.mu.Lock()
	flight.value = value
	flight.err = err
	delete(c.inflight, key)
	close(flight.done)
	c.mu.Unlock()
	return value, err
}
