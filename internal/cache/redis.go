package cache

import (
	"context"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"
)

type redisCache struct {
	client *redis.Client
	logger *slog.Logger
}

// New builds a Redis-backed cache from a redis:// or rediss:// URL. It is
// deliberately non-fatal: an empty or unparseable URL yields a disabled (no-op)
// cache and logs a warning, so a cache misconfiguration can never take down the
// API — the cache is best-effort by design.
func New(rawURL string, logger *slog.Logger) Cache {
	if logger == nil {
		logger = slog.Default()
	}
	if rawURL == "" {
		return Disabled()
	}
	opts, err := redis.ParseURL(rawURL)
	if err != nil {
		logger.Warn("invalid NAGG_REDIS_URL; response cache disabled", "error", err)
		return Disabled()
	}
	return &redisCache{client: redis.NewClient(opts), logger: logger}
}

func (c *redisCache) Get(ctx context.Context, key string) ([]byte, bool) {
	val, err := c.client.Get(ctx, key).Bytes()
	if err == redis.Nil {
		return nil, false
	}
	if err != nil {
		c.logger.Warn("cache get failed", "error", err, "key", key)
		return nil, false
	}
	return val, true
}

func (c *redisCache) Set(ctx context.Context, key string, value []byte, ttl time.Duration) {
	if ttl <= 0 {
		return
	}
	// Detach from the request context so a client disconnect does not abort the
	// write, but keep a short bound so a slow Redis cannot pile up goroutines.
	wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 2*time.Second)
	defer cancel()
	if err := c.client.Set(wctx, key, value, ttl).Err(); err != nil {
		c.logger.Warn("cache set failed", "error", err, "key", key)
	}
}

func (c *redisCache) Enabled() bool { return true }
