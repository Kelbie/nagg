package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/firehose"
	"github.com/vertex-lab/nagg/internal/ingest"
)

type Config struct {
	ClickHouse chstore.Config
	Firehose   firehose.Config
	Ingest     ingest.Config
}

func Load() (Config, error) {
	cfg := Config{
		ClickHouse: chstore.Config{
			Addr:     env("NAGG_CLICKHOUSE_ADDR", "127.0.0.1:9000"),
			Database: env("NAGG_CLICKHOUSE_DATABASE", "default"),
			Username: env("NAGG_CLICKHOUSE_USERNAME", "default"),
			Password: os.Getenv("NAGG_CLICKHOUSE_PASSWORD"),
		},
		Firehose: firehose.Config{
			Relays:        splitCSV(env("NAGG_RELAYS", "wss://relay.damus.io,wss://relay.primal.net,wss://nos.lol,wss://relay.nostr.band")),
			Kinds:         parseKinds(os.Getenv("NAGG_KINDS")),
			Since:         parseDurationPtr(env("NAGG_SINCE", "24h")),
			RelayRetry:    parseDuration(env("NAGG_RELAY_RETRY", "30s")),
			SeenCacheSize: parseInt(env("NAGG_SEEN_CACHE_SIZE", "200000")),
			ReadLimit:     parseInt64(env("NAGG_RELAY_READ_LIMIT_BYTES", "2097152")),
			SubID:         env("NAGG_SUB_ID", "nagg-firehose"),
		},
		Ingest: ingest.Config{
			BatchSize:     parseInt(env("NAGG_BATCH_SIZE", "1000")),
			FlushInterval: parseDuration(env("NAGG_FLUSH_INTERVAL", "5s")),
			QueueSize:     parseInt(env("NAGG_QUEUE_SIZE", "10000")),
			VerifyEvents:  parseBool(env("NAGG_VERIFY_EVENTS", "true")),
		},
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if _, _, err := net.SplitHostPort(c.ClickHouse.Addr); err != nil {
		return fmt.Errorf("NAGG_CLICKHOUSE_ADDR: %w", err)
	}
	if len(c.Firehose.Relays) == 0 {
		return errors.New("NAGG_RELAYS must contain at least one relay URL")
	}
	if c.Ingest.BatchSize < 1 {
		return errors.New("NAGG_BATCH_SIZE must be positive")
	}
	if c.Ingest.FlushInterval <= 0 {
		return errors.New("NAGG_FLUSH_INTERVAL must be positive")
	}
	return nil
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}

func splitCSV(value string) []string {
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	seen := make(map[string]struct{}, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func parseKinds(value string) []int {
	var kinds []int
	for _, part := range splitCSV(value) {
		if start, end, ok := strings.Cut(part, "-"); ok {
			a, errA := strconv.Atoi(strings.TrimSpace(start))
			b, errB := strconv.Atoi(strings.TrimSpace(end))
			if errA == nil && errB == nil && a <= b {
				for k := a; k <= b; k++ {
					kinds = append(kinds, k)
				}
			}
			continue
		}
		if k, err := strconv.Atoi(part); err == nil {
			kinds = append(kinds, k)
		}
	}
	return kinds
}

func parseDuration(value string) time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return d
}

func parseDurationPtr(value string) *time.Duration {
	d, err := time.ParseDuration(value)
	if err != nil || d <= 0 {
		return nil
	}
	return &d
}

func parseInt(value string) int {
	n, err := strconv.Atoi(value)
	if err != nil {
		return 0
	}
	return n
}

func parseInt64(value string) int64 {
	n, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func parseBool(value string) bool {
	v, err := strconv.ParseBool(value)
	return err == nil && v
}
