package clickhouse

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"
)

//go:embed migrations/001_ingestion.sql
var ingestionMigration string

//go:embed migrations/002_appview.sql
var appviewMigration string

//go:embed migrations/003_ranking.sql
var rankingMigration string

//go:embed migrations/004_derived.sql
var derivedMigration string

//go:embed migrations/005_reply_parity.sql
var replyParityMigration string

//go:embed migrations/006_notifications.sql
var notificationsMigration string

//go:embed migrations/007_direct_replies.sql
var directRepliesMigration string

//go:embed migrations/008_raw_counts.sql
var rawCountsMigration string

//go:embed migrations/009_engagement_real.sql
var engagementRealMigration string

//go:embed migrations/010_user_aggregates.sql
var userAggregatesMigration string

//go:embed migrations/011_rank_features.sql
var rankFeaturesMigration string

//go:embed migrations/012_feature_ttl.sql
var featureTTLMigration string

//go:embed migrations/013_like_repost_backfill.sql
var likeRepostBackfillMigration string

type Config struct {
	Addr         string
	Database     string
	Username     string
	Password     string
	MaxOpenConns int
	MaxIdleConns int
	// MaxQueryMemoryBytes caps per-read ClickHouse memory via the max_memory_usage
	// setting. 0 leaves it unset (server default). See config.APIConfig docs.
	MaxQueryMemoryBytes int64
}

type Store struct {
	conn ch.Conn
}

const clickHouseStartupProbeTimeout = 2 * time.Second

// retryConn wraps the driver connection so transient connection-level failures
// (a pooled connection the server closed out from under us — "connection reset
// by peer", broken pipe, EOF) are retried instead of surfacing as a 5xx. Under
// concurrent/burst load — e.g. a phone opening several profiles at once, each
// firing feed + thread + notification reads — these resets are the dominant
// non-fatal error, and ClickHouse can transiently shed connections when busy
// (TOO_MANY_SIMULTANEOUS_QUERIES). A few retries on a fresh pooled connection,
// spaced by a small backoff, ride out the blip rather than piling straight back
// on. It only retries reads (Query/QueryRow), never on context cancel/deadline
// (the caller gave up or the query is genuinely too slow) or on Exec/inserts
// (not idempotent). All other driver methods promote unchanged.
type retryConn struct {
	chdriver.Conn
	// readSettings, when non-nil, is layered onto every read's context (e.g. a
	// max_memory_usage cap). Reads carry no settings of their own, so this never
	// clobbers a caller's — rollup/insert paths use Exec, which bypasses this.
	readSettings ch.Settings
}

// withReadSettings layers readSettings onto ctx for a read. Inserts (Exec) never
// reach here, so they keep their own per-statement settings (see rollup.go).
func (c retryConn) withReadSettings(ctx context.Context) context.Context {
	if len(c.readSettings) == 0 {
		return ctx
	}
	return ch.Context(ctx, ch.WithSettings(c.readSettings))
}

const (
	// retryReadAttempts is the total number of read attempts (1 initial + retries).
	retryReadAttempts = 3
	// retryReadBackoff is the base delay between read attempts; it scales linearly
	// per attempt (25ms, 50ms), so the worst-case added latency stays well under
	// 100ms while spreading retries out enough to let a busy server recover.
	retryReadBackoff = 25 * time.Millisecond
)

// retryTransientRead runs op up to attempts times, retrying only on transient
// connection errors (isTransientConnErr) with a linear backoff, and bailing
// immediately on a non-transient error, success, or a cancelled/expired context.
// op must re-issue the read; the driver hands out a fresh pooled connection each
// call, which is what recovers a server-closed connection.
func retryTransientRead(ctx context.Context, attempts int, backoff time.Duration, op func() error) error {
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		err = op()
		if !isTransientConnErr(err) {
			return err
		}
		if attempt == attempts-1 || ctx.Err() != nil {
			return err
		}
		timer := time.NewTimer(backoff * time.Duration(attempt+1))
		select {
		case <-ctx.Done():
			timer.Stop()
			return err
		case <-timer.C:
		}
	}
	return err
}

func isTransientConnErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	msg := err.Error()
	for _, marker := range []string{
		"connection reset by peer",
		"broken pipe",
		"use of closed network connection",
		"unexpected EOF",
		"EOF",
	} {
		if strings.Contains(msg, marker) {
			return true
		}
	}
	return false
}

func (c retryConn) Query(ctx context.Context, query string, args ...any) (chdriver.Rows, error) {
	ctx = c.withReadSettings(ctx)
	var rows chdriver.Rows
	err := retryTransientRead(ctx, retryReadAttempts, retryReadBackoff, func() error {
		var e error
		rows, e = c.Conn.Query(ctx, query, args...)
		return e
	})
	return rows, err
}

func (c retryConn) QueryRow(ctx context.Context, query string, args ...any) chdriver.Row {
	ctx = c.withReadSettings(ctx)
	var row chdriver.Row
	_ = retryTransientRead(ctx, retryReadAttempts, retryReadBackoff, func() error {
		row = c.Conn.QueryRow(ctx, query, args...)
		return row.Err()
	})
	return row
}

type EventRecord struct {
	Event *nostr.Event
	Relay string
	Seen  time.Time
}

func Open(ctx context.Context, cfg Config) (*Store, error) {
	maxOpenConns := positiveOrDefault(cfg.MaxOpenConns, 30)
	maxIdleConns := positiveOrDefault(cfg.MaxIdleConns, 10)
	if maxIdleConns > maxOpenConns {
		maxIdleConns = maxOpenConns
	}
	conn, err := ch.Open(&ch.Options{
		Addr: []string{cfg.Addr},
		Auth: ch.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:      5 * time.Second,
		ConnOpenStrategy: ch.ConnOpenInOrder,
		MaxOpenConns:     maxOpenConns,
		MaxIdleConns:     maxIdleConns,
		// Recycle pooled connections well before Railway's private network / the
		// ClickHouse server drops idle ones, so we hand out fewer dead connections
		// (the retryConn wrapper recovers the rest).
		ConnMaxLifetime: 10 * time.Minute,
	})
	if err != nil {
		return nil, err
	}
	pingCtx, cancel := context.WithTimeout(ctx, clickHouseStartupProbeTimeout)
	defer cancel()
	if err := conn.Ping(pingCtx); err != nil {
		return nil, err
	}
	var readSettings ch.Settings
	if cfg.MaxQueryMemoryBytes > 0 {
		readSettings = ch.Settings{"max_memory_usage": cfg.MaxQueryMemoryBytes}
	}
	return &Store{conn: retryConn{Conn: conn, readSettings: readSettings}}, nil
}

type openRetryConfig struct {
	Attempts     int
	InitialDelay time.Duration
	MaxDelay     time.Duration
}

// OpenWithRetry absorbs short ClickHouse availability blips during Railway
// deploys. Heavy background mutations can briefly reset TCP connections; a
// single failed Ping should not fail the whole pre-deploy or app startup.
func OpenWithRetry(ctx context.Context, cfg Config, logger *slog.Logger) (*Store, error) {
	return openWithRetry(ctx, cfg, defaultOpenRetryConfig(), logger, Open)
}

func defaultOpenRetryConfig() openRetryConfig {
	cfg := openRetryConfig{
		// Railway production healthchecks allow 300s. Keep probes short and
		// frequent so transient ClickHouse connection resets get many recovery
		// chances before Railway gives up on the deployment.
		Attempts:     42,
		InitialDelay: time.Second,
		MaxDelay:     5 * time.Second,
	}
	// The pre-deploy migrate command is not bound by the app healthcheck window,
	// so when ClickHouse is saturated (e.g. heavy queries starving new native
	// connections) a deploy can ride out a longer outage by raising the budget
	// via env, without a code change and without affecting app-startup defaults.
	if v := envPositiveInt("NAGG_CLICKHOUSE_CONNECT_ATTEMPTS"); v > 0 {
		cfg.Attempts = v
	}
	if v := envPositiveInt("NAGG_CLICKHOUSE_CONNECT_MAX_DELAY_SECONDS"); v > 0 {
		cfg.MaxDelay = time.Duration(v) * time.Second
	}
	return cfg
}

// envPositiveInt reads a strictly-positive integer from an env var, returning 0
// when unset, empty, malformed or non-positive (so callers keep their default).
func envPositiveInt(name string) int {
	v, err := strconv.Atoi(strings.TrimSpace(os.Getenv(name)))
	if err != nil || v <= 0 {
		return 0
	}
	return v
}

func openWithRetry(
	ctx context.Context,
	cfg Config,
	retry openRetryConfig,
	logger *slog.Logger,
	open func(context.Context, Config) (*Store, error),
) (*Store, error) {
	attempts := positiveOrDefault(retry.Attempts, 1)
	delay := retry.InitialDelay
	if delay <= 0 {
		delay = time.Second
	}
	maxDelay := retry.MaxDelay
	if maxDelay <= 0 {
		maxDelay = delay
	}

	var lastErr error
	for attempt := 1; attempt <= attempts; attempt++ {
		store, err := open(ctx, cfg)
		if err == nil {
			if attempt > 1 && logger != nil {
				logger.Info("clickhouse connection recovered", "attempt", attempt)
			}
			return store, nil
		}
		lastErr = err
		if attempt == attempts {
			break
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if logger != nil {
			logger.Warn(
				"clickhouse connection failed; retrying",
				"attempt", attempt,
				"max_attempts", attempts,
				"next_delay", delay,
				"error", err,
			)
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
		delay *= 2
		if delay > maxDelay {
			delay = maxDelay
		}
	}
	return nil, lastErr
}

func positiveOrDefault(value int, fallback int) int {
	if value > 0 {
		return value
	}
	return fallback
}

func (s *Store) Close() error {
	return s.conn.Close()
}

func (s *Store) EventCount(ctx context.Context) (uint64, error) {
	var count uint64
	if err := s.conn.QueryRow(ctx, "SELECT count() FROM nostr_events").Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}

type EventKindStats struct {
	Count                uint64
	StoredBytesEstimated uint64
}

func (s *Store) EventKindStats(ctx context.Context, kinds []int) (map[int]EventKindStats, error) {
	kinds = uniqueEventKinds(kinds)
	out := make(map[int]EventKindStats, len(kinds))
	for _, kind := range kinds {
		out[kind] = EventKindStats{}
	}
	if len(kinds) == 0 {
		return out, nil
	}

	counts, err := s.EventKindCounts(ctx, kinds)
	if err != nil {
		return nil, err
	}
	totalBytes, totalRows, err := s.nostrEventsStorageFootprint(ctx)
	if err != nil {
		return nil, err
	}

	for kind, count := range counts {
		out[kind] = EventKindStats{
			Count:                count,
			StoredBytesEstimated: estimateStoredBytes(count, totalBytes, totalRows),
		}
	}
	return out, nil
}

func (s *Store) nostrEventsStorageFootprint(ctx context.Context) (uint64, uint64, error) {
	var bytes uint64
	var rows uint64
	if err := s.conn.QueryRow(ctx, `
		SELECT sum(data_compressed_bytes), sum(rows)
		FROM system.parts
		WHERE active AND database = currentDatabase() AND table = 'nostr_events'
	`).Scan(&bytes, &rows); err != nil {
		return 0, 0, err
	}
	return bytes, rows, nil
}

func (s *Store) EventKindCounts(ctx context.Context, kinds []int) (map[int]uint64, error) {
	kinds = uniqueEventKinds(kinds)
	out := make(map[int]uint64, len(kinds))
	for _, kind := range kinds {
		out[kind] = 0
	}
	if len(kinds) == 0 {
		return out, nil
	}

	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		SELECT kind, count()
		FROM nostr_events
		WHERE kind IN (%s)
		GROUP BY kind
	`, ints(kinds)))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var kind uint32
		var count uint64
		if err := rows.Scan(&kind, &count); err != nil {
			return nil, err
		}
		out[int(kind)] = count
	}
	return out, rows.Err()
}

func estimateStoredBytes(count uint64, totalBytes uint64, totalRows uint64) uint64 {
	if count == 0 || totalBytes == 0 || totalRows == 0 {
		return 0
	}
	return uint64(math.Round(float64(count) * float64(totalBytes) / float64(totalRows)))
}

func uniqueEventKinds(values []int) []int {
	seen := make(map[int]struct{}, len(values))
	out := make([]int, 0, len(values))
	for _, value := range values {
		if value < 0 {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	return out
}

func (s *Store) prepareInsertBatch(ctx context.Context, query string) (chdriver.Batch, error) {
	return s.conn.PrepareBatch(ctx, query, chdriver.WithReleaseConnection())
}

func closeUnsentBatch(batch chdriver.Batch) {
	if batch != nil && !batch.IsSent() {
		_ = batch.Close()
	}
}

func (s *Store) Migrate(ctx context.Context) error {
	// Run migrations — and especially the historical backfill INSERT…SELECTs — with
	// a low thread budget. Managed ClickHouse defaults max_threads to auto(N) (e.g.
	// 32); a backfill that tries to spawn that many worker threads fails on a small,
	// thread-constrained server with "Cannot schedule a task: failed to start the
	// thread" (error 439). Migrations run sequentially, so single-threaded is fine
	// and still fast at our data volume.
	migCtx := ch.Context(ctx, ch.WithSettings(ch.Settings{
		"max_threads":        1,
		"max_insert_threads": 1,
		// MODIFY TTL (012) must not materialize over the whole table at deploy time;
		// let background merges apply it so the migrate stays a metadata change.
		"materialize_ttl_after_modify": 0,
	}))
	for _, migration := range embeddedMigrations() {
		for _, stmt := range splitSQLStatements(migration) {
			if err := s.conn.Exec(migCtx, stmt); err != nil {
				return fmt.Errorf("migration failed: %w", err)
			}
		}
	}
	// The CREATEs above ensure declared tables/views exist; the reconciler then
	// strips anything the embedded SQL no longer declares and evolves columns,
	// making the SQL files the single declarative source of truth.
	if err := s.reconcileSchema(ctx, schemaReconcileMode()); err != nil {
		return fmt.Errorf("schema reconcile failed: %w", err)
	}
	return nil
}

func (s *Store) Backfill(ctx context.Context) error {
	statements := []string{
		"TRUNCATE TABLE IF EXISTS note_like_counts",
		"TRUNCATE TABLE IF EXISTS note_repost_counts",
		"TRUNCATE TABLE IF EXISTS note_reply_counts",
		"TRUNCATE TABLE IF EXISTS note_zaps",
		"TRUNCATE TABLE IF EXISTS note_zap_totals",
		"TRUNCATE TABLE IF EXISTS profiles_latest",
		"TRUNCATE TABLE IF EXISTS notification_candidates",
		`INSERT INTO note_like_counts
		 SELECT tag_value AS target_event_id, uniqState(pubkey) AS likes
		 FROM event_tags
		 WHERE kind = 7 AND tag_key = 'e' AND length(tag_value) = 64
		 GROUP BY target_event_id`,
		`INSERT INTO note_repost_counts
		 SELECT tag_value AS target_event_id, uniqState(pubkey) AS reposts
		 FROM event_tags
		 WHERE kind IN (6, 16) AND tag_key = 'e' AND length(tag_value) = 64
		 GROUP BY target_event_id`,
		`INSERT INTO note_reply_counts
		 SELECT tag_value AS target_event_id, uniqState(event_id) AS replies
		 FROM event_tags
		 WHERE kind = 1 AND tag_key = 'e' AND length(tag_value) = 64
		 GROUP BY target_event_id`,
		`INSERT INTO profiles_latest
		 SELECT
		   pubkey,
		   id AS event_id,
		   created_at,
		   JSONExtractString(content, 'name') AS name,
		   JSONExtractString(content, 'display_name') AS display_name,
		   JSONExtractString(content, 'picture') AS picture,
		   JSONExtractString(content, 'about') AS about,
		   JSONExtractString(content, 'nip05') AS nip05,
		   JSONExtractString(content, 'lud16') AS lud16,
		   JSONExtractString(content, 'lud06') AS lud06,
		   JSONExtractString(content, 'banner') AS banner,
		   JSONExtractString(content, 'website') AS website,
		   content AS raw_json
		 FROM nostr_events FINAL
		 WHERE kind = 0`,
		`INSERT INTO notification_candidates
		 SELECT
		   tag_value AS viewer,
		   event_id,
		   pubkey AS actor_pubkey,
		   kind,
		   created_at,
		   multiIf(
		     kind = 3, 'follow',
		     kind = 1, 'mention',
		     kind IN (6, 16), 'repost',
		     kind = 7, 'reaction',
		     kind = 9735, 'zap',
		     'mention'
		   ) AS reason
		 FROM event_tags
		 WHERE tag_key = 'p'
		   AND length(tag_value) = 64
		   AND kind IN (1, 3, 6, 7, 16, 9735)
		   AND pubkey != tag_value`,
	}
	for _, stmt := range statements {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
	}
	if err := s.backfillZaps(ctx); err != nil {
		return fmt.Errorf("backfill zaps failed: %w", err)
	}
	return nil
}

func (s *Store) backfillZaps(ctx context.Context) error {
	rows, err := s.conn.Query(ctx, `
		SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
		FROM nostr_events FINAL
		WHERE kind = 9735
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	events, err := scanEventRows(rows)
	if err != nil {
		return err
	}
	if len(events) == 0 {
		return nil
	}

	batch, err := s.prepareInsertBatch(ctx, "INSERT INTO note_zaps")
	if err != nil {
		return err
	}
	defer closeUnsentBatch(batch)
	appended := false
	for _, event := range events {
		zap, ok := extractNoteZap(eventViewToNostrEvent(event), event.CreatedAt)
		if !ok {
			continue
		}
		appended = true
		if err := batch.Append(
			zap.ReceiptID,
			zap.TargetEventID,
			zap.PubKey,
			zap.CreatedAt,
			zap.Sats,
		); err != nil {
			return err
		}
	}
	if appended {
		return batch.Send()
	}
	return nil
}

func (s *Store) InsertEvents(ctx context.Context, records []EventRecord) error {
	if len(records) == 0 {
		return nil
	}

	eventsBatch, err := s.prepareInsertBatch(ctx, "INSERT INTO nostr_events")
	if err != nil {
		return err
	}
	defer closeUnsentBatch(eventsBatch)
	relaysBatch, err := s.prepareInsertBatch(ctx, "INSERT INTO event_seen_relays")
	if err != nil {
		return err
	}
	defer closeUnsentBatch(relaysBatch)
	tagsBatch, err := s.prepareInsertBatch(ctx, "INSERT INTO event_tags")
	if err != nil {
		return err
	}
	defer closeUnsentBatch(tagsBatch)
	zapsBatch, err := s.prepareInsertBatch(ctx, "INSERT INTO note_zaps")
	if err != nil {
		return err
	}
	defer closeUnsentBatch(zapsBatch)
	zapsAppended := false

	for _, record := range records {
		event := record.Event
		createdAt := time.Unix(int64(event.CreatedAt), 0).UTC()
		tagsJSON, err := json.Marshal(event.Tags)
		if err != nil {
			return err
		}

		if err := eventsBatch.Append(
			event.ID,
			event.PubKey,
			createdAt,
			uint32(event.Kind),
			string(tagsJSON),
			event.Content,
			event.Sig,
			record.Seen,
			record.Seen,
		); err != nil {
			return err
		}

		if err := relaysBatch.Append(event.ID, record.Relay, record.Seen, record.Seen); err != nil {
			return err
		}

		for i, tag := range event.Tags {
			key, value, extra := flattenTag(tag)
			if key == "" {
				continue
			}
			if err := tagsBatch.Append(
				event.ID,
				event.PubKey,
				uint32(event.Kind),
				createdAt,
				uint16(i),
				key,
				value,
				extra,
			); err != nil {
				return err
			}
		}

		if zap, ok := extractNoteZap(event, createdAt); ok {
			zapsAppended = true
			if err := zapsBatch.Append(
				zap.ReceiptID,
				zap.TargetEventID,
				zap.PubKey,
				zap.CreatedAt,
				zap.Sats,
			); err != nil {
				return err
			}
		}
	}

	if err := eventsBatch.Send(); err != nil {
		return err
	}
	if err := relaysBatch.Send(); err != nil {
		return err
	}
	if err := tagsBatch.Send(); err != nil {
		return err
	}
	if zapsAppended {
		if err := zapsBatch.Send(); err != nil {
			return err
		}
	}
	return nil
}

func flattenTag(tag nostr.Tag) (string, string, []string) {
	if len(tag) == 0 {
		return "", "", nil
	}
	key := tag[0]
	value := ""
	if len(tag) > 1 {
		value = tag[1]
	}
	extra := []string{}
	if len(tag) > 2 {
		extra = append(extra, tag[2:]...)
	}
	return key, value, extra
}

type noteZap struct {
	ReceiptID     string
	TargetEventID string
	PubKey        string
	CreatedAt     time.Time
	Sats          uint64
}

func eventViewToNostrEvent(event EventView) *nostr.Event {
	tags := make(nostr.Tags, 0, len(event.Tags))
	for _, tag := range event.Tags {
		tags = append(tags, nostr.Tag(tag))
	}
	return &nostr.Event{
		ID:        event.ID,
		PubKey:    event.PubKey,
		CreatedAt: nostr.Timestamp(event.CreatedAt.Unix()),
		Kind:      event.Kind,
		Tags:      tags,
		Content:   event.Content,
		Sig:       event.Sig,
	}
}

func extractNoteZap(event *nostr.Event, createdAt time.Time) (noteZap, bool) {
	if event.Kind != 9735 {
		return noteZap{}, false
	}
	targetID := firstHexNostrTag(event.Tags, "e")
	description := firstNostrTag(event.Tags, "description")
	if targetID == "" && description != "" {
		targetID = targetIDFromZapRequest(description)
	}
	if targetID == "" {
		return noteZap{}, false
	}

	msats := amountMSatsFromZapRequest(description)
	var sats uint64
	if msats > 0 {
		sats = msats / 1000
	} else if bolt11 := firstNostrTag(event.Tags, "bolt11"); bolt11 != "" {
		sats = satsFromBolt11(bolt11)
	}

	return noteZap{
		ReceiptID:     event.ID,
		TargetEventID: targetID,
		PubKey:        event.PubKey,
		CreatedAt:     createdAt,
		Sats:          sats,
	}, true
}

func targetIDFromZapRequest(raw string) string {
	var req struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return ""
	}
	return firstHexTag(req.Tags, "e")
}

func amountMSatsFromZapRequest(raw string) uint64 {
	var req struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return 0
	}
	value := firstTag(req.Tags, "amount")
	if value == "" {
		return 0
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func satsFromBolt11(invoice string) uint64 {
	invoice = strings.ToLower(strings.TrimSpace(invoice))
	if !strings.HasPrefix(invoice, "lnbc") {
		return 0
	}
	sep := strings.LastIndexByte(invoice, '1')
	if sep <= len("lnbc") {
		return 0
	}
	amount := invoice[len("lnbc"):sep]
	if amount == "" {
		return 0
	}

	unit := byte(0)
	last := amount[len(amount)-1]
	if last < '0' || last > '9' {
		unit = last
		amount = amount[:len(amount)-1]
	}
	n, err := strconv.ParseUint(amount, 10, 64)
	if err != nil {
		return 0
	}

	switch unit {
	case 0:
		return n * 100_000_000
	case 'm':
		return n * 100_000
	case 'u':
		return n * 100
	case 'n':
		return n / 10
	case 'p':
		return n / 10_000
	default:
		return 0
	}
}

func firstHexTag(tags [][]string, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func firstTag(tags [][]string, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key {
			return tag[1]
		}
	}
	return ""
}

func firstHexNostrTag(tags nostr.Tags, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func firstNostrTag(tags nostr.Tags, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key {
			return tag[1]
		}
	}
	return ""
}
