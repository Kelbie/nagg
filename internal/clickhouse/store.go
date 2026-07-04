package clickhouse

import (
	"context"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/nbd-wtf/go-nostr"

	"github.com/vertex-lab/nagg/internal/rules"
)

// migrationsFS holds every migrations/NNN_*.sql file, compiled into the binary.
// Adding a migration is just dropping a correctly-numbered .sql file into the
// directory — no Go edit, no hand-maintained order list.
//
//go:embed migrations/*.sql
var migrationsFS embed.FS

// embeddedMigrations returns every migration SQL payload in filename order. The
// zero-padded numeric prefix (001_, 002_, …) makes lexical sort == apply order
// (safe up to 999 migrations). A read error is impossible at runtime — the
// directory is embedded in the binary — so it signals a build/programming bug
// and panics rather than widening every caller's signature.
func embeddedMigrations() []string {
	names := migrationNames()
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, mustReadMigration(name))
	}
	return out
}

// migrationNames lists the embedded migration filenames in apply order.
func migrationNames() []string {
	entries, err := fs.ReadDir(migrationsFS, "migrations")
	if err != nil {
		panic(fmt.Sprintf("read embedded migrations dir: %v", err))
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names
}

func mustReadMigration(name string) string {
	b, err := migrationsFS.ReadFile("migrations/" + name)
	if err != nil {
		panic(fmt.Sprintf("read embedded migration %s: %v", name, err))
	}
	return string(b)
}

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
	// NotificationsLegacyRead forces the legacy live-join notifications query
	// even when the notifications_feed read-model is caught up. Emergency
	// escape hatch for the first read-model deploys — remove once the model
	// has served production for a while.
	NotificationsLegacyRead bool
	// Rules is the declarative rule registry driving the generated aggregate
	// schema, the ingest event_refs fan-out, retention, and read specs. Nil
	// falls back to rules.Default with the standard cap, so existing callers
	// and tests keep working without wiring.
	Rules *rules.Registry
}

type Store struct {
	conn ch.Conn
	// rules mirrors Config.Rules (defaulted when nil).
	rules *rules.Registry
	// notificationsLegacyRead mirrors Config.NotificationsLegacyRead.
	notificationsLegacyRead bool
	// Cached notifications_feed readiness (see notificationsFeedReady).
	feedReadyMu        sync.Mutex
	feedReady          bool
	feedReadyCheckedAt time.Time
}

// Rules returns the store's rule registry.
func (s *Store) Rules() *rules.Registry { return s.rules }

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
	reg := cfg.Rules
	if reg == nil {
		reg, err = rules.Default(20)
		if err != nil {
			return nil, fmt.Errorf("default rules: %w", err)
		}
	}
	return &Store{
		conn:                    retryConn{Conn: conn, readSettings: readSettings},
		rules:                   reg,
		notificationsLegacyRead: cfg.NotificationsLegacyRead,
	}, nil
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

// migrationsLedgerFile is the migration that creates the schema_migrations
// ledger itself. It is applied unconditionally (idempotent CREATE) so the
// ledger exists before we try to read it.
const migrationsLedgerFile = "000_schema_migrations.sql"

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

	// Bootstrap the ledger first — its own migration is idempotent and cheap.
	for _, stmt := range splitSQLStatements(mustReadMigration(migrationsLedgerFile)) {
		if err := s.conn.Exec(migCtx, stmt); err != nil {
			return fmt.Errorf("migration ledger bootstrap failed: %w", err)
		}
	}
	applied, err := s.appliedMigrations(ctx)
	if err != nil {
		return fmt.Errorf("read schema_migrations ledger: %w", err)
	}

	// Apply only unrecorded migrations. Historically every file re-ran on every
	// deploy "so the backfills keep converging with the live MVs" — that design
	// stopped scaling once the backfill sources crossed billions of rows (a
	// full event_tags re-aggregation per deploy, twice). Statement idempotency
	// is still mandatory (migrations_test.go): the ledger is the fast-path, not
	// the safety net, and deleting a row from schema_migrations deliberately
	// re-runs that file.
	for _, name := range migrationNames() {
		if _, done := applied[name]; done {
			continue
		}
		start := time.Now()
		for _, stmt := range splitSQLStatements(mustReadMigration(name)) {
			if err := s.conn.Exec(migCtx, stmt); err != nil {
				return fmt.Errorf("migration %s failed: %w", name, err)
			}
		}
		if err := s.recordMigration(ctx, name); err != nil {
			return fmt.Errorf("record migration %s: %w", name, err)
		}
		slog.Info("migration applied", "name", name, "duration", time.Since(start))
	}

	// Rule-derived schema: every declared relationship owns an aggregate
	// table (and, for the ingest tier, the materialized view feeding it),
	// generated from the registry rather than hand-written SQL. Tables that
	// did not exist before this pass get a one-shot historical backfill so a
	// newly declared rule immediately covers data that predates it — the
	// "prototype the count, then declare it" flow.
	if err := s.applyGeneratedSchema(migCtx); err != nil {
		return fmt.Errorf("generated schema failed: %w", err)
	}

	// The CREATEs above ensure declared tables/views exist; the reconciler then
	// strips anything the embedded SQL + rule registry no longer declare and
	// evolves columns, making the declarations the single source of truth.
	if err := s.reconcileSchema(ctx, schemaReconcileMode()); err != nil {
		return fmt.Errorf("schema reconcile failed: %w", err)
	}
	return nil
}

// applyGeneratedSchema creates the registry-derived tables and views, then
// backfills any aggregate table created for the first time. Backfills run
// against empty tables only, so uniq states stay exact and sum states are
// never double-counted.
func (s *Store) applyGeneratedSchema(ctx context.Context) error {
	existing, err := s.readActualTables(ctx)
	if err != nil {
		return fmt.Errorf("read existing tables: %w", err)
	}

	for _, stmt := range s.rules.GeneratedDDL() {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("generated DDL failed: %w\nstatement:\n%s", err, stmt)
		}
	}

	_, refsExisted := existing["event_refs"]
	needRefsReplay := false
	for _, rel := range s.rules.Relationships() {
		table := rules.TableName(rel.Name)
		if _, ok := existing[table]; ok {
			continue
		}
		if sql, ok := rules.BackfillSQL(rel); ok {
			start := time.Now()
			if err := s.conn.Exec(ctx, sql); err != nil {
				return fmt.Errorf("backfill %s: %w", rel.Name, err)
			}
			slog.Info("rule backfilled", "rule", rel.Name, "duration", time.Since(start))
			continue
		}
		if rel.Ref.Extractor != "" && rel.Refresh == rules.RefreshIngest {
			needRefsReplay = true
		}
	}

	// Extractor-based history must be replayed through Go: the sources are
	// raw events, not tag rows. One pass repopulates event_refs; the
	// materialized views fire per inserted row and fill the new aggregates.
	if needRefsReplay || (!refsExisted && len(s.rules.IngestExtractorRules()) > 0) {
		if err := s.replayEventRefs(ctx); err != nil {
			return fmt.Errorf("replay event_refs: %w", err)
		}
	}
	return nil
}

// appliedMigrations returns the set of migration filenames recorded in the
// schema_migrations ledger.
func (s *Store) appliedMigrations(ctx context.Context) (map[string]struct{}, error) {
	rows, err := s.conn.Query(ctx, "SELECT DISTINCT name FROM schema_migrations")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	applied := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		applied[name] = struct{}{}
	}
	return applied, rows.Err()
}

// recordMigration marks a migration file as applied. ReplacingMergeTree keyed
// on name makes repeat records converge instead of duplicating.
func (s *Store) recordMigration(ctx context.Context, name string) error {
	return s.conn.Exec(ctx, "INSERT INTO schema_migrations (name) VALUES (?)", name)
}

func (s *Store) Backfill(ctx context.Context) error {
	// Rebuild every rule-derived aggregate from raw history: truncate so the
	// uniq/sum states are exact, then re-run each rule's backfill SELECT.
	// Extractor-based rules are rebuilt by replayEventRefs below.
	statements := []string{"TRUNCATE TABLE IF EXISTS event_refs"}
	for _, rel := range s.rules.Relationships() {
		statements = append(statements, "TRUNCATE TABLE IF EXISTS "+rules.TableName(rel.Name))
	}
	for _, rel := range s.rules.Relationships() {
		if sql, ok := rules.BackfillSQL(rel); ok {
			statements = append(statements, sql)
		}
	}
	statements = append(statements,
		"TRUNCATE TABLE IF EXISTS profiles_latest",
		"TRUNCATE TABLE IF EXISTS notification_candidates",
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
	)
	for _, stmt := range statements {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("backfill failed: %w", err)
		}
	}
	if err := s.replayEventRefs(ctx); err != nil {
		return fmt.Errorf("backfill event refs failed: %w", err)
	}
	return nil
}

// replayEventRefs re-derives event_refs rows from raw history for every
// ingest-tier extractor rule: one pass over the rules' source kinds, each
// event run through its extractor. The materialized views feeding the
// aggregate tables fire per inserted row, so this also (re)fills those
// aggregates — callers must ensure the destination aggregates are empty or
// freshly created, or sums would double-count.
func (s *Store) replayEventRefs(ctx context.Context) error {
	extractorRules := s.rules.IngestExtractorRules()
	if len(extractorRules) == 0 {
		return nil
	}
	kindSet := map[int]struct{}{}
	for _, rel := range extractorRules {
		for _, k := range rel.Kinds {
			kindSet[k] = struct{}{}
		}
	}
	kinds := make([]string, 0, len(kindSet))
	for k := range kindSet {
		kinds = append(kinds, strconv.Itoa(k))
	}
	sort.Strings(kinds)

	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
		FROM nostr_events FINAL
		WHERE kind IN (%s)
	`, strings.Join(kinds, ", ")))
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

	batch, err := s.prepareInsertBatch(ctx, "INSERT INTO event_refs")
	if err != nil {
		return err
	}
	defer closeUnsentBatch(batch)
	appended := false
	for _, view := range events {
		event := eventViewToNostrEvent(view)
		for _, rel := range extractorRules {
			if !kindIn(rel.Kinds, event.Kind) {
				continue
			}
			for _, ref := range rules.Extractor(rel.Ref.Extractor)(event) {
				appended = true
				if err := batch.Append(
					rel.Name,
					event.ID,
					event.PubKey,
					view.CreatedAt,
					ref.Target,
					ref.Value,
				); err != nil {
					return err
				}
			}
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

	extractorRules := s.rules.IngestExtractorRules()
	var refsBatch chdriver.Batch
	if len(extractorRules) > 0 {
		refsBatch, err = s.prepareInsertBatch(ctx, "INSERT INTO event_refs")
		if err != nil {
			return err
		}
		defer closeUnsentBatch(refsBatch)
	}
	refsAppended := false

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

		for _, rel := range extractorRules {
			if !kindIn(rel.Kinds, event.Kind) {
				continue
			}
			for _, ref := range rules.Extractor(rel.Ref.Extractor)(event) {
				refsAppended = true
				if err := refsBatch.Append(
					rel.Name,
					event.ID,
					event.PubKey,
					createdAt,
					ref.Target,
					ref.Value,
				); err != nil {
					return err
				}
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
	if refsAppended {
		if err := refsBatch.Send(); err != nil {
			return err
		}
	}
	return nil
}

func kindIn(kinds []int, kind int) bool {
	for _, k := range kinds {
		if k == kind {
			return true
		}
	}
	return false
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
