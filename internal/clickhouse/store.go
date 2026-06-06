package clickhouse

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
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

//go:embed migrations/006_notifications.sql
var notificationsMigration string

type Config struct {
	Addr         string
	Database     string
	Username     string
	Password     string
	MaxOpenConns int
	MaxIdleConns int
}

type Store struct {
	conn ch.Conn
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
		DialTimeout:      10 * time.Second,
		ConnOpenStrategy: ch.ConnOpenInOrder,
		MaxOpenConns:     maxOpenConns,
		MaxIdleConns:     maxIdleConns,
		ConnMaxLifetime:  time.Hour,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return &Store{conn: conn}, nil
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

func (s *Store) prepareInsertBatch(ctx context.Context, query string) (chdriver.Batch, error) {
	return s.conn.PrepareBatch(ctx, query, chdriver.WithReleaseConnection())
}

func closeUnsentBatch(batch chdriver.Batch) {
	if batch != nil && !batch.IsSent() {
		_ = batch.Close()
	}
}

func (s *Store) Migrate(ctx context.Context) error {
	for _, migration := range []string{ingestionMigration, appviewMigration, rankingMigration, derivedMigration, notificationsMigration} {
		for _, stmt := range splitSQLStatements(migration) {
			if err := s.conn.Exec(ctx, stmt); err != nil {
				return fmt.Errorf("migration failed: %w", err)
			}
		}
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
