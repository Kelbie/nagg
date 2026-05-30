package clickhouse

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/nbd-wtf/go-nostr"
)

//go:embed migrations/001_ingestion.sql
var ingestionMigration string

type Config struct {
	Addr     string
	Database string
	Username string
	Password string
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
	conn, err := ch.Open(&ch.Options{
		Addr: []string{cfg.Addr},
		Auth: ch.Auth{
			Database: cfg.Database,
			Username: cfg.Username,
			Password: cfg.Password,
		},
		DialTimeout:      10 * time.Second,
		ConnOpenStrategy: ch.ConnOpenInOrder,
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(ctx); err != nil {
		return nil, err
	}
	return &Store{conn: conn}, nil
}

func (s *Store) Close() error {
	return s.conn.Close()
}

func (s *Store) Migrate(ctx context.Context) error {
	for _, stmt := range splitSQLStatements(ingestionMigration) {
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}
	return nil
}

func (s *Store) InsertEvents(ctx context.Context, records []EventRecord) error {
	if len(records) == 0 {
		return nil
	}

	eventsBatch, err := s.conn.PrepareBatch(ctx, "INSERT INTO nostr_events")
	if err != nil {
		return err
	}
	relaysBatch, err := s.conn.PrepareBatch(ctx, "INSERT INTO event_seen_relays")
	if err != nil {
		return err
	}
	tagsBatch, err := s.conn.PrepareBatch(ctx, "INSERT INTO event_tags")
	if err != nil {
		return err
	}

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
	}

	if err := eventsBatch.Send(); err != nil {
		return err
	}
	if err := relaysBatch.Send(); err != nil {
		return err
	}
	return tagsBatch.Send()
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
