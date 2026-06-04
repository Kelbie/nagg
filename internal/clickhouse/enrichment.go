package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/vertex-lab/nagg/internal/enrich"
)

const emptyCursorEventID = "0000000000000000000000000000000000000000000000000000000000000000"

func (s *Store) LoadEnrichmentState(ctx context.Context, task string) (enrich.State, bool, error) {
	task = strings.TrimSpace(task)
	if task == "" {
		return enrich.State{}, false, fmt.Errorf("enrichment task is required")
	}

	rows, err := s.conn.Query(ctx, `
		SELECT task, cursor_created_at, cursor_event_id, processed, failed, updated_at
		FROM enrichment_state FINAL
		WHERE task = ?
		LIMIT 1
	`, task)
	if err != nil {
		return enrich.State{}, false, err
	}
	defer rows.Close()
	if !rows.Next() {
		return enrich.State{}, false, rows.Err()
	}

	var state enrich.State
	if err := rows.Scan(
		&state.Task,
		&state.Cursor.CreatedAt,
		&state.Cursor.EventID,
		&state.Processed,
		&state.Failed,
		&state.UpdatedAt,
	); err != nil {
		return enrich.State{}, false, err
	}
	if state.Cursor.EventID == emptyCursorEventID {
		state.Cursor.EventID = ""
	}
	return state, true, rows.Err()
}

func (s *Store) FetchEnrichmentEvents(ctx context.Context, cursor enrich.Cursor, limit int) ([]enrich.Event, error) {
	if limit <= 0 {
		limit = 256
	}

	query := `
		SELECT id, pubkey, kind, created_at, content, tags_json
		FROM nostr_events FINAL
	`
	args := []any{}
	if !cursor.CreatedAt.IsZero() {
		if cursor.EventID == "" {
			query += " WHERE created_at > ?"
			args = append(args, cursor.CreatedAt)
		} else {
			query += " WHERE created_at > ? OR (created_at = ? AND id > ?)"
			args = append(args, cursor.CreatedAt, cursor.CreatedAt, cursor.EventID)
		}
	}
	query += fmt.Sprintf(" ORDER BY created_at ASC, id ASC LIMIT %d", limit)

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	events := []enrich.Event{}
	for rows.Next() {
		var event enrich.Event
		var kind uint32
		var tagsJSON string
		if err := rows.Scan(&event.ID, &event.PubKey, &kind, &event.CreatedAt, &event.Content, &tagsJSON); err != nil {
			return nil, err
		}
		event.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &event.Tags)
		events = append(events, event)
	}
	return events, rows.Err()
}

func (s *Store) WriteEnrichmentAnnotations(ctx context.Context, annotations []enrich.Annotation) error {
	if len(annotations) == 0 {
		return nil
	}
	tagRows, metricRows, embeddingRows, clusterRows := countAnnotationRows(annotations)
	if tagRows > 0 {
		if err := s.writeDerivedTags(ctx, annotations); err != nil {
			return err
		}
	}
	if metricRows > 0 {
		if err := s.writeDerivedMetrics(ctx, annotations); err != nil {
			return err
		}
	}
	if embeddingRows > 0 {
		if err := s.writeEventEmbeddings(ctx, annotations); err != nil {
			return err
		}
	}
	if clusterRows > 0 {
		if err := s.writeTrendingClusters(ctx, annotations); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) SaveEnrichmentState(ctx context.Context, state enrich.State) error {
	task := strings.TrimSpace(state.Task)
	if task == "" {
		return fmt.Errorf("enrichment task is required")
	}
	cursorCreatedAt := state.Cursor.CreatedAt
	if cursorCreatedAt.IsZero() {
		cursorCreatedAt = time.Unix(0, 0).UTC()
	}
	cursorEventID := strings.TrimSpace(state.Cursor.EventID)
	if cursorEventID == "" {
		cursorEventID = emptyCursorEventID
	}
	updatedAt := state.UpdatedAt
	if updatedAt.IsZero() {
		updatedAt = time.Now().UTC()
	}
	return s.conn.Exec(ctx, `
		INSERT INTO enrichment_state (task, cursor_created_at, cursor_event_id, processed, failed, updated_at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, task, cursorCreatedAt, cursorEventID, state.Processed, state.Failed, updatedAt)
}

func countAnnotationRows(annotations []enrich.Annotation) (int, int, int, int) {
	var tagRows int
	var metricRows int
	var embeddingRows int
	var clusterRows int
	for _, annotation := range annotations {
		for _, tag := range annotation.Tags {
			if strings.TrimSpace(tag.Key) != "" {
				tagRows++
			}
		}
		for _, metric := range annotation.Metrics {
			if strings.TrimSpace(metric.Name) != "" {
				metricRows++
			}
		}
		if len(annotation.Embedding) > 0 {
			embeddingRows++
		}
		for _, cluster := range annotation.Clusters {
			if strings.TrimSpace(cluster.ID) != "" {
				clusterRows++
			}
		}
	}
	return tagRows, metricRows, embeddingRows, clusterRows
}

func (s *Store) writeDerivedTags(ctx context.Context, annotations []enrich.Annotation) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO derived_tags")
	if err != nil {
		return err
	}
	for _, annotation := range annotations {
		event := annotation.Event
		computedAt := annotationComputedAt(annotation)
		modelVersion := annotationModelVersion(annotation)
		for i, tag := range annotation.Tags {
			key := strings.TrimSpace(tag.Key)
			if key == "" {
				continue
			}
			if err := batch.Append(
				event.ID,
				event.PubKey,
				uint32(event.Kind),
				event.CreatedAt,
				uint16(i),
				key,
				tag.Value,
				tag.Extra,
				modelVersion,
				computedAt,
			); err != nil {
				return err
			}
		}
	}
	return batch.Send()
}

func (s *Store) writeDerivedMetrics(ctx context.Context, annotations []enrich.Annotation) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO derived_metrics")
	if err != nil {
		return err
	}
	for _, annotation := range annotations {
		event := annotation.Event
		computedAt := annotationComputedAt(annotation)
		modelVersion := annotationModelVersion(annotation)
		for _, metric := range annotation.Metrics {
			name := strings.TrimSpace(metric.Name)
			if name == "" {
				continue
			}
			if err := batch.Append(
				event.ID,
				event.PubKey,
				uint32(event.Kind),
				event.CreatedAt,
				name,
				metric.Value,
				modelVersion,
				computedAt,
			); err != nil {
				return err
			}
		}
	}
	return batch.Send()
}

func (s *Store) writeEventEmbeddings(ctx context.Context, annotations []enrich.Annotation) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO event_embeddings")
	if err != nil {
		return err
	}
	for _, annotation := range annotations {
		if len(annotation.Embedding) == 0 {
			continue
		}
		event := annotation.Event
		if err := batch.Append(
			event.ID,
			event.PubKey,
			uint32(event.Kind),
			event.CreatedAt,
			annotation.Embedding,
			annotationModelVersion(annotation),
			annotationComputedAt(annotation),
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *Store) writeTrendingClusters(ctx context.Context, annotations []enrich.Annotation) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO trending_clusters")
	if err != nil {
		return err
	}
	seen := map[string]struct{}{}
	for _, annotation := range annotations {
		for _, cluster := range annotation.Clusters {
			id := strings.TrimSpace(cluster.ID)
			if id == "" {
				continue
			}
			if _, ok := seen[id]; ok {
				continue
			}
			seen[id] = struct{}{}
			if err := batch.Append(
				id,
				cluster.Window,
				cluster.StartedAt,
				cluster.Category,
				cluster.Subcategory,
				cluster.Title,
				cluster.Description,
				cluster.Centroid,
				cluster.EventCount,
				cluster.Score,
				clusterComputedAt(annotation, cluster),
			); err != nil {
				return err
			}
		}
	}
	return batch.Send()
}

func annotationComputedAt(annotation enrich.Annotation) time.Time {
	if !annotation.ComputedAt.IsZero() {
		return annotation.ComputedAt
	}
	return time.Now().UTC()
}

func annotationModelVersion(annotation enrich.Annotation) string {
	version := strings.TrimSpace(annotation.ModelVersion)
	if version == "" {
		return "unknown"
	}
	return version
}

func clusterComputedAt(annotation enrich.Annotation, cluster enrich.TrendingCluster) time.Time {
	if !cluster.ComputedAt.IsZero() {
		return cluster.ComputedAt
	}
	return annotationComputedAt(annotation)
}
