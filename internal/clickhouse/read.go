package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

type EventView struct {
	ID        string
	PubKey    string
	Kind      int
	CreatedAt time.Time
	Content   string
	Tags      [][]string
	Sig       string
	UpdatedAt time.Time
}

type ProfileView struct {
	PubKey      string
	Name        string
	DisplayName string
	Picture     string
	About       string
	NIP05       string
	LUD16       string
	UpdatedAt   time.Time
}

type ActorEdge struct {
	PubKey    string
	Content   string
	CreatedAt time.Time
	Count     uint64
}

type CommentView struct {
	ID        string
	PubKey    string
	Content   string
	CreatedAt time.Time
}

type AggregateInput struct {
	Dataset string
	GroupBy []string
	Metrics []string
	Kinds   []int
	Limit   uint64
}

type AggregateRow struct {
	Dimensions map[string]string `json:"dimensions"`
	Metrics    map[string]uint64 `json:"metrics"`
}

func (s *Store) EventByID(ctx context.Context, id string) (*EventView, error) {
	row := s.conn.QueryRow(ctx, `
		SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
		FROM nostr_events
		WHERE id = ?
		ORDER BY last_seen_at DESC
		LIMIT 1
	`, id)

	var tagsJSON string
	var ev EventView
	if err := row.Scan(&ev.ID, &ev.PubKey, &ev.Kind, &ev.CreatedAt, &ev.Content, &tagsJSON, &ev.Sig, &ev.UpdatedAt); err != nil {
		if err == sql.ErrNoRows {
			return &EventView{ID: id, UpdatedAt: time.Now().UTC()}, nil
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(tagsJSON), &ev.Tags)
	return &ev, nil
}

func (s *Store) ProfileByPubKey(ctx context.Context, pubkey string) (*ProfileView, error) {
	profile := &ProfileView{PubKey: pubkey, UpdatedAt: time.Now().UTC()}
	row := s.conn.QueryRow(ctx, `
		SELECT content, created_at
		FROM nostr_events
		WHERE kind = 0 AND pubkey = ?
		ORDER BY created_at DESC
		LIMIT 1
	`, pubkey)

	var content string
	if err := row.Scan(&content, &profile.UpdatedAt); err == nil {
		var raw map[string]any
		if json.Unmarshal([]byte(content), &raw) == nil {
			profile.Name = stringValue(raw["name"])
			profile.DisplayName = firstString(raw["display_name"], raw["displayName"])
			profile.Picture = stringValue(raw["picture"])
			profile.About = stringValue(raw["about"])
			profile.NIP05 = stringValue(raw["nip05"])
			profile.LUD16 = stringValue(raw["lud16"])
		}
	}
	return profile, nil
}

func (s *Store) LikeCount(ctx context.Context, target string) (uint64, error) {
	return s.countTaggedEvents(ctx, []int{7}, target, "AND e.content IN ('', '+')")
}

func (s *Store) RepostCount(ctx context.Context, target string) (uint64, error) {
	return s.countTaggedEvents(ctx, []int{6, 16}, target, "")
}

func (s *Store) CommentCount(ctx context.Context, target string) (uint64, error) {
	return s.countTaggedEvents(ctx, []int{1, 1111}, target, "")
}

func (s *Store) DirectReplyCount(ctx context.Context, target string) (uint64, error) {
	return s.CommentCount(ctx, target)
}

func (s *Store) ThreadParticipants(ctx context.Context, target string) (uint64, error) {
	row := s.conn.QueryRow(ctx, `
		SELECT uniqExact(e.pubkey)
		FROM event_tags t
		INNER JOIN nostr_events e ON e.id = t.event_id
		WHERE t.tag_key IN ('e', 'E') AND t.tag_value = ? AND e.kind IN (1, 1111)
	`, target)
	var n uint64
	return n, row.Scan(&n)
}

func (s *Store) ReactionTallies(ctx context.Context, target string, limit uint64) ([]ActorEdge, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT e.content, uniqExact(e.id) AS c
		FROM event_tags t
		INNER JOIN nostr_events e ON e.id = t.event_id
		WHERE t.tag_key = 'e' AND t.tag_value = ? AND e.kind = 7
		GROUP BY e.content
		ORDER BY c DESC
		LIMIT ?
	`, target, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ActorEdge
	for rows.Next() {
		var edge ActorEdge
		if err := rows.Scan(&edge.Content, &edge.Count); err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, rows.Err()
}

func (s *Store) Likers(ctx context.Context, target string, limit uint64) ([]ActorEdge, error) {
	return s.actorEdges(ctx, []int{7}, target, "AND e.content IN ('', '+')", limit)
}

func (s *Store) Reposters(ctx context.Context, target string, limit uint64) ([]ActorEdge, error) {
	return s.actorEdges(ctx, []int{6, 16}, target, "", limit)
}

func (s *Store) Comments(ctx context.Context, target string, limit uint64, newest bool) ([]CommentView, error) {
	order := "ASC"
	if newest {
		order = "DESC"
	}
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		SELECT e.id, e.pubkey, e.content, e.created_at
		FROM event_tags t
		INNER JOIN nostr_events e ON e.id = t.event_id
		WHERE t.tag_key IN ('e', 'E') AND t.tag_value = ? AND e.kind IN (1, 1111)
		ORDER BY e.created_at %s, e.id %s
		LIMIT ?
	`, order, order), target, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CommentView
	for rows.Next() {
		var comment CommentView
		if err := rows.Scan(&comment.ID, &comment.PubKey, &comment.Content, &comment.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, comment)
	}
	return out, rows.Err()
}

func (s *Store) Followers(ctx context.Context, pubkey string) (uint64, error) {
	row := s.conn.QueryRow(ctx, `
		SELECT uniqExact(pubkey)
		FROM event_tags
		WHERE kind = 3 AND tag_key = 'p' AND tag_value = ?
	`, pubkey)
	var n uint64
	return n, row.Scan(&n)
}

func (s *Store) Following(ctx context.Context, pubkey string) (uint64, error) {
	row := s.conn.QueryRow(ctx, `
		SELECT uniqExact(tag_value)
		FROM event_tags
		WHERE kind = 3 AND tag_key = 'p' AND pubkey = ?
	`, pubkey)
	var n uint64
	return n, row.Scan(&n)
}

func (s *Store) FollowerList(ctx context.Context, pubkey string, limit uint64) ([]ActorEdge, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT pubkey, max(created_at) AS followed_at
		FROM event_tags
		WHERE kind = 3 AND tag_key = 'p' AND tag_value = ?
		GROUP BY pubkey
		ORDER BY followed_at DESC
		LIMIT ?
	`, pubkey, limit)
	return scanTwoColumnProfileEdges(rows, err)
}

func (s *Store) FollowingList(ctx context.Context, pubkey string, limit uint64) ([]ActorEdge, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT tag_value, max(created_at) AS followed_at
		FROM event_tags
		WHERE kind = 3 AND tag_key = 'p' AND pubkey = ?
		GROUP BY tag_value
		ORDER BY followed_at DESC
		LIMIT ?
	`, pubkey, limit)
	return scanTwoColumnProfileEdges(rows, err)
}

func (s *Store) FollowedBy(ctx context.Context, pubkey, viewerPubkey string) (bool, error) {
	row := s.conn.QueryRow(ctx, `
		SELECT count() > 0
		FROM event_tags
		WHERE kind = 3 AND tag_key = 'p' AND pubkey = ? AND tag_value = ?
	`, viewerPubkey, pubkey)
	var ok bool
	return ok, row.Scan(&ok)
}

func (s *Store) AggregateEvents(ctx context.Context, input AggregateInput) ([]AggregateRow, error) {
	if input.Limit == 0 || input.Limit > 1000 {
		input.Limit = 100
	}
	if len(input.Metrics) == 0 {
		input.Metrics = []string{"COUNT"}
	}

	spec, err := aggregateSpec(input)
	if err != nil {
		return nil, err
	}

	query := fmt.Sprintf("SELECT %s, %s FROM %s %s GROUP BY %s ORDER BY %s DESC LIMIT %d",
		strings.Join(spec.selectDims, ", "),
		strings.Join(spec.selectMetrics, ", "),
		spec.from,
		spec.where,
		strings.Join(spec.groupDims, ", "),
		spec.orderMetric,
		input.Limit,
	)

	rows, err := s.conn.Query(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []AggregateRow
	for rows.Next() {
		values := make([]any, 0, len(spec.scanDims)+len(spec.scanMetrics))
		dimValues := make([]string, len(spec.scanDims))
		metricValues := make([]uint64, len(spec.scanMetrics))
		for i := range dimValues {
			values = append(values, &dimValues[i])
		}
		for i := range metricValues {
			values = append(values, &metricValues[i])
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		row := AggregateRow{Dimensions: map[string]string{}, Metrics: map[string]uint64{}}
		for i, key := range spec.scanDims {
			row.Dimensions[key] = dimValues[i]
		}
		for i, key := range spec.scanMetrics {
			row.Metrics[key] = metricValues[i]
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) countTaggedEvents(ctx context.Context, kinds []int, target, extra string) (uint64, error) {
	row := s.conn.QueryRow(ctx, fmt.Sprintf(`
		SELECT uniqExact(e.id)
		FROM event_tags t
		INNER JOIN nostr_events e ON e.id = t.event_id
		WHERE t.tag_key IN ('e', 'E') AND t.tag_value = ? AND e.kind IN (%s) %s
	`, ints(kinds), extra), target)
	var n uint64
	return n, row.Scan(&n)
}

func (s *Store) actorEdges(ctx context.Context, kinds []int, target, extra string, limit uint64) ([]ActorEdge, error) {
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		SELECT e.pubkey, any(e.content), max(e.created_at) AS acted_at
		FROM event_tags t
		INNER JOIN nostr_events e ON e.id = t.event_id
		WHERE t.tag_key IN ('e', 'E') AND t.tag_value = ? AND e.kind IN (%s) %s
		GROUP BY e.pubkey
		ORDER BY acted_at DESC
		LIMIT ?
	`, ints(kinds), extra), target, limit)
	return scanThreeColumnActorEdges(rows, err)
}

type rowScanner interface {
	Next() bool
	Scan(...any) error
	Close() error
	Err() error
}

func scanTwoColumnProfileEdges(rows rowScanner, err error) ([]ActorEdge, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActorEdge
	for rows.Next() {
		var edge ActorEdge
		if err := rows.Scan(&edge.PubKey, &edge.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, rows.Err()
}

func scanThreeColumnActorEdges(rows rowScanner, err error) ([]ActorEdge, error) {
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ActorEdge
	for rows.Next() {
		var edge ActorEdge
		if err := rows.Scan(&edge.PubKey, &edge.Content, &edge.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, edge)
	}
	return out, rows.Err()
}

type aggSpec struct {
	selectDims    []string
	groupDims     []string
	scanDims      []string
	selectMetrics []string
	scanMetrics   []string
	orderMetric   string
	from          string
	where         string
}

func aggregateSpec(input AggregateInput) (aggSpec, error) {
	dataset := strings.ToUpper(input.Dataset)
	spec := aggSpec{where: "WHERE 1 = 1"}

	switch dataset {
	case "EVENTS":
		spec.from = "nostr_events e"
	case "TAGS":
		spec.from = "event_tags t"
	case "REACTIONS":
		spec.from = "event_tags t INNER JOIN nostr_events e ON e.id = t.event_id"
		spec.where += " AND e.kind = 7 AND t.tag_key IN ('e', 'E')"
	case "REPLIES":
		spec.from = "event_tags t INNER JOIN nostr_events e ON e.id = t.event_id"
		spec.where += " AND e.kind IN (1, 1111) AND t.tag_key IN ('e', 'E')"
	default:
		return spec, fmt.Errorf("unsupported dataset %q", input.Dataset)
	}

	if len(input.Kinds) > 0 && (dataset == "EVENTS" || dataset == "TAGS") {
		alias := "e"
		if dataset == "TAGS" {
			alias = "t"
		}
		spec.where += fmt.Sprintf(" AND %s.kind IN (%s)", alias, ints(input.Kinds))
	}

	for _, dim := range input.GroupBy {
		key := strings.ToUpper(dim)
		expr, err := dimensionExpr(dataset, key)
		if err != nil {
			return spec, err
		}
		alias := strings.ToLower(key)
		spec.selectDims = append(spec.selectDims, fmt.Sprintf("toString(%s) AS %s", expr, alias))
		spec.groupDims = append(spec.groupDims, alias)
		spec.scanDims = append(spec.scanDims, alias)
	}
	if len(spec.selectDims) == 0 {
		return spec, fmt.Errorf("at least one groupBy dimension is required")
	}

	for _, metric := range input.Metrics {
		key := strings.ToUpper(metric)
		expr, err := metricExpr(dataset, key)
		if err != nil {
			return spec, err
		}
		alias := strings.ToLower(key)
		spec.selectMetrics = append(spec.selectMetrics, fmt.Sprintf("%s AS %s", expr, alias))
		spec.scanMetrics = append(spec.scanMetrics, alias)
		if spec.orderMetric == "" {
			spec.orderMetric = alias
		}
	}
	return spec, nil
}

func dimensionExpr(dataset, key string) (string, error) {
	switch key {
	case "DAY":
		if dataset == "TAGS" {
			return "toDate(t.created_at)", nil
		}
		return "toDate(e.created_at)", nil
	case "HOUR":
		if dataset == "TAGS" {
			return "toStartOfHour(t.created_at)", nil
		}
		return "toStartOfHour(e.created_at)", nil
	case "KIND":
		if dataset == "TAGS" {
			return "t.kind", nil
		}
		return "e.kind", nil
	case "AUTHOR":
		if dataset == "TAGS" {
			return "t.pubkey", nil
		}
		return "e.pubkey", nil
	case "EVENT_ID":
		if dataset == "TAGS" {
			return "t.event_id", nil
		}
		return "e.id", nil
	case "TARGET_EVENT":
		if dataset == "REACTIONS" || dataset == "REPLIES" {
			return "t.tag_value", nil
		}
	case "REACTION":
		if dataset == "REACTIONS" {
			return "e.content", nil
		}
	case "TAG_KEY":
		if dataset == "TAGS" {
			return "t.tag_key", nil
		}
	case "TAG_VALUE":
		if dataset == "TAGS" {
			return "t.tag_value", nil
		}
	}
	return "", fmt.Errorf("dimension %q is not allowed for dataset %s", key, dataset)
}

func metricExpr(dataset, key string) (string, error) {
	switch key {
	case "COUNT":
		return "count()", nil
	case "UNIQUE_EVENTS":
		if dataset == "TAGS" {
			return "uniqExact(t.event_id)", nil
		}
		return "uniqExact(e.id)", nil
	case "UNIQUE_AUTHORS":
		if dataset == "TAGS" {
			return "uniqExact(t.pubkey)", nil
		}
		return "uniqExact(e.pubkey)", nil
	case "UNIQUE_TARGETS":
		if dataset == "REACTIONS" || dataset == "REPLIES" || dataset == "TAGS" {
			return "uniqExact(t.tag_value)", nil
		}
	}
	return "", fmt.Errorf("metric %q is not allowed for dataset %s", key, dataset)
}

func ints(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}

func stringValue(v any) string {
	s, _ := v.(string)
	return s
}

func firstString(values ...any) string {
	for _, value := range values {
		if s := stringValue(value); s != "" {
			return s
		}
	}
	return ""
}
