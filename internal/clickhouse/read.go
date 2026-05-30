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
	ID        string     `json:"id"`
	PubKey    string     `json:"pubkey"`
	Kind      int        `json:"kind"`
	CreatedAt time.Time  `json:"createdAt"`
	Content   string     `json:"content"`
	Tags      [][]string `json:"tags"`
	Sig       string     `json:"sig"`
	UpdatedAt time.Time  `json:"updatedAt"`
}

type TagFilter struct {
	Key    string
	Value  string
	Values []string
}

type EventQueryInput struct {
	IDs     []string
	PubKeys []string
	Kinds   []int
	Tags    []TagFilter
	Limit   uint64
}

type AggregateInput struct {
	Dataset string
	GroupBy []string
	Metrics []string
	IDs     []string
	PubKeys []string
	Kinds   []int
	Tags    []TagFilter
	Limit   uint64
}

type AggregateRow struct {
	Dimensions map[string]string `json:"dimensions"`
	Metrics    map[string]uint64 `json:"metrics"`
}

func (s *Store) EventByID(ctx context.Context, id string) (*EventView, error) {
	events, err := s.QueryEvents(ctx, EventQueryInput{IDs: []string{id}, Limit: 1})
	if err != nil {
		return nil, err
	}
	if len(events) == 0 {
		return nil, sql.ErrNoRows
	}
	return &events[0], nil
}

func (s *Store) QueryEvents(ctx context.Context, input EventQueryInput) ([]EventView, error) {
	if input.Limit == 0 || input.Limit > 500 {
		input.Limit = 50
	}

	where, args := eventWhere("e", input.IDs, input.PubKeys, input.Kinds, input.Tags)
	args = append(args, input.Limit)
	rows, err := s.conn.Query(ctx, `
		SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
		FROM nostr_events e
		`+where+`
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventView
	for rows.Next() {
		var tagsJSON string
		var ev EventView
		var kind uint32
		if err := rows.Scan(&ev.ID, &ev.PubKey, &kind, &ev.CreatedAt, &ev.Content, &tagsJSON, &ev.Sig, &ev.UpdatedAt); err != nil {
			return nil, err
		}
		ev.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &ev.Tags)
		out = append(out, ev)
	}
	return out, rows.Err()
}

func (s *Store) AggregateEvents(ctx context.Context, input AggregateInput) ([]AggregateRow, error) {
	if input.Limit == 0 || input.Limit > 1000 {
		input.Limit = 100
	}
	if len(input.Metrics) == 0 {
		input.Metrics = []string{"COUNT"}
	}

	spec, args, err := aggregateSpec(input)
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

	rows, err := s.conn.Query(ctx, query, args...)
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

func aggregateSpec(input AggregateInput) (aggSpec, []any, error) {
	dataset := strings.ToUpper(input.Dataset)
	if dataset == "" {
		dataset = "EVENTS"
	}

	spec := aggSpec{}
	var args []any
	switch dataset {
	case "EVENTS":
		spec.from = "nostr_events e"
		spec.where, args = eventWhere("e", input.IDs, input.PubKeys, input.Kinds, input.Tags)
	case "TAGS":
		spec.from = "event_tags t INNER JOIN nostr_events e ON e.id = t.event_id"
		spec.where, args = tagWhere(input.IDs, input.PubKeys, input.Kinds, input.Tags)
	case "RELAYS":
		spec.from = "event_seen_relays r"
		spec.where = "WHERE 1 = 1"
	default:
		return spec, nil, fmt.Errorf("unsupported dataset %q", input.Dataset)
	}

	for _, dim := range input.GroupBy {
		key := strings.ToUpper(dim)
		expr, err := dimensionExpr(dataset, key)
		if err != nil {
			return spec, nil, err
		}
		alias := strings.ToLower(key)
		spec.selectDims = append(spec.selectDims, fmt.Sprintf("toString(%s) AS %s", expr, alias))
		spec.groupDims = append(spec.groupDims, alias)
		spec.scanDims = append(spec.scanDims, alias)
	}
	if len(spec.selectDims) == 0 {
		return spec, nil, fmt.Errorf("at least one groupBy dimension is required")
	}

	for _, metric := range input.Metrics {
		key := strings.ToUpper(metric)
		expr, err := metricExpr(dataset, key)
		if err != nil {
			return spec, nil, err
		}
		alias := strings.ToLower(key)
		spec.selectMetrics = append(spec.selectMetrics, fmt.Sprintf("%s AS %s", expr, alias))
		spec.scanMetrics = append(spec.scanMetrics, alias)
		if spec.orderMetric == "" {
			spec.orderMetric = alias
		}
	}
	return spec, args, nil
}

func eventWhere(alias string, ids, pubkeys []string, kinds []int, tags []TagFilter) (string, []any) {
	clauses := []string{"WHERE 1 = 1"}
	var args []any
	if len(ids) > 0 {
		clauses = append(clauses, alias+".id IN (?)")
		args = append(args, ids)
	}
	if len(pubkeys) > 0 {
		clauses = append(clauses, alias+".pubkey IN (?)")
		args = append(args, pubkeys)
	}
	if len(kinds) > 0 {
		clauses = append(clauses, alias+".kind IN ("+ints(kinds)+")")
	}
	for i, tag := range tags {
		subAlias := fmt.Sprintf("tf%d", i)
		clause := fmt.Sprintf("EXISTS (SELECT 1 FROM event_tags %s WHERE %s.event_id = %s.id AND %s.tag_key = ?", subAlias, subAlias, alias, subAlias)
		args = append(args, tag.Key)
		clause, args = addTagValueClause(clause, args, subAlias, tag)
		clauses = append(clauses, clause+")")
	}
	return strings.Join(clauses, " AND "), args
}

func tagWhere(ids, pubkeys []string, kinds []int, tags []TagFilter) (string, []any) {
	clauses := []string{"WHERE 1 = 1"}
	var args []any
	if len(ids) > 0 {
		clauses = append(clauses, "t.event_id IN (?)")
		args = append(args, ids)
	}
	if len(pubkeys) > 0 {
		clauses = append(clauses, "t.pubkey IN (?)")
		args = append(args, pubkeys)
	}
	if len(kinds) > 0 {
		clauses = append(clauses, "t.kind IN ("+ints(kinds)+")")
	}
	for _, tag := range tags {
		clause := "t.tag_key = ?"
		args = append(args, tag.Key)
		clause, args = addTagValueClause(clause, args, "t", tag)
		clauses = append(clauses, clause)
	}
	return strings.Join(clauses, " AND "), args
}

func addTagValueClause(clause string, args []any, alias string, tag TagFilter) (string, []any) {
	if tag.Value != "" {
		clause += " AND " + alias + ".tag_value = ?"
		args = append(args, tag.Value)
	} else if len(tag.Values) > 0 {
		clause += " AND " + alias + ".tag_value IN (?)"
		args = append(args, tag.Values)
	}
	return clause, args
}

func dimensionExpr(dataset, key string) (string, error) {
	switch key {
	case "DAY":
		return timeExpr(dataset, "toDate")
	case "HOUR":
		return timeExpr(dataset, "toStartOfHour")
	case "KIND":
		return eventOrTagExpr(dataset, "kind")
	case "PUBKEY", "AUTHOR":
		return eventOrTagExpr(dataset, "pubkey")
	case "EVENT_ID":
		if dataset == "TAGS" {
			return "t.event_id", nil
		}
		if dataset == "RELAYS" {
			return "r.event_id", nil
		}
		return "e.id", nil
	case "CONTENT":
		if dataset == "EVENTS" || dataset == "TAGS" {
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
	case "RELAY":
		if dataset == "RELAYS" {
			return "r.relay", nil
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
		if dataset == "RELAYS" {
			return "uniqExact(r.event_id)", nil
		}
		return "uniqExact(e.id)", nil
	case "UNIQUE_PUBKEYS", "UNIQUE_AUTHORS":
		if dataset == "TAGS" {
			return "uniqExact(t.pubkey)", nil
		}
		if dataset == "EVENTS" {
			return "uniqExact(e.pubkey)", nil
		}
	case "UNIQUE_TAG_VALUES":
		if dataset == "TAGS" {
			return "uniqExact(t.tag_value)", nil
		}
	case "UNIQUE_RELAYS":
		if dataset == "RELAYS" {
			return "uniqExact(r.relay)", nil
		}
	}
	return "", fmt.Errorf("metric %q is not allowed for dataset %s", key, dataset)
}

func timeExpr(dataset, fn string) (string, error) {
	switch dataset {
	case "TAGS":
		return fn + "(t.created_at)", nil
	case "RELAYS":
		return fn + "(r.last_seen_at)", nil
	default:
		return fn + "(e.created_at)", nil
	}
}

func eventOrTagExpr(dataset, col string) (string, error) {
	if dataset == "TAGS" {
		return "t." + col, nil
	}
	if dataset == "EVENTS" {
		return "e." + col, nil
	}
	return "", fmt.Errorf("%s is not available for dataset %s", col, dataset)
}

func ints(values []int) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.Itoa(value))
	}
	return strings.Join(parts, ",")
}
