package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
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

type NoteStats struct {
	LikeCount   uint64 `json:"likeCount"`
	RepostCount uint64 `json:"repostCount"`
	ReplyCount  uint64 `json:"replyCount"`
	SatsZapped  uint64 `json:"satsZapped"`
}

type FollowCounts struct {
	Follows   uint64 `json:"follows"`
	Followers uint64 `json:"followers"`
}

type ProfileRow struct {
	PubKey      string    `json:"pubkey"`
	EventID     string    `json:"event_id"`
	CreatedAt   time.Time `json:"created_at"`
	Name        string    `json:"name"`
	DisplayName string    `json:"display_name"`
	Picture     string    `json:"picture"`
	About       string    `json:"about"`
	NIP05       string    `json:"nip05"`
	LUD16       string    `json:"lud16"`
	LUD06       string    `json:"lud06"`
	Banner      string    `json:"banner"`
	Website     string    `json:"website"`
	RawJSON     string    `json:"raw_json"`
}

func (s *Store) QueryLatestEventsByPubKeys(ctx context.Context, pubkeys []string, kinds []int, limitPerPubKey uint64) (map[string][]EventView, error) {
	pubkeys = uniqueStrings(pubkeys)
	if len(pubkeys) == 0 {
		return map[string][]EventView{}, nil
	}
	if limitPerPubKey == 0 || limitPerPubKey > 20 {
		limitPerPubKey = 1
	}

	where, args := eventWhere("e", nil, pubkeys, kinds, nil)
	query := fmt.Sprintf(`
		SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
		FROM nostr_events AS e FINAL
		%s
		ORDER BY e.pubkey ASC, e.created_at DESC, e.id DESC
		LIMIT %d BY e.pubkey
	`, where, limitPerPubKey)
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make(map[string][]EventView, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = nil
	}
	for rows.Next() {
		var tagsJSON string
		var ev EventView
		var kind uint32
		if err := rows.Scan(&ev.ID, &ev.PubKey, &kind, &ev.CreatedAt, &ev.Content, &tagsJSON, &ev.Sig, &ev.UpdatedAt); err != nil {
			return nil, err
		}
		ev.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &ev.Tags)
		out[ev.PubKey] = append(out[ev.PubKey], ev)
	}
	return out, rows.Err()
}

func (s *Store) NoteStats(ctx context.Context, ids []string) (map[string]NoteStats, error) {
	ids = uniqueStrings(ids)
	out := make(map[string]NoteStats, len(ids))
	for _, id := range ids {
		out[id] = NoteStats{}
	}
	if len(ids) == 0 {
		return out, nil
	}

	var mu sync.Mutex
	set := func(id string, update func(*NoteStats)) {
		mu.Lock()
		defer mu.Unlock()
		stats := out[id]
		update(&stats)
		out[id] = stats
	}

	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		return s.mergeCount(ctx, "note_like_counts", "likes", ids, func(id string, value uint64) {
			set(id, func(stats *NoteStats) { stats.LikeCount = value })
		})
	})
	g.Go(func() error {
		return s.mergeCount(ctx, "note_repost_counts", "reposts", ids, func(id string, value uint64) {
			set(id, func(stats *NoteStats) { stats.RepostCount = value })
		})
	})
	g.Go(func() error {
		return s.mergeCount(ctx, "note_reply_counts", "replies", ids, func(id string, value uint64) {
			set(id, func(stats *NoteStats) { stats.ReplyCount = value })
		})
	})
	g.Go(func() error {
		return s.mergeZapStats(ctx, ids, func(id string, value uint64) {
			set(id, func(stats *NoteStats) { stats.SatsZapped = value })
		})
	})
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return out, nil
}

func (s *Store) mergeZapStats(ctx context.Context, ids []string, set func(string, uint64)) error {
	rows, err := s.conn.Query(ctx, `
		SELECT target_event_id, sumMerge(sats)
		FROM note_zap_totals
		WHERE target_event_id IN (?)
		GROUP BY target_event_id
	`, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var value uint64
		if err := rows.Scan(&id, &value); err != nil {
			return err
		}
		set(id, value)
	}
	return rows.Err()
}

func (s *Store) mergeCount(ctx context.Context, table, column string, ids []string, set func(string, uint64)) error {
	query := fmt.Sprintf(`
		SELECT target_event_id, uniqMerge(%s)
		FROM %s
		WHERE target_event_id IN (?)
		GROUP BY target_event_id
	`, column, table)
	rows, err := s.conn.Query(ctx, query, ids)
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		var value uint64
		if err := rows.Scan(&id, &value); err != nil {
			return err
		}
		set(id, value)
	}
	return rows.Err()
}

func (s *Store) FollowCounts(ctx context.Context, pubkey string) (FollowCounts, error) {
	var out FollowCounts
	latest, err := s.QueryLatestEventsByPubKeys(ctx, []string{pubkey}, []int{3}, 1)
	if err != nil {
		return out, err
	}
	seenFollows := map[string]struct{}{}
	for _, event := range latest[pubkey] {
		for _, tag := range event.Tags {
			if len(tag) >= 2 && tag[0] == "p" && tag[1] != "" {
				seenFollows[tag[1]] = struct{}{}
			}
		}
	}
	out.Follows = uint64(len(seenFollows))

	if err := s.conn.QueryRow(ctx, `
		SELECT uniqExact(pubkey)
		FROM event_tags
		WHERE kind = 3 AND tag_key = 'p' AND tag_value = ?
	`, pubkey).Scan(&out.Followers); err != nil {
		return out, err
	}
	return out, nil
}

func (s *Store) LatestProfiles(ctx context.Context, pubkeys []string) (map[string]ProfileRow, error) {
	pubkeys = uniqueStrings(pubkeys)
	out := make(map[string]ProfileRow, len(pubkeys))
	if len(pubkeys) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT pubkey, event_id, created_at, name, display_name, picture, about, nip05, lud16, lud06, banner, website, raw_json
		FROM profiles_latest FINAL
		WHERE pubkey IN (?)
	`, pubkeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var profile ProfileRow
		if err := rows.Scan(
			&profile.PubKey,
			&profile.EventID,
			&profile.CreatedAt,
			&profile.Name,
			&profile.DisplayName,
			&profile.Picture,
			&profile.About,
			&profile.NIP05,
			&profile.LUD16,
			&profile.LUD06,
			&profile.Banner,
			&profile.Website,
			&profile.RawJSON,
		); err != nil {
			return nil, err
		}
		out[profile.PubKey] = profile
	}
	return out, rows.Err()
}

func (s *Store) FeedByPubKeys(ctx context.Context, pubkeys []string, until int64, limit, offset uint64) ([]EventView, error) {
	return s.FollowsFeed(ctx, pubkeys, until, limit, offset)
}

func (s *Store) FollowsFeed(ctx context.Context, pubkeys []string, until int64, limit, offset uint64) ([]EventView, error) {
	pubkeys = uniqueStrings(pubkeys)
	if len(pubkeys) == 0 {
		return []EventView{}, nil
	}
	if limit == 0 || limit > 100 {
		limit = 30
	}
	if until <= 0 {
		until = time.Now().Add(time.Second).Unix()
	}

	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
		FROM nostr_events AS e FINAL
		WHERE e.pubkey IN (?) AND e.kind IN (1, 6, 16) AND e.created_at < ?
		ORDER BY e.created_at DESC, e.id DESC
		LIMIT %d OFFSET %d
	`, limit, offset), pubkeys, time.Unix(until, 0).UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEventRows(rows)
}

func (s *Store) TrendingFeed(ctx context.Context, since time.Time, limit uint64) ([]EventView, error) {
	if limit == 0 || limit > 100 {
		limit = 30
	}
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	since, cacheKey := trendingCacheKey(since, limit)
	if cached, ok := s.getTrendingCache(cacheKey); ok {
		return cached, nil
	}
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		WITH
		  likes AS (
		    SELECT target_event_id, uniqMerge(likes) AS like_count
		    FROM note_like_counts
		    GROUP BY target_event_id
		  ),
		  reposts AS (
		    SELECT target_event_id, uniqMerge(reposts) AS repost_count
		    FROM note_repost_counts
		    GROUP BY target_event_id
		  ),
		  replies AS (
		    SELECT target_event_id, uniqMerge(replies) AS reply_count
		    FROM note_reply_counts
		    GROUP BY target_event_id
		  )
		SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
		FROM nostr_events AS e FINAL
		LEFT JOIN likes ON likes.target_event_id = e.id
		LEFT JOIN reposts ON reposts.target_event_id = e.id
		LEFT JOIN replies ON replies.target_event_id = e.id
		WHERE e.kind = 1 AND e.created_at >= ?
		ORDER BY (ifNull(like_count, 0) + 2 * ifNull(repost_count, 0) + ifNull(reply_count, 0)) DESC, e.created_at DESC
		LIMIT %d
	`, limit), since.UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	events, err := scanEventRows(rows)
	if err != nil {
		return nil, err
	}
	s.setTrendingCache(cacheKey, events, 30*time.Second)
	return events, nil
}

func (s *Store) getTrendingCache(key string) ([]EventView, bool) {
	s.trendingCacheMu.Lock()
	defer s.trendingCacheMu.Unlock()
	entry, ok := s.trendingCache[key]
	if !ok {
		return nil, false
	}
	if time.Now().After(entry.expiresAt) {
		delete(s.trendingCache, key)
		return nil, false
	}
	return cloneEvents(entry.events), true
}

func trendingCacheKey(since time.Time, limit uint64) (time.Time, string) {
	since = since.UTC().Truncate(time.Minute)
	return since, fmt.Sprintf("%d:%d", since.Unix(), limit)
}

func (s *Store) setTrendingCache(key string, events []EventView, ttl time.Duration) {
	s.trendingCacheMu.Lock()
	defer s.trendingCacheMu.Unlock()
	s.trendingCache[key] = trendingCacheEntry{
		expiresAt: time.Now().Add(ttl),
		events:    cloneEvents(events),
	}
}

func (s *Store) ThreadEvents(ctx context.Context, id string, limit int) (*EventView, []EventView, error) {
	root, err := s.EventByID(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if limit <= 0 || limit > 2000 {
		limit = 1000
	}

	eventsByID := map[string]EventView{root.ID: *root}
	visited := map[string]struct{}{}
	frontier := []string{root.ID}

	for depth := 0; depth < 8 && len(frontier) > 0 && len(eventsByID) < limit; depth++ {
		batch := takeUnvisited(visited, frontier, 100)
		if len(batch) == 0 {
			break
		}
		remaining := limit - len(eventsByID)
		if remaining <= 0 {
			break
		}
		events, err := s.QueryEvents(ctx, EventQueryInput{
			Tags:  []TagFilter{{Key: "e", Values: batch}},
			Kinds: []int{1, 1111},
			Limit: uint64(min(remaining, 500)),
		})
		if err != nil {
			return nil, nil, err
		}
		frontier = frontier[:0]
		for _, event := range events {
			if _, ok := eventsByID[event.ID]; ok {
				continue
			}
			eventsByID[event.ID] = event
			frontier = append(frontier, event.ID)
		}
	}

	events := make([]EventView, 0, len(eventsByID)-1)
	for _, event := range eventsByID {
		if event.ID != root.ID {
			events = append(events, event)
		}
	}
	return root, events, nil
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
		FROM nostr_events AS e FINAL
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

type eventRows interface {
	Next() bool
	Scan(dest ...any) error
	Err() error
}

func scanEventRows(rows eventRows) ([]EventView, error) {
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
		spec.from = "nostr_events AS e FINAL"
		spec.where, args = eventWhere("e", input.IDs, input.PubKeys, input.Kinds, input.Tags)
	case "TAGS":
		spec.from = "event_tags t INNER JOIN nostr_events AS e FINAL ON e.id = t.event_id"
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

func uniqueStrings(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
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

func takeUnvisited(visited map[string]struct{}, ids []string, max int) []string {
	out := make([]string, 0, min(len(ids), max))
	for _, id := range ids {
		if _, ok := visited[id]; ok {
			continue
		}
		visited[id] = struct{}{}
		out = append(out, id)
		if len(out) >= max {
			break
		}
	}
	return out
}

func cloneEvents(events []EventView) []EventView {
	out := make([]EventView, len(events))
	copy(out, events)
	for i := range out {
		if out[i].Tags == nil {
			continue
		}
		out[i].Tags = append([][]string(nil), out[i].Tags...)
		for j := range out[i].Tags {
			out[i].Tags[j] = append([]string(nil), out[i].Tags[j]...)
		}
	}
	return out
}
