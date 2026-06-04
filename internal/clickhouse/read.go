package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/vertex-lab/nagg/internal/vertex"
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
	Key           string
	Value         string
	Values        []string
	ExcludeValues []string
	Dataset       string
}

type EventQueryInput struct {
	IDs     []string
	PubKeys []string
	Kinds   []int
	Tags    []TagFilter
	Since   int64
	Until   int64
	Limit   uint64
	Offset  uint64
	Shuffle ShuffleInput
	Empty   bool
}

type ShuffleInput struct {
	Seed     string
	Counter  int
	Strength float64
}

type AggregateInput struct {
	Dataset string
	GroupBy []string
	Metrics []string
	IDs     []string
	PubKeys []string
	Kinds   []int
	Tags    []TagFilter
	Since   int64
	Until   int64
	Limit   uint64
	Shuffle ShuffleInput
	Empty   bool
}

type AggregateRow struct {
	Dimensions map[string]string `json:"dimensions"`
	Metrics    map[string]uint64 `json:"metrics"`
}

type ReferenceAggregateInput struct {
	Events         EventQueryInput
	Tag            TagFilter
	Targets        []string
	LimitPerTarget uint64
	GroupBy        []ReferenceAggregateDimension
	Metrics        []ReferenceAggregateMetric
	First          uint64
	OrderBy        string
}

type ReferenceAggregateDimension struct {
	Name     string
	Field    string
	TagKey   string
	TagIndex int
	Derived  string
}

type ReferenceAggregateMetric struct {
	Name          string
	Op            string
	Field         string
	TagKey        string
	TagIndex      int
	Derived       string
	DistinctField string
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

type PubkeyScore struct {
	PubKey    string    `json:"pubkey"`
	Source    string    `json:"source"`
	Score     float64   `json:"score"`
	Rank      float64   `json:"rank"`
	Followers uint64    `json:"followers"`
	Nodes     uint64    `json:"nodes"`
	FetchedAt time.Time `json:"fetchedAt"`
}

type TopicRow struct {
	Value     string `json:"value"`
	Parent    string `json:"parent"`
	Label     string `json:"label"`
	IsDefault bool   `json:"isDefault"`
	Count     uint64 `json:"count"`
}

type TrendingInput struct {
	Window   string
	Category string
	Limit    uint64
}

type TrendingClusterRow struct {
	ID          string    `json:"id"`
	Window      string    `json:"window"`
	StartedAt   time.Time `json:"startedAt"`
	Category    string    `json:"category"`
	Subcategory string    `json:"subcategory"`
	Title       string    `json:"title"`
	Description string    `json:"description"`
	EventCount  uint64    `json:"eventCount"`
	Score       float64   `json:"score"`
	ComputedAt  time.Time `json:"computedAt"`
}

type NotificationInput struct {
	Viewer string
	Tab    string
	Policy string
	Since  int64
	Until  int64
	Limit  uint64
}

type NotificationRow struct {
	Event            EventView `json:"event"`
	Reason           string    `json:"reason"`
	ActorVertexScore float64   `json:"actorVertexScore"`
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
	counts, err := s.BatchFollowCounts(ctx, []string{pubkey})
	if err != nil {
		return FollowCounts{}, err
	}
	return counts[pubkey], nil
}

func (s *Store) BatchFollowCounts(ctx context.Context, pubkeys []string) (map[string]FollowCounts, error) {
	pubkeys = uniqueStrings(pubkeys)
	out := make(map[string]FollowCounts, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = FollowCounts{}
	}
	if len(pubkeys) == 0 {
		return out, nil
	}
	latest, err := s.QueryLatestEventsByPubKeys(ctx, pubkeys, []int{3}, 1)
	if err != nil {
		return nil, err
	}
	for _, pubkey := range pubkeys {
		seenFollows := map[string]struct{}{}
		for _, event := range latest[pubkey] {
			for _, tag := range event.Tags {
				if len(tag) >= 2 && tag[0] == "p" && tag[1] != "" {
					seenFollows[tag[1]] = struct{}{}
				}
			}
		}
		counts := out[pubkey]
		counts.Follows = uint64(len(seenFollows))
		out[pubkey] = counts
	}

	rows, err := s.conn.Query(ctx, `
		SELECT tag_value, uniqExact(pubkey)
		FROM event_tags
		WHERE kind = 3 AND tag_key = 'p' AND tag_value IN (?)
		GROUP BY tag_value
	`, pubkeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pubkey string
		var followers uint64
		if err := rows.Scan(&pubkey, &followers); err != nil {
			return nil, err
		}
		counts := out[pubkey]
		counts.Followers = followers
		out[pubkey] = counts
	}
	return out, rows.Err()
}

func (s *Store) FollowerCount(ctx context.Context, pubkey string) (uint64, error) {
	counts, err := s.BatchFollowCounts(ctx, []string{pubkey})
	if err != nil {
		return 0, err
	}
	return counts[pubkey].Followers, nil
}

func (s *Store) RecentAuthorPubkeysByFollowers(ctx context.Context, minFollowers uint64, limit int) ([]string, error) {
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.conn.Query(ctx, `
		SELECT recent.pubkey
		FROM
		(
			SELECT pubkey, max(created_at) AS last_event_at
			FROM nostr_events FINAL
			WHERE length(pubkey) = 64 AND created_at >= now() - INTERVAL 30 DAY
			GROUP BY pubkey
		) AS recent
		INNER JOIN
		(
			SELECT tag_value AS pubkey, uniqExact(pubkey) AS followers
			FROM event_tags
			WHERE kind = 3 AND tag_key = 'p' AND length(tag_value) = 64
			GROUP BY tag_value
			HAVING followers >= ?
		) AS follower_counts ON follower_counts.pubkey = recent.pubkey
		LEFT JOIN
		(
			SELECT pubkey, max(fetched_at) AS fetched_at
			FROM vertex_scores FINAL
			WHERE source = 'vertex'
			GROUP BY pubkey
		) AS scores ON scores.pubkey = recent.pubkey
		WHERE ifNull(scores.fetched_at, toDateTime(0)) < now() - INTERVAL 6 HOUR
		ORDER BY recent.last_event_at DESC
		LIMIT ?
	`, minFollowers, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var pubkey string
		if err := rows.Scan(&pubkey); err != nil {
			return nil, err
		}
		out = append(out, pubkey)
	}
	return out, rows.Err()
}

func (s *Store) ProfileFirstEventCreatedAt(ctx context.Context, pubkey string) (*time.Time, error) {
	var first sql.NullTime
	if err := s.conn.QueryRow(ctx, `
		SELECT minOrNull(created_at)
		FROM nostr_events
		WHERE pubkey = ?
	`, pubkey).Scan(&first); err != nil {
		return nil, err
	}
	if !first.Valid {
		return nil, nil
	}
	firstTime := first.Time.UTC()
	return &firstTime, nil
}

func (s *Store) CachedVertexProfile(ctx context.Context, pubkey string) (vertex.ProfileResult, bool, error) {
	var payload string
	if err := s.conn.QueryRow(ctx, `
		SELECT payload
		FROM vertex_profile_cache FINAL
		WHERE pubkey = ?
	`, pubkey).Scan(&payload); err != nil {
		if err == sql.ErrNoRows {
			return vertex.ProfileResult{}, false, nil
		}
		return vertex.ProfileResult{}, false, err
	}
	var profile vertex.ProfileResult
	if err := json.Unmarshal([]byte(payload), &profile); err != nil {
		return vertex.ProfileResult{}, false, err
	}
	if profile.PubKey == "" {
		profile.PubKey = pubkey
	}
	if profile.Npub == "" {
		profile.Npub = vertex.Npub(profile.PubKey)
	}
	return profile, true, nil
}

func (s *Store) SaveVertexProfile(ctx context.Context, profile vertex.ProfileResult) error {
	pubkey, ok := vertex.NormalizePubkey(profile.PubKey)
	if !ok {
		return fmt.Errorf("invalid vertex profile pubkey")
	}
	profile.PubKey = pubkey
	if profile.Npub == "" {
		profile.Npub = vertex.Npub(pubkey)
	}
	payload, err := json.Marshal(profile)
	if err != nil {
		return err
	}
	fetchedAt := time.Now().UTC()
	if err := s.conn.Exec(ctx, `
		INSERT INTO vertex_profile_cache (pubkey, fetched_at, payload)
		VALUES (?, ?, ?)
	`, pubkey, fetchedAt, string(payload)); err != nil {
		return err
	}
	if profile.Score == nil {
		return nil
	}
	var followers uint64
	if profile.Followers != nil {
		followers = *profile.Followers
	}
	var nodes uint64
	if profile.Nodes != nil && *profile.Nodes > 0 {
		nodes = uint64(*profile.Nodes)
	}
	return s.conn.Exec(ctx, `
		INSERT INTO vertex_scores (source, pubkey, score, rank, followers, nodes, fetched_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, "vertex", pubkey, *profile.Score, profile.Rank, followers, nodes, fetchedAt)
}

func (s *Store) AuthorVertexScores(ctx context.Context, pubkeys []string) (map[string]PubkeyScore, error) {
	return s.PubkeyScores(ctx, "vertex", pubkeys)
}

func (s *Store) PubkeyScores(ctx context.Context, source string, pubkeys []string) (map[string]PubkeyScore, error) {
	source = strings.TrimSpace(strings.ToLower(source))
	if source == "" {
		source = "vertex"
	}
	pubkeys = uniqueStrings(pubkeys)
	out := make(map[string]PubkeyScore, len(pubkeys))
	if len(pubkeys) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT source, pubkey, score, rank, followers, nodes, fetched_at
		FROM vertex_scores FINAL
		WHERE source = ? AND pubkey IN (?)
	`, source, pubkeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var score PubkeyScore
		if err := rows.Scan(
			&score.Source,
			&score.PubKey,
			&score.Score,
			&score.Rank,
			&score.Followers,
			&score.Nodes,
			&score.FetchedAt,
		); err != nil {
			return nil, err
		}
		out[score.PubKey] = score
	}
	return out, rows.Err()
}

func (s *Store) DerivedMetricValues(ctx context.Context, metric string, eventIDs []string) (map[string]float64, error) {
	metric = strings.TrimSpace(metric)
	eventIDs = uniqueStrings(eventIDs)
	out := make(map[string]float64, len(eventIDs))
	if metric == "" || len(eventIDs) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT event_id, argMax(value, computed_at)
		FROM derived_metrics FINAL
		WHERE metric = ? AND event_id IN (?)
		GROUP BY event_id
	`, metric, eventIDs)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var eventID string
		var value float64
		if err := rows.Scan(&eventID, &value); err != nil {
			return nil, err
		}
		out[eventID] = value
	}
	return out, rows.Err()
}

func (s *Store) AvailableTopics(ctx context.Context, input EventQueryInput) ([]TopicRow, error) {
	if input.Empty {
		return []TopicRow{}, nil
	}
	if input.Limit == 0 || input.Limit > 500 {
		input.Limit = 100
	}
	where, args := eventWhere("e", input.IDs, input.PubKeys, input.Kinds, input.Tags)
	if input.Since > 0 {
		where += " AND e.created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		where += " AND e.created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}
	args = append(args, input.Limit)
	rows, err := s.conn.Query(ctx, `
		SELECT
			dt.tag_value,
			anyLast(ifNull(tt.parent, '')),
			anyLast(ifNull(tt.label, dt.tag_value)),
			max(ifNull(tt.is_default, 0)),
			uniqExact(dt.event_id) AS event_count
		FROM derived_tags AS dt
		INNER JOIN nostr_events AS e FINAL ON e.id = dt.event_id
		LEFT JOIN topic_taxonomy AS tt FINAL ON tt.value = dt.tag_value
		`+where+`
		  AND dt.tag_key = 'topic'
		GROUP BY dt.tag_value
		ORDER BY event_count DESC, dt.tag_value ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []TopicRow
	for rows.Next() {
		var row TopicRow
		var isDefault uint8
		if err := rows.Scan(&row.Value, &row.Parent, &row.Label, &isDefault, &row.Count); err != nil {
			return nil, err
		}
		row.IsDefault = isDefault != 0
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) TrendingClusters(ctx context.Context, input TrendingInput) ([]TrendingClusterRow, error) {
	window := strings.TrimSpace(strings.ToUpper(input.Window))
	if window == "" {
		window = "H24"
	}
	if input.Limit == 0 || input.Limit > 50 {
		input.Limit = 20
	}

	where := "WHERE window = ?"
	args := []any{window}
	if category := strings.TrimSpace(input.Category); category != "" {
		where += " AND category = ?"
		args = append(args, category)
	}
	args = append(args, input.Limit)
	rows, err := s.conn.Query(ctx, `
		SELECT id, window, started_at, category, subcategory, title, description, event_count, score, computed_at
		FROM trending_clusters FINAL
		`+where+`
		ORDER BY score DESC, event_count DESC, started_at DESC, id ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []TrendingClusterRow
	for rows.Next() {
		var row TrendingClusterRow
		if err := rows.Scan(
			&row.ID,
			&row.Window,
			&row.StartedAt,
			&row.Category,
			&row.Subcategory,
			&row.Title,
			&row.Description,
			&row.EventCount,
			&row.Score,
			&row.ComputedAt,
		); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

func (s *Store) Notifications(ctx context.Context, input NotificationInput) ([]NotificationRow, error) {
	input.Viewer = strings.TrimSpace(strings.ToLower(input.Viewer))
	if input.Viewer == "" {
		return []NotificationRow{}, nil
	}
	if input.Limit == 0 || input.Limit > 100 {
		input.Limit = 50
	}
	tab := strings.ToUpper(strings.TrimSpace(input.Tab))
	if tab == "" {
		tab = "ALL"
	}
	policy := strings.ToUpper(strings.TrimSpace(input.Policy))
	if policy == "" {
		policy = "STRICT"
	}

	where := "WHERE n.viewer = ?"
	args := []any{input.Viewer}
	if tab == "MENTIONS" {
		where += " AND n.reason = 'mention'"
	}
	if input.Since > 0 {
		where += " AND n.created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		where += " AND n.created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}
	actorThreshold, viewerThreshold := notificationPolicyThresholds(policy)
	if actorThreshold > 0 || viewerThreshold > 0 {
		where += " AND (ifNull(actor_score.score, 0) >= ? OR ifNull(viewer_score.score, 0) >= ?)"
		args = append(args, actorThreshold, viewerThreshold)
	}
	args = append(args, input.Limit)

	rows, err := s.conn.Query(ctx, `
		SELECT
			e.id,
			e.pubkey,
			e.kind,
			e.created_at,
			e.content,
			e.tags_json,
			e.sig,
			e.last_seen_at,
			n.reason,
			ifNull(actor_score.score, 0) AS actor_vertex_score
		FROM notification_candidates AS n FINAL
		INNER JOIN nostr_events AS e FINAL ON e.id = n.event_id
		LEFT JOIN vertex_scores AS actor_score FINAL
			ON actor_score.source = 'vertex' AND actor_score.pubkey = n.actor_pubkey
		LEFT JOIN vertex_scores AS viewer_score FINAL
			ON viewer_score.source = 'vertex' AND viewer_score.pubkey = n.viewer
		`+where+`
		ORDER BY n.created_at DESC, n.event_id DESC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []NotificationRow{}
	for rows.Next() {
		var row NotificationRow
		var tagsJSON string
		var kind uint32
		if err := rows.Scan(
			&row.Event.ID,
			&row.Event.PubKey,
			&kind,
			&row.Event.CreatedAt,
			&row.Event.Content,
			&tagsJSON,
			&row.Event.Sig,
			&row.Event.UpdatedAt,
			&row.Reason,
			&row.ActorVertexScore,
		); err != nil {
			return nil, err
		}
		row.Event.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &row.Event.Tags)
		out = append(out, row)
	}
	return out, rows.Err()
}

func notificationPolicyThresholds(policy string) (float64, float64) {
	switch strings.ToUpper(strings.TrimSpace(policy)) {
	case "RELAXED":
		return 0, 0
	case "MODERATE":
		return 20, 60
	default:
		return 50, 80
	}
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
	if input.Empty {
		return []EventView{}, nil
	}
	if input.Limit == 0 || input.Limit > 500 {
		input.Limit = 50
	}
	if len(input.IDs) > 0 {
		return s.queryEventsByIDFilter(ctx, input)
	}

	where, args := eventWhere("e", input.IDs, input.PubKeys, input.Kinds, input.Tags)
	if input.Since > 0 {
		where += " AND e.created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		where += " AND e.created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}
	orderBy, orderArgs := eventOrderBy("e.created_at", "e.id", input.Shuffle)
	args = append(args, orderArgs...)
	args = append(args, input.Limit)
	query := `
		SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
		FROM nostr_events AS e FINAL
		` + where + `
		` + orderBy + `
		LIMIT ?
	`
	if input.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, input.Offset)
	}
	rows, err := s.conn.Query(ctx, query, args...)
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

func (s *Store) queryEventsByIDFilter(ctx context.Context, input EventQueryInput) ([]EventView, error) {
	where, args := eventWhere("e", input.IDs, input.PubKeys, input.Kinds, input.Tags)
	if input.Since > 0 {
		where += " AND e.created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		where += " AND e.created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}
	orderBy, orderArgs := eventOrderBy("created_at", "id", input.Shuffle)
	args = append(args, orderArgs...)
	args = append(args, input.Limit)
	query := `
		SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
		FROM (
			SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
			FROM nostr_events AS e
			` + where + `
			ORDER BY e.id ASC, e.last_seen_at DESC
			LIMIT 1 BY e.id
		)
		` + orderBy + `
		LIMIT ?
	`
	if input.Offset > 0 {
		query += " OFFSET ?"
		args = append(args, input.Offset)
	}
	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEventRows(rows)
}

func eventOrderBy(createdAtColumn, idColumn string, shuffle ShuffleInput) (string, []any) {
	if !shuffleEnabled(shuffle) {
		return fmt.Sprintf("ORDER BY %s DESC, %s DESC", createdAtColumn, idColumn), nil
	}
	return fmt.Sprintf(
		"ORDER BY %s DESC, cityHash64(concat(%s, ?, toString(?))) DESC, %s DESC",
		createdAtColumn,
		idColumn,
		idColumn,
	), []any{strings.TrimSpace(shuffle.Seed), shuffle.Counter}
}

func aggregateOrderBy(spec aggSpec, shuffle ShuffleInput) (string, []any) {
	base := spec.orderMetric + " DESC"
	if !shuffleEnabled(shuffle) {
		return base, nil
	}
	parts := make([]string, 0, len(spec.groupDims)+2)
	for _, dim := range spec.groupDims {
		parts = append(parts, "toString("+dim+")")
	}
	parts = append(parts, "?", "toString(?)")
	return base + ", cityHash64(concat(" + strings.Join(parts, ", ") + ")) DESC", []any{strings.TrimSpace(shuffle.Seed), shuffle.Counter}
}

func shuffleEnabled(shuffle ShuffleInput) bool {
	return strings.TrimSpace(shuffle.Seed) != "" && shuffle.Strength > 0
}

func (s *Store) QueryEventsByTagTargets(ctx context.Context, input EventQueryInput, tag TagFilter, targets []string, limitPerTarget uint64) (map[string][]EventView, error) {
	targets = uniqueStrings(targets)
	sort.Strings(targets)
	out := make(map[string][]EventView, len(targets))
	for _, target := range targets {
		out[target] = nil
	}
	if input.Empty || tag.Key == "" || len(targets) == 0 {
		return out, nil
	}
	if tag.Value != "" {
		targets = []string{tag.Value}
	} else if len(tag.Values) > 0 {
		targets = intersectStrings(targets, tag.Values)
	}
	if len(targets) == 0 {
		return out, nil
	}
	if limitPerTarget == 0 || limitPerTarget > 500 {
		limitPerTarget = 50
	}
	if len(input.Tags) == 0 {
		return s.queryEventsByTagTargetsFromTags(ctx, out, input, tag, targets, limitPerTarget)
	}
	queryLimit := limitPerTarget + input.Offset
	if queryLimit == 0 || queryLimit > 1000 {
		queryLimit = limitPerTarget
	}

	where, args := eventWhere("e", input.IDs, input.PubKeys, input.Kinds, input.Tags)
	if input.Since > 0 {
		where += " AND e.created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		where += " AND e.created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}
	where += " AND rt.tag_key = ? AND rt.tag_value IN (?)"
	args = append(args, tag.Key, targets)

	innerOrderBy, innerOrderArgs := eventOrderBy("e.created_at", "e.id", input.Shuffle)
	outerOrderBy, outerOrderArgs := eventOrderBy("created_at", "id", input.Shuffle)
	args = append(args, innerOrderArgs...)
	args = append(args, outerOrderArgs...)
	query := fmt.Sprintf(`
		SELECT target_value, id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
		FROM (
			SELECT
				rt.tag_value AS target_value,
				e.id AS id,
				e.pubkey AS pubkey,
				e.kind AS kind,
				e.created_at AS created_at,
				e.content AS content,
				e.tags_json AS tags_json,
				e.sig AS sig,
				e.last_seen_at AS last_seen_at
			FROM event_tags AS rt
			INNER JOIN nostr_events AS e FINAL ON e.id = rt.event_id
			%s
			ORDER BY rt.tag_value ASC, %s
			LIMIT %d BY rt.tag_value
		)
		ORDER BY target_value ASC, %s
	`, where, strings.TrimPrefix(innerOrderBy, "ORDER BY "), queryLimit, strings.TrimPrefix(outerOrderBy, "ORDER BY "))

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	seenByTarget := make(map[string]map[string]struct{}, len(targets))
	for rows.Next() {
		var target string
		var tagsJSON string
		var ev EventView
		var kind uint32
		if err := rows.Scan(&target, &ev.ID, &ev.PubKey, &kind, &ev.CreatedAt, &ev.Content, &tagsJSON, &ev.Sig, &ev.UpdatedAt); err != nil {
			return nil, err
		}
		if seenByTarget[target] == nil {
			seenByTarget[target] = map[string]struct{}{}
		}
		if _, ok := seenByTarget[target][ev.ID]; ok {
			continue
		}
		seenByTarget[target][ev.ID] = struct{}{}
		ev.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &ev.Tags)
		out[target] = append(out[target], ev)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if input.Offset == 0 && limitPerTarget == queryLimit {
		return out, nil
	}
	for target, events := range out {
		if input.Offset > 0 {
			if input.Offset >= uint64(len(events)) {
				out[target] = nil
				continue
			}
			events = events[input.Offset:]
		}
		if uint64(len(events)) > limitPerTarget {
			events = events[:limitPerTarget]
		}
		out[target] = events
	}
	return out, nil
}

func (s *Store) queryEventsByTagTargetsFromTags(ctx context.Context, out map[string][]EventView, input EventQueryInput, tag TagFilter, targets []string, limitPerTarget uint64) (map[string][]EventView, error) {
	queryLimit := limitPerTarget + input.Offset
	if queryLimit == 0 || queryLimit > 1000 {
		queryLimit = limitPerTarget
	}

	clauses := []string{"WHERE rt.tag_key = ?", "rt.tag_value IN (?)"}
	args := []any{tag.Key, targets}
	if len(input.IDs) > 0 {
		clauses = append(clauses, "rt.event_id IN (?)")
		args = append(args, input.IDs)
	}
	if len(input.PubKeys) > 0 {
		clauses = append(clauses, "rt.pubkey IN (?)")
		args = append(args, input.PubKeys)
	}
	if len(input.Kinds) > 0 {
		clauses = append(clauses, "rt.kind IN ("+ints(input.Kinds)+")")
	}
	if input.Since > 0 {
		clauses = append(clauses, "rt.created_at >= ?")
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		clauses = append(clauses, "rt.created_at < ?")
		args = append(args, time.Unix(input.Until, 0).UTC())
	}

	innerOrderBy, innerOrderArgs := eventOrderBy("rt.created_at", "rt.event_id", input.Shuffle)
	outerOrderBy, outerOrderArgs := eventOrderBy("created_at", "event_id", input.Shuffle)
	args = append(args, innerOrderArgs...)
	args = append(args, outerOrderArgs...)
	query := fmt.Sprintf(`
		SELECT target_value, event_id, created_at
		FROM (
			SELECT
				rt.tag_value AS target_value,
				rt.event_id AS event_id,
				rt.created_at AS created_at
			FROM event_tags AS rt
			%s
			ORDER BY rt.tag_value ASC, %s
			LIMIT %d BY rt.tag_value
		)
		ORDER BY target_value ASC, %s
	`, strings.Join(clauses, " AND "), strings.TrimPrefix(innerOrderBy, "ORDER BY "), queryLimit, strings.TrimPrefix(outerOrderBy, "ORDER BY "))

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	idsByTarget := make(map[string][]string, len(targets))
	allIDs := make([]string, 0)
	seenByTarget := make(map[string]map[string]struct{}, len(targets))
	seenAll := map[string]struct{}{}
	for rows.Next() {
		var target string
		var id string
		var createdAt time.Time
		if err := rows.Scan(&target, &id, &createdAt); err != nil {
			return nil, err
		}
		if seenByTarget[target] == nil {
			seenByTarget[target] = map[string]struct{}{}
		}
		if _, ok := seenByTarget[target][id]; ok {
			continue
		}
		seenByTarget[target][id] = struct{}{}
		idsByTarget[target] = append(idsByTarget[target], id)
		if _, ok := seenAll[id]; ok {
			continue
		}
		seenAll[id] = struct{}{}
		allIDs = append(allIDs, id)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(allIDs) == 0 {
		return out, nil
	}

	events, err := s.QueryEvents(ctx, EventQueryInput{IDs: allIDs, Limit: uint64(len(allIDs))})
	if err != nil {
		return nil, err
	}
	eventsByID := make(map[string]EventView, len(events))
	for _, event := range events {
		eventsByID[event.ID] = event
	}
	for target, ids := range idsByTarget {
		if input.Offset > 0 {
			if input.Offset >= uint64(len(ids)) {
				out[target] = nil
				continue
			}
			ids = ids[input.Offset:]
		}
		if uint64(len(ids)) > limitPerTarget {
			ids = ids[:limitPerTarget]
		}
		targetEvents := make([]EventView, 0, len(ids))
		for _, id := range ids {
			event, ok := eventsByID[id]
			if !ok {
				continue
			}
			targetEvents = append(targetEvents, event)
		}
		out[target] = targetEvents
	}
	return out, nil
}

func (s *Store) AggregateEventsByTagTargets(ctx context.Context, input ReferenceAggregateInput) (map[string][]AggregateRow, bool, error) {
	targets := uniqueStrings(input.Targets)
	sort.Strings(targets)
	out := make(map[string][]AggregateRow, len(targets))
	for _, target := range targets {
		out[target] = nil
	}
	if input.Events.Empty || input.Tag.Key == "" || len(targets) == 0 {
		return out, true, nil
	}
	if input.Tag.Value != "" {
		targets = []string{input.Tag.Value}
	} else if len(input.Tag.Values) > 0 {
		targets = intersectStrings(targets, input.Tag.Values)
	}
	if len(targets) == 0 {
		return out, true, nil
	}
	if input.LimitPerTarget == 0 || input.LimitPerTarget > 500 {
		input.LimitPerTarget = 50
	}
	if input.Events.Offset > 0 {
		return nil, false, nil
	}
	queryLimit := input.LimitPerTarget + input.Events.Offset
	if queryLimit == 0 || queryLimit > 1000 {
		queryLimit = input.LimitPerTarget
	}
	if len(input.Metrics) == 0 {
		input.Metrics = []ReferenceAggregateMetric{{Name: "count", Op: "COUNT"}}
	}

	spec, ok := referenceAggregateSpec(input)
	if !ok {
		return nil, false, nil
	}

	where, args := eventWhere("e", input.Events.IDs, input.Events.PubKeys, input.Events.Kinds, input.Events.Tags)
	if input.Events.Since > 0 {
		where += " AND e.created_at >= ?"
		args = append(args, time.Unix(input.Events.Since, 0).UTC())
	}
	if input.Events.Until > 0 {
		where += " AND e.created_at < ?"
		args = append(args, time.Unix(input.Events.Until, 0).UTC())
	}
	where += " AND rt.tag_key = ? AND rt.tag_value IN (?)"
	args = append(args, input.Tag.Key, targets)

	selectParts := append([]string{"target_value"}, spec.selectDims...)
	selectParts = append(selectParts, spec.selectMetrics...)
	groupParts := append([]string{"target_value"}, spec.groupDims...)
	query := fmt.Sprintf(`
		SELECT *
		FROM (
			SELECT %s
			FROM (
				SELECT DISTINCT
					rt.tag_value AS target_value,
					e.id AS id,
					e.pubkey AS pubkey,
					e.kind AS kind,
					e.created_at AS created_at,
					e.content AS content
				FROM event_tags AS rt
				INNER JOIN nostr_events AS e FINAL ON e.id = rt.event_id
				%s
				ORDER BY rt.tag_value ASC, e.created_at DESC, e.id DESC
				LIMIT %d BY rt.tag_value
			)
			GROUP BY %s
		)
		ORDER BY target_value ASC, %s DESC
	`, strings.Join(selectParts, ", "), where, queryLimit, strings.Join(groupParts, ", "), spec.orderMetric)
	if input.First > 0 {
		query += fmt.Sprintf(" LIMIT %d BY target_value", input.First)
	}

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, true, err
	}
	defer rows.Close()

	for rows.Next() {
		var target string
		values := make([]any, 0, 1+len(spec.scanDims)+len(spec.scanMetrics))
		dimValues := make([]string, len(spec.scanDims))
		metricValues := make([]uint64, len(spec.scanMetrics))
		values = append(values, &target)
		for i := range dimValues {
			values = append(values, &dimValues[i])
		}
		for i := range metricValues {
			values = append(values, &metricValues[i])
		}
		if err := rows.Scan(values...); err != nil {
			return nil, true, err
		}

		row := AggregateRow{Dimensions: map[string]string{}, Metrics: map[string]uint64{}}
		for i, key := range spec.scanDims {
			row.Dimensions[key] = dimValues[i]
		}
		for i, key := range spec.scanMetrics {
			row.Metrics[key] = metricValues[i]
		}
		out[target] = append(out[target], row)
	}
	return out, true, rows.Err()
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
	if input.Empty {
		return []AggregateRow{}, nil
	}
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

	orderBy, orderArgs := aggregateOrderBy(spec, input.Shuffle)
	args = append(args, orderArgs...)
	query := fmt.Sprintf("SELECT %s, %s FROM %s %s GROUP BY %s ORDER BY %s LIMIT %d",
		strings.Join(spec.selectDims, ", "),
		strings.Join(spec.selectMetrics, ", "),
		spec.from,
		spec.where,
		strings.Join(spec.groupDims, ", "),
		orderBy,
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

func (s *Store) AggregateEventReferencesToTargets(ctx context.Context, references AggregateInput, target EventQueryInput) ([]AggregateRow, error) {
	if references.Empty || target.Empty {
		return []AggregateRow{}, nil
	}
	if references.Limit == 0 || references.Limit > 1000 {
		references.Limit = 100
	}
	if len(references.Metrics) == 0 {
		references.Metrics = []string{"COUNT"}
	}
	if dataset := strings.ToUpper(references.Dataset); dataset != "" && dataset != "TAGS" {
		return nil, fmt.Errorf("target-aware reference aggregation requires TAGS dataset")
	}
	references.Dataset = "TAGS"

	spec, args, err := aggregateSpec(references)
	if err != nil {
		return nil, err
	}
	spec.from = "event_tags t INNER JOIN nostr_events AS e FINAL ON e.id = t.event_id INNER JOIN nostr_events AS target FINAL ON target.id = t.tag_value"

	targetWhere, targetArgs := eventWhere("target", target.IDs, target.PubKeys, target.Kinds, target.Tags)
	spec.where += " AND " + whereBody(targetWhere)
	args = append(args, targetArgs...)
	if target.Since > 0 {
		spec.where += " AND target.created_at >= ?"
		args = append(args, time.Unix(target.Since, 0).UTC())
	}
	if target.Until > 0 {
		spec.where += " AND target.created_at < ?"
		args = append(args, time.Unix(target.Until, 0).UTC())
	}

	orderBy, orderArgs := aggregateOrderBy(spec, references.Shuffle)
	args = append(args, orderArgs...)
	query := fmt.Sprintf("SELECT %s, %s FROM %s %s GROUP BY %s ORDER BY %s LIMIT %d",
		strings.Join(spec.selectDims, ", "),
		strings.Join(spec.selectMetrics, ", "),
		spec.from,
		spec.where,
		strings.Join(spec.groupDims, ", "),
		orderBy,
		references.Limit,
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
	case "TAGS", "DERIVED_TAGS":
		spec.from = tagDatasetTable(dataset) + " t INNER JOIN nostr_events AS e FINAL ON e.id = t.event_id"
		spec.where, args = tagWhere(dataset, input.IDs, input.PubKeys, input.Kinds, input.Tags)
	case "RELAYS":
		spec.from = "event_seen_relays r"
		spec.where = "WHERE 1 = 1"
	default:
		return spec, nil, fmt.Errorf("unsupported dataset %q", input.Dataset)
	}
	timeColumn, err := aggregateTimeColumn(dataset)
	if err != nil {
		return spec, nil, err
	}
	if input.Since > 0 {
		spec.where += " AND " + timeColumn + " >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		spec.where += " AND " + timeColumn + " < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
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

func referenceAggregateSpec(input ReferenceAggregateInput) (aggSpec, bool) {
	spec := aggSpec{}
	for i, dim := range input.GroupBy {
		expr, ok := referenceSelectorStringExpr(ReferenceAggregateMetric{
			Field:    dim.Field,
			TagKey:   dim.TagKey,
			TagIndex: dim.TagIndex,
			Derived:  dim.Derived,
		}, false)
		if !ok || dim.Name == "" {
			return spec, false
		}
		alias := fmt.Sprintf("d%d", i)
		spec.selectDims = append(spec.selectDims, fmt.Sprintf("toString(%s) AS %s", expr, alias))
		spec.groupDims = append(spec.groupDims, alias)
		spec.scanDims = append(spec.scanDims, dim.Name)
	}

	matchedOrder := input.OrderBy == ""
	for i, metric := range input.Metrics {
		if metric.Name == "" {
			return spec, false
		}
		expr, ok := referenceMetricExpr(metric)
		if !ok {
			return spec, false
		}
		alias := fmt.Sprintf("m%d", i)
		spec.selectMetrics = append(spec.selectMetrics, fmt.Sprintf("%s AS %s", expr, alias))
		spec.scanMetrics = append(spec.scanMetrics, metric.Name)
		if spec.orderMetric == "" || input.OrderBy == metric.Name {
			spec.orderMetric = alias
			if input.OrderBy == metric.Name {
				matchedOrder = true
			}
		}
	}
	if !matchedOrder {
		return spec, false
	}
	if spec.orderMetric == "" && len(spec.selectMetrics) > 0 {
		spec.orderMetric = "m0"
	}
	if spec.orderMetric == "" {
		return spec, false
	}
	return spec, true
}

func referenceMetricExpr(metric ReferenceAggregateMetric) (string, bool) {
	switch strings.ToUpper(metric.Op) {
	case "", "COUNT":
		return "count()", true
	case "COUNT_DISTINCT":
		selector := ReferenceAggregateMetric{
			Field: metric.DistinctField,
		}
		if selector.Field == "" {
			selector.Field = metric.Field
		}
		if selector.Field == "" && metric.Derived == "" && metric.TagKey == "" {
			selector.Field = "PUBKEY"
		}
		selector.TagKey = metric.TagKey
		selector.TagIndex = metric.TagIndex
		selector.Derived = metric.Derived
		expr, ok := referenceSelectorStringExpr(selector, true)
		if !ok {
			return "", false
		}
		return "uniqExact(" + expr + ")", true
	case "SUM":
		expr, ok := referenceSelectorUintExpr(metric)
		if !ok {
			return "", false
		}
		return "sum(" + expr + ")", true
	case "AVG":
		expr, ok := referenceSelectorUintExpr(metric)
		if !ok {
			return "", false
		}
		return "toUInt64(avg(" + expr + "))", true
	case "MIN":
		expr, ok := referenceSelectorUintExpr(metric)
		if !ok {
			return "", false
		}
		return "min(" + expr + ")", true
	case "MAX":
		expr, ok := referenceSelectorUintExpr(metric)
		if !ok {
			return "", false
		}
		return "max(" + expr + ")", true
	default:
		return "", false
	}
}

func referenceSelectorUintExpr(metric ReferenceAggregateMetric) (string, bool) {
	expr, ok := referenceSelectorStringExpr(metric, false)
	if !ok {
		return "", false
	}
	switch strings.ToUpper(metric.Field) {
	case "KIND":
		return "toUInt64(kind)", true
	case "CREATED_AT":
		return "toUInt64(toUnixTimestamp(created_at))", true
	default:
		return "toUInt64OrZero(" + expr + ")", true
	}
}

func referenceSelectorStringExpr(metric ReferenceAggregateMetric, defaultPubkey bool) (string, bool) {
	if metric.Derived != "" || metric.TagKey != "" {
		return "", false
	}
	if metric.Field == "" && !defaultPubkey {
		return "", false
	}
	switch strings.ToUpper(metric.Field) {
	case "", "PUBKEY", "AUTHOR":
		return "pubkey", true
	case "ID", "EVENT_ID":
		return "id", true
	case "KIND":
		return "toString(kind)", true
	case "CREATED_AT":
		return "toString(toUnixTimestamp(created_at))", true
	case "CONTENT":
		return "content", true
	default:
		return "", false
	}
}

func aggregateTimeColumn(dataset string) (string, error) {
	switch dataset {
	case "EVENTS":
		return "e.created_at", nil
	case "TAGS", "DERIVED_TAGS":
		return "t.created_at", nil
	case "RELAYS":
		return "r.last_seen_at", nil
	default:
		return "", fmt.Errorf("unsupported dataset %q", dataset)
	}
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
		table := tagDatasetTable(tag.Dataset)
		hasIncludeFilter := tag.Value != "" || len(tag.Values) > 0 || len(tag.ExcludeValues) == 0
		if tag.Key != "" && hasIncludeFilter {
			clause := fmt.Sprintf("%s.id IN (SELECT %s.event_id FROM %s %s WHERE %s.tag_key = ?", alias, subAlias, table, subAlias, subAlias)
			args = append(args, tag.Key)
			clause, args = addTagValueClause(clause, args, subAlias, tag)
			clauses = append(clauses, clause+")")
		}
		if tag.Key != "" && len(tag.ExcludeValues) > 0 {
			excludeAlias := fmt.Sprintf("%sx", subAlias)
			clause := fmt.Sprintf("%s.id NOT IN (SELECT %s.event_id FROM %s %s WHERE %s.tag_key = ? AND %s.tag_value IN (?))", alias, excludeAlias, table, excludeAlias, excludeAlias, excludeAlias)
			args = append(args, tag.Key, tag.ExcludeValues)
			clauses = append(clauses, clause)
		}
	}
	return strings.Join(clauses, " AND "), args
}

func whereBody(where string) string {
	return strings.TrimPrefix(where, "WHERE ")
}

func tagWhere(dataset string, ids, pubkeys []string, kinds []int, tags []TagFilter) (string, []any) {
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
		if strings.ToUpper(strings.TrimSpace(tag.Dataset)) == "DERIVED_TAGS" && dataset != "DERIVED_TAGS" {
			continue
		}
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

func tagDatasetTable(dataset string) string {
	switch strings.ToUpper(strings.TrimSpace(dataset)) {
	case "DERIVED_TAGS":
		return "derived_tags"
	default:
		return "event_tags"
	}
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
		if dataset == "TAGS" || dataset == "DERIVED_TAGS" {
			return "t.event_id", nil
		}
		if dataset == "RELAYS" {
			return "r.event_id", nil
		}
		return "e.id", nil
	case "CONTENT":
		if dataset == "EVENTS" || dataset == "TAGS" || dataset == "DERIVED_TAGS" {
			return "e.content", nil
		}
	case "TAG_KEY":
		if dataset == "TAGS" || dataset == "DERIVED_TAGS" {
			return "t.tag_key", nil
		}
	case "TAG_VALUE":
		if dataset == "TAGS" || dataset == "DERIVED_TAGS" {
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
		if dataset == "TAGS" || dataset == "DERIVED_TAGS" {
			return "uniqExact(t.event_id)", nil
		}
		if dataset == "RELAYS" {
			return "uniqExact(r.event_id)", nil
		}
		return "uniqExact(e.id)", nil
	case "UNIQUE_PUBKEYS", "UNIQUE_AUTHORS":
		if dataset == "TAGS" || dataset == "DERIVED_TAGS" {
			return "uniqExact(t.pubkey)", nil
		}
		if dataset == "EVENTS" {
			return "uniqExact(e.pubkey)", nil
		}
	case "UNIQUE_TAG_VALUES":
		if dataset == "TAGS" || dataset == "DERIVED_TAGS" {
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
	case "TAGS", "DERIVED_TAGS":
		return fn + "(t.created_at)", nil
	case "RELAYS":
		return fn + "(r.last_seen_at)", nil
	default:
		return fn + "(e.created_at)", nil
	}
}

func eventOrTagExpr(dataset, col string) (string, error) {
	if dataset == "TAGS" || dataset == "DERIVED_TAGS" {
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

func intersectStrings(left, right []string) []string {
	allowed := make(map[string]struct{}, len(right))
	for _, value := range right {
		if value != "" {
			allowed[value] = struct{}{}
		}
	}
	out := make([]string, 0, min(len(left), len(right)))
	for _, value := range left {
		if _, ok := allowed[value]; ok {
			out = append(out, value)
		}
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
