package clickhouse

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/vertex-lab/nagg/internal/rules"
	"github.com/vertex-lab/nagg/internal/vertex"
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
	IDs            []string
	PubKeys        []string
	ExcludeIDs     []string
	ExcludePubKeys []string
	Kinds          []int
	Tags           []TagFilter
	Search         string
	Since          int64
	Until          int64
	Limit          uint64
	Offset         uint64
	Shuffle        ShuffleInput
	PubkeyScore    PubkeyScoreFilter
	Empty          bool
}

type ShuffleInput struct {
	Seed     string
	Counter  int
	Strength float64
}

type AggregateInput struct {
	Dataset     string
	GroupBy     []string
	Metrics     []string
	IDs         []string
	PubKeys     []string
	Kinds       []int
	Tags        []TagFilter
	Since       int64
	Until       int64
	Limit       uint64
	Shuffle     ShuffleInput
	PubkeyScore PubkeyScoreFilter
	Empty       bool
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

type PubkeyScoreFilter struct {
	Source       string
	MinFollowers uint64
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

type PubkeyStats struct {
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

type VertexSearchCacheEntry struct {
	Rows      []vertex.SearchResult
	FetchedAt time.Time
}

type ViewerFeedInput struct {
	Viewer     string
	Tab        string
	Policy     string
	ReplyScope string
	Since      int64
	Until      int64
	Limit      uint64
	// Kinds / ExcludeKinds filter the candidate window by the triggering
	// event's kind. The app-view grouping layer uses them to fetch kind-3
	// references and everything else on separate windows so a flood of kind-3
	// candidates can't crowd everything else out of the page.
	Kinds        []int64
	ExcludeKinds []int64
}

type ViewerFeedRow struct {
	Event EventView `json:"event"`
	// Kind is the triggering event's kind (3, 1, 6/16, 7, 9735); the client
	// derives any display label from it plus the event's own tags.
	Kind             int     `json:"kind"`
	ActorVertexScore float64 `json:"actorVertexScore"`
	// ActorPubKey is the follower/reposter/reactor/zapper. RefCreatedAt
	// is the candidate's recency (not the event's created_at, which can differ
	// for re-broadcast follows). Both feed the app-view grouping layer; the
	// generic GraphQL path ignores them.
	ActorPubKey  string    `json:"actorPubkey"`
	RefCreatedAt time.Time `json:"notificationCreatedAt"`
}

type K0Row struct {
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

type ProfileSearchRow struct {
	Profile K0Row   `json:"profile"`
	Rank    float64 `json:"rank"`
	Score   float64 `json:"score"`
}

func (s *Store) QueryLatestEventsByPubKeys(ctx context.Context, pubkeys []string, kinds []int, limitPerPubKey uint64) (map[string][]EventView, error) {
	pubkeys = uniqueStrings(pubkeys)
	if len(pubkeys) == 0 {
		return map[string][]EventView{}, nil
	}
	if limitPerPubKey == 0 || limitPerPubKey > 20 {
		limitPerPubKey = 1
	}

	// Two-step read. nostr_events is sorted (kind, created_at, pubkey, id), so a
	// per-pubkey latest lookup cannot use the primary key past `kind` — the old
	// single-query form dragged content/tags_json for the WHOLE kind slice
	// through FINAL + sort, costing 6-7.3 GiB per lookup on kind 3 (the standing
	// ClickHouse OOM trigger; see system.query_log exception history). Resolving
	// ids first touches only narrow sort columns; the full rows are then fetched
	// for just those ids. DISTINCT dedupes unmerged ReplacingMergeTree versions
	// (same id ⇒ same pubkey/created_at), replacing FINAL.
	where, args := eventWhere("e", nil, pubkeys, kinds, nil)
	idQuery := fmt.Sprintf(`
		SELECT id FROM (
			SELECT DISTINCT e.id AS id, e.pubkey AS pubkey, e.created_at AS created_at
			FROM nostr_events AS e
			%s
		)
		ORDER BY pubkey ASC, created_at DESC, id DESC
		LIMIT %d BY pubkey
	`, where, limitPerPubKey)
	idRows, err := s.conn.Query(ctx, idQuery, args...)
	if err != nil {
		return nil, err
	}
	ids := make([]string, 0, len(pubkeys))
	for idRows.Next() {
		var id string
		if err := idRows.Scan(&id); err != nil {
			idRows.Close()
			return nil, err
		}
		ids = append(ids, id)
	}
	if err := idRows.Err(); err != nil {
		idRows.Close()
		return nil, err
	}
	idRows.Close()

	out := make(map[string][]EventView, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = nil
	}
	if len(ids) == 0 {
		return out, nil
	}

	rows, err := s.conn.Query(ctx, `
		SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
		FROM nostr_events AS e
		WHERE e.id IN (?)
		ORDER BY e.pubkey ASC, e.created_at DESC, e.id DESC, e.last_seen_at DESC
		LIMIT 1 BY e.id
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

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

// LatestK3Refs returns each pubkey's latest kind-3 contact set (p-tag values)
// from the ingest-maintained latest_k3 table. Follow-graph reads must
// use this instead of QueryLatestEventsByPubKeys(kind=3): contact lists are the
// largest events on the network, and pulling them through the raw events table
// was the 7 GiB-per-lookup query that OOMed ClickHouse.
func (s *Store) LatestK3Refs(ctx context.Context, pubkeys []string) (map[string]map[string]struct{}, error) {
	pubkeys = uniqueStrings(pubkeys)
	out := make(map[string]map[string]struct{}, len(pubkeys))
	if len(pubkeys) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT pubkey, argMax(refs, created_at)
		FROM latest_k3
		WHERE pubkey IN (?)
		GROUP BY pubkey
	`, pubkeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pubkey string
		var contacts []string
		if err := rows.Scan(&pubkey, &contacts); err != nil {
			return nil, err
		}
		set := make(map[string]struct{}, len(contacts))
		for _, contact := range contacts {
			set[contact] = struct{}{}
		}
		out[pubkey] = set
	}
	return out, rows.Err()
}

// aggCol maps one selected column of the aggregates query back to its
// (rule, metric) identity, in select order.
type aggCol struct {
	rule   string
	metric string
}

// buildEventAggregatesQuery renders the one-round-trip aggregates read for
// every event-target rule in the registry plus the vertex-real block, and
// reports how many `?` bind slots (each an id list) the query carries.
func buildEventAggregatesQuery(reg *rules.Registry) (string, []aggCol, int) {
	var (
		unions  []string
		joins   []string
		selects []string
		cols    []aggCol
	)
	eventRules := 0
	for _, rel := range reg.Relationships() {
		if rel.Ref.Target != rules.TargetEventID {
			continue
		}
		table := rules.TableName(rel.Name)
		alias := fmt.Sprintf("a%d", eventRules)
		eventRules++
		if len(unions) == 0 {
			unions = append(unions, fmt.Sprintf("SELECT target AS id FROM %s WHERE target IN (?)", table))
		} else {
			unions = append(unions, fmt.Sprintf("UNION DISTINCT SELECT target FROM %s WHERE target IN (?)", table))
		}
		var metricSelects []string
		for _, m := range rel.Metrics {
			spec, _ := reg.ReadSpec(rel.Name, m.Name)
			metricSelects = append(metricSelects, fmt.Sprintf("%s(%s) AS %s", spec.MergeFunc, m.Name, m.Name))
			selects = append(selects, fmt.Sprintf("ifNull(%s.%s, 0)", alias, m.Name))
			cols = append(cols, aggCol{rule: rel.Name, metric: m.Name})
		}
		joins = append(joins, fmt.Sprintf(
			"LEFT JOIN (SELECT target, %s FROM %s WHERE target IN (?) GROUP BY target) %s ON %s.target = ids.id",
			strings.Join(metricSelects, ", "), table, alias, alias))
	}
	unions = append(unions, "UNION DISTINCT SELECT event_id FROM gated_ref_counts WHERE event_id IN (?)")
	joins = append(joins, `LEFT JOIN (
			SELECT event_id,
				argMax(k7_e_actors, computed_at) AS rl,
				argMax(k6_16_e_actors, computed_at) AS rr,
				argMax(k1_1111_e_reply_sources, computed_at) AS re,
				argMax(k1_q_sources, computed_at) AS rq,
				argMax(k9735_e_sources, computed_at) AS rzn,
				argMax(k9735_e_value_total, computed_at) AS rz,
				argMax(actors, computed_at) AS ra
			FROM gated_ref_counts WHERE event_id IN (?) GROUP BY event_id
		) er ON er.event_id = ids.id`)
	for _, c := range []struct{ expr, rule, metric string }{
		{"ifNull(er.rl, 0)", "vertex_k7_e", "actors"},
		{"ifNull(er.rr, 0)", "vertex_k6_16_e", "actors"},
		{"ifNull(er.re, 0)", "vertex_k1_1111_e_reply", "sources"},
		{"ifNull(er.rq, 0)", "vertex_k1_q", "sources"},
		{"ifNull(er.rzn, 0)", "vertex_k9735_e", "sources"},
		{"ifNull(er.rz, 0)", "vertex_k9735_e", "value_total"},
		{"ifNull(er.ra, 0)", "vertex_actors", "actors"},
	} {
		selects = append(selects, c.expr)
		cols = append(cols, aggCol{rule: c.rule, metric: c.metric})
	}

	query := fmt.Sprintf("SELECT\n\tids.id,\n\t%s\nFROM (\n\t%s\n) ids\n%s",
		strings.Join(selects, ",\n\t"),
		strings.Join(unions, "\n\t"),
		strings.Join(joins, "\n"))
	return query, cols, len(unions) + len(joins)
}

// EventAggregates returns, for each requested event id, every declared
// event-target aggregation's finalized metric values, keyed
// target -> rule name -> metric name. This is the generic successor of
// NoteStats: the rule set comes from the registry, so a newly declared
// relationship shows up in every envelope without a read-path change. The
// vertex-real threshold-filtered counts (gated_ref_counts) are exposed
// under "vertex_"-prefixed rule names pending the DVM plugin seam.
//
// ONE query on ONE connection: LEFT JOIN every aggregate onto the UNION of
// per-table target keys, so a whole page's aggregates are a single
// round-trip (see NoteStats for the history behind that constraint). Ids
// with no aggregates anywhere are simply absent from the result.
func (s *Store) EventAggregates(ctx context.Context, ids []string) (map[string]map[string]map[string]uint64, error) {
	ids = uniqueStrings(ids)
	out := make(map[string]map[string]map[string]uint64, len(ids))
	if len(ids) == 0 {
		return out, nil
	}

	query, cols, bindSlots := buildEventAggregatesQuery(s.rules)
	args := make([]any, 0, bindSlots)
	for i := 0; i < bindSlots; i++ {
		args = append(args, ids)
	}

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id string
		values := make([]uint64, len(cols))
		dest := make([]any, 0, len(cols)+1)
		dest = append(dest, &id)
		for i := range values {
			dest = append(dest, &values[i])
		}
		if err := rows.Scan(dest...); err != nil {
			return nil, err
		}
		byRule := make(map[string]map[string]uint64)
		for i, c := range cols {
			if values[i] == 0 {
				continue
			}
			if byRule[c.rule] == nil {
				byRule[c.rule] = make(map[string]uint64)
			}
			byRule[c.rule][c.metric] = values[i]
		}
		if len(byRule) > 0 {
			out[id] = byRule
		}
	}
	return out, rows.Err()
}

// FeatureWeights are the rank weights applied to the precomputed per-event
// feature columns. Engagement weights apply to the VERTEX-REAL counts (the
// bot-resistant signal); the raw counts are surfaced for display only. The
// values are owned by the caller (nagg-ts rank recipes) and arrive as SQL bind
// params, never hardcoded in the query.
type FeatureWeights struct {
	Likes   float64
	Reposts float64
	Replies float64
	Quotes  float64
	ZapSats float64
	// Actors is the weight on the distinct-engager ("actors") count — For-You's
	// primary engagement signal. It is applied WITHOUT LOG1P (identity), unlike
	// the per-type counts.
	Actors              float64
	AuthorVertexScore   float64
	ContributionQuality float64
	Recency             float64
}

// FeatureRankInput parameterizes the DB-side weighted top-N scan over
// rank_features. Since bounds the scan to a recent window so the trending
// query is a partition-pruned range scan, not a full-table read.
type FeatureRankInput struct {
	Kinds              []int
	Since              int64 // unix seconds; lower bound on created_at (required)
	Until              int64 // unix seconds; optional upper bound (0 = none)
	HalfLifeSeconds    float64
	Weights            FeatureWeights
	MinAuthorFollowers uint64 // gate the author-score contribution (0 = no gate)
	Limit              uint64
	ExcludeIDs         []string
	ExcludePubKeys     []string
}

// RankedFeatureRow is one scored candidate from the feature scan, already ordered
// by descending score.
type RankedFeatureRow struct {
	EventID string
	PubKey  string
	Score   float64
}

// RankedEventsByFeatures runs the whole weighted top-N ranking as one ClickHouse
// scan over the precomputed rank_features table. It replaces the per-request,
// per-term live aggregation (weightedRankBaseScores): recency decay + weighted sum
// over feature columns, ORDER BY score DESC LIMIT N. The recency and LOG1P
// transforms mirror candidateFieldValue / transformedFloatRankValue exactly
// (pow(0.5, age/halflife); log(1+x)) so the DB scores match the Go path.
func (s *Store) RankedEventsByFeatures(ctx context.Context, in FeatureRankInput) ([]RankedFeatureRow, error) {
	if in.Limit == 0 {
		return nil, nil
	}
	query, args := buildRankedEventsByFeaturesQuery(in)

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := make([]RankedFeatureRow, 0, in.Limit)
	for rows.Next() {
		var row RankedFeatureRow
		if err := rows.Scan(&row.EventID, &row.PubKey, &row.Score); err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, rows.Err()
}

// buildRankedEventsByFeaturesQuery renders the DB-first ranked read over
// rank_features. Split from the executor so tests can pin its column
// references against the declared schema (SQL is not compile-checked).
func buildRankedEventsByFeaturesQuery(in FeatureRankInput) (string, []any) {
	halfLife := in.HalfLifeSeconds
	if halfLife <= 0 {
		halfLife = 86400
	}

	w := in.Weights
	// SELECT bind params, in column order.
	args := []any{
		w.Likes, w.Replies, w.Reposts, w.Quotes, w.ZapSats,
		w.Actors,
		w.AuthorVertexScore, in.MinAuthorFollowers,
		w.ContributionQuality,
		w.Recency, halfLife,
	}
	score := `
		  ? * log(1 + gated_k7_e_actors)
		+ ? * log(1 + gated_k1_1111_e_reply_sources)
		+ ? * log(1 + gated_k6_16_e_actors)
		+ ? * log(1 + gated_k1_q_sources)
		+ ? * log(1 + gated_k9735_e_value_total)
		+ ? * gated_actors
		+ ? * if(author_followers >= ?, author_score, 0)
		+ ? * contribution_quality
		+ ? * pow(0.5, greatest(toUnixTimestamp(now()) - toUnixTimestamp(created_at), 0) / ?)`

	// Filters run in the inner (pre-GROUP BY) WHERE. created_at and pubkey are
	// qualified with the table name so they bind to the raw columns rather than the
	// argMax/max aliases of the same name in the SELECT (ClickHouse's analyzer would
	// otherwise reject "max(created_at) found in WHERE").
	where := "WHERE rank_features.created_at >= toDateTime(?)"
	args = append(args, in.Since)
	if in.Until > 0 {
		where += " AND rank_features.created_at <= toDateTime(?)"
		args = append(args, in.Until)
	}
	if len(in.Kinds) > 0 {
		where += fmt.Sprintf(" AND kind IN (%s)", ints(in.Kinds))
	}
	if len(in.ExcludeIDs) > 0 {
		where += " AND event_id NOT IN (?)"
		args = append(args, in.ExcludeIDs)
	}
	if len(in.ExcludePubKeys) > 0 {
		where += " AND rank_features.pubkey NOT IN (?)"
		args = append(args, in.ExcludePubKeys)
	}

	// Collapse the ReplacingMergeTree(computed_at) duplicates with argMax/GROUP BY
	// instead of FINAL. The rollup re-inserts the whole target set each tick, so
	// rank_features carries several unmerged versions per event until a
	// background merge; FINAL merges every part on each read, whereas a hash
	// aggregation over the (partition-pruned) recent window does not. The score
	// expression and bind order are unchanged — only the source is grouped.
	query := fmt.Sprintf(`
		SELECT event_id, pubkey, (%s) AS score
		FROM (
			SELECT
				event_id,
				argMax(pubkey, computed_at) AS pubkey,
				max(created_at) AS created_at,
				argMax(gated_k7_e_actors, computed_at) AS gated_k7_e_actors,
				argMax(gated_k1_1111_e_reply_sources, computed_at) AS gated_k1_1111_e_reply_sources,
				argMax(gated_k6_16_e_actors, computed_at) AS gated_k6_16_e_actors,
				argMax(gated_k1_q_sources, computed_at) AS gated_k1_q_sources,
				argMax(gated_k9735_e_value_total, computed_at) AS gated_k9735_e_value_total,
				argMax(gated_actors, computed_at) AS gated_actors,
				argMax(author_followers, computed_at) AS author_followers,
				argMax(author_score, computed_at) AS author_score,
				argMax(contribution_quality, computed_at) AS contribution_quality
			FROM rank_features
			%s
			GROUP BY event_id
		)
		ORDER BY score DESC, created_at DESC, event_id DESC
		LIMIT %d
	`, score, where, in.Limit)

	return query, args
}

// RefSourceIDs returns the ids of the DIRECT (NIP-10/22) replies to parentID,
// from the authoritative ref_edges table. This excludes grandchildren and
// quotes that the any-e-tag reverse-reference query would otherwise include. The
// caller (thread view) applies its own ordering / ranking / pagination over the
// returned set; the cap bounds a pathological reply storm.
func (s *Store) RefSourceIDs(ctx context.Context, parentID string) ([]string, error) {
	if len(parentID) != 64 {
		return nil, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT source_id
		FROM ref_edges
		WHERE target_id = ?
		GROUP BY source_id
		ORDER BY max(created_at) DESC, source_id DESC
		LIMIT 5000
	`, parentID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// refRankSpec resolves a "rule.metric" sort key (e.g. "k7_e.actors",
// "k9735_e.value_total") to the declared relationship's read spec. Only
// event-target rules rank replies; anything else — including the recency
// keys "new" and "" — reports ok == false.
func (s *Store) refRankSpec(sort string) (rules.ReadSpec, bool) {
	rule, metric, ok := strings.Cut(sort, ".")
	if !ok {
		return rules.ReadSpec{}, false
	}
	rel := s.rules.Relationship(rule)
	if rel == nil || rel.Ref.Target != rules.TargetEventID {
		return rules.ReadSpec{}, false
	}
	return s.rules.ReadSpec(rule, metric)
}

// RankedRefSources returns the DIRECT (NIP-10/22) replies to parentID
// ordered by a declared aggregation ("rule.metric" sort keys such as
// "k7_e.actors" or "k9735_e.value_total") or by recency ("new"/""). It reads
// only precomputed tables — ref_edges for the reply set + the rule's
// aggregate table for the sort — so the thread reply list ranks without a
// live aggregation. A LEFT JOIN keeps replies with zero engagement
// (recency tiebreak), so the full reply set is returned, ranked.
func (s *Store) RankedRefSources(ctx context.Context, parentID, sort string, limit, offset int) ([]string, error) {
	if len(parentID) != 64 {
		return nil, nil
	}
	if limit <= 0 || limit > 5000 {
		limit = 5000
	}
	if offset < 0 {
		offset = 0
	}

	spec, ranked := s.refRankSpec(sort)
	var query string
	var args []any
	if !ranked {
		// Recency: the edge table already carries the reply created_at.
		query = `
			SELECT source_id
			FROM ref_edges
			WHERE target_id = ?
			GROUP BY source_id
			ORDER BY max(created_at) DESC, source_id DESC
			LIMIT ? OFFSET ?`
		args = []any{parentID, limit, offset}
	} else {
		query = fmt.Sprintf(`
			SELECT e.source_id
			FROM (
				SELECT source_id, max(created_at) AS created_at
				FROM ref_edges WHERE target_id = ? GROUP BY source_id
			) e
			LEFT JOIN (
				SELECT target, %s(%s) AS c
				FROM %s
				WHERE target IN (SELECT source_id FROM ref_edges WHERE target_id = ?)
				GROUP BY target
			) m ON m.target = e.source_id
			ORDER BY ifNull(m.c, 0) DESC, e.created_at DESC, e.source_id DESC
			LIMIT ? OFFSET ?`, spec.MergeFunc, spec.Column, spec.Table)
		args = []any{parentID, parentID, limit, offset}
	}

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []string{}
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
}

// AuthoredRefChain walks the precomputed direct-reply edges (ref_edges)
// down from rootID, following the single EARLIEST direct reply authored by
// `author` at each level — the author's self-reply chain that the live
// authoredReplyChain field computed with a per-depth event_tags scan. Each step is
// an indexed target_id lookup; bounded by maxDepth.
func (s *Store) AuthoredRefChain(ctx context.Context, rootID, author string, maxDepth int) ([]string, error) {
	if len(rootID) != 64 || len(author) != 64 {
		return nil, nil
	}
	if maxDepth <= 0 || maxDepth > 64 {
		maxDepth = 8
	}
	chain := make([]string, 0, maxDepth)
	visited := map[string]struct{}{rootID: {}}
	current := rootID
	for depth := 0; depth < maxDepth; depth++ {
		child, err := s.earliestAuthoredChild(ctx, current, author)
		if err != nil {
			return nil, err
		}
		if child == "" {
			break
		}
		if _, seen := visited[child]; seen {
			break
		}
		visited[child] = struct{}{}
		chain = append(chain, child)
		current = child
	}
	return chain, nil
}

// earliestAuthoredChild returns the earliest direct reply to parentID authored by
// `author` (matching bestAuthoredDirectChild's selection), or "" when none.
func (s *Store) earliestAuthoredChild(ctx context.Context, parentID, author string) (string, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT source_id
		FROM ref_edges
		WHERE target_id = ? AND source_pubkey = ?
		GROUP BY source_id
		ORDER BY min(created_at) ASC, source_id ASC
		LIMIT 1
	`, parentID, author)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	if !rows.Next() {
		return "", rows.Err()
	}
	var child string
	if err := rows.Scan(&child); err != nil {
		return "", err
	}
	return child, rows.Err()
}

// FollowedRefs returns, for each parent in parentIDs, the single best reply
// authored by someone the viewer follows — the precomputed, BATCHED replacement
// for the per-node followedReply (rankedReferencedBy over the viewer's follow list)
// the feed used to embed. "Best" = most-liked (note_like_counts), tie-broken by
// recency. Reads only precomputed tables: ref_edges (NIP-10/22 direct
// replies), note_like_counts (ranking), latest_k3 (the viewer's
// replaceable-aware follow set) — no live aggregation, one round-trip for the page.
func (s *Store) FollowedRefs(ctx context.Context, viewerPubkey string, parentIDs []string) (map[string]string, error) {
	out := make(map[string]string, len(parentIDs))
	if len(viewerPubkey) != 64 || len(parentIDs) == 0 {
		return out, nil
	}
	parentIDs = uniqueStrings(parentIDs)
	// sort_key packs likes into the high 32 bits and the reply timestamp into the
	// low 32 bits, so argMax picks the most-liked reply, tie-broken by newest.
	rows, err := s.conn.Query(ctx, `
		SELECT target_id, argMax(source_id, sort_key) AS reply_id
		FROM (
			SELECT
				e.target_id AS target_id,
				e.source_id  AS source_id,
				bitShiftLeft(toUInt64(ifNull(lc.likes, 0)), 32) + toUInt64(toUnixTimestamp(e.created_at)) AS sort_key
			FROM ref_edges e
			LEFT JOIN (
				SELECT target, uniqMerge(actors) AS likes
				FROM agg_k7_e
				WHERE target IN (
					SELECT source_id FROM ref_edges WHERE target_id IN (?)
				)
				GROUP BY target
			) lc ON e.source_id = lc.target
			WHERE e.target_id IN (?)
			  AND e.source_pubkey IN (
				SELECT arrayJoin(refs) FROM latest_k3 FINAL WHERE pubkey = ?
			  )
		)
		GROUP BY target_id
	`, parentIDs, parentIDs, viewerPubkey)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var parentID, replyID string
		if err := rows.Scan(&parentID, &replyID); err != nil {
			return nil, err
		}
		if replyID != "" {
			out[parentID] = replyID
		}
	}
	return out, rows.Err()
}

const batchPubkeyStatsQuery = `
		SELECT pubkey, argMax(k3_in, computed_at)
		FROM pubkey_stats
		WHERE pubkey IN (?)
		GROUP BY pubkey
	`

func (s *Store) PubkeyStats(ctx context.Context, pubkey string) (PubkeyStats, error) {
	counts, err := s.BatchPubkeyStats(ctx, []string{pubkey})
	if err != nil {
		return PubkeyStats{}, err
	}
	return counts[pubkey], nil
}

func (s *Store) BatchPubkeyStats(ctx context.Context, pubkeys []string) (map[string]PubkeyStats, error) {
	pubkeys = uniqueStrings(pubkeys)
	out := make(map[string]PubkeyStats, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = PubkeyStats{}
	}
	if len(pubkeys) == 0 {
		return out, nil
	}
	contacts, err := s.LatestK3Refs(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	for _, pubkey := range pubkeys {
		counts := out[pubkey]
		counts.Follows = uint64(len(contacts[pubkey]))
		out[pubkey] = counts
	}

	// Followers come from the rollup-maintained pubkey_stats table (015 backfilled
	// it to full coverage). The previous read — uniqExact over the ENTIRE global
	// kind-3 p-tag history in event_tags — was both wrong (counted every pubkey
	// that EVER followed, ignoring NIP-02 replaceability) and one of the heavy
	// per-request aggregations on a 2B-row table.
	found, err := s.readPubkeyStatsInto(ctx, pubkeys, out)
	if err != nil {
		return nil, err
	}

	// Read-through fill: the rollup only covers recently-active authors, so a
	// cold profile (no events inside the touched window) has no row and would
	// read as zero inbound refs forever — even with its referencing lists
	// fully indexed. Compute the missing pubkeys' stats on demand and write
	// them back, so the next read is a plain row hit.
	missing := make([]string, 0)
	for _, pubkey := range pubkeys {
		if _, ok := found[pubkey]; !ok {
			missing = append(missing, pubkey)
		}
	}
	if len(missing) > 0 && len(missing) <= pubkeyStatsFillCap {
		quoted := make([]string, len(missing))
		for i, pk := range missing {
			quoted[i] = "'" + pk + "'"
		}
		population := "SELECT arrayJoin([" + strings.Join(quoted, ", ") + "]) AS pubkey"
		if err := s.conn.Exec(ctx, buildPubkeyStatsForSQL(population, time.Now().UTC())); err != nil {
			// Fill is best-effort: the profile still renders with the live
			// k3_out count; followers stay zero until a later fill succeeds.
			return out, nil
		}
		if _, err := s.readPubkeyStatsInto(ctx, missing, out); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// pubkeyStatsFillCap bounds the on-demand fill population per read — profile
// pages ask for a handful of pubkeys; anything larger waits for the rollup.
const pubkeyStatsFillCap = 20

// readPubkeyStatsInto reads stored rows into out, returning which pubkeys had
// a row at all (a stored zero is a real answer; a missing row is not).
func (s *Store) readPubkeyStatsInto(ctx context.Context, pubkeys []string, out map[string]PubkeyStats) (map[string]struct{}, error) {
	rows, err := s.conn.Query(ctx, batchPubkeyStatsQuery, pubkeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	found := make(map[string]struct{}, len(pubkeys))
	for rows.Next() {
		var pubkey string
		var followers uint64
		if err := rows.Scan(&pubkey, &followers); err != nil {
			return nil, err
		}
		found[pubkey] = struct{}{}
		counts := out[pubkey]
		counts.Followers = followers
		out[pubkey] = counts
	}
	return found, rows.Err()
}

// FollowEdge describes the directional follow relationship between a viewer and
// a candidate pubkey.
type FollowEdge struct {
	Following  bool `json:"following"`  // viewer -> candidate
	FollowsYou bool `json:"followsYou"` // candidate -> viewer
}

// FollowEdges reports, for each candidate, whether the viewer follows them and
// whether they follow the viewer. Both directions are derived from the latest
// kind-3 contact list of the relevant author (not raw tag history) so a contact
// dropped from a newer list is not reported as a stale follow.
func (s *Store) FollowEdges(ctx context.Context, viewer string, candidates []string) (map[string]FollowEdge, error) {
	candidates = uniqueStrings(candidates)
	out := make(map[string]FollowEdge, len(candidates))
	for _, candidate := range candidates {
		out[candidate] = FollowEdge{}
	}
	if viewer == "" || len(candidates) == 0 {
		return out, nil
	}

	// Both directions from the ingest-maintained latest contact lists — one
	// batched read covers the viewer and every candidate.
	contacts, err := s.LatestK3Refs(ctx, append([]string{viewer}, candidates...))
	if err != nil {
		return nil, err
	}
	viewerFollows := contacts[viewer]
	for _, candidate := range candidates {
		edge := out[candidate]
		if _, ok := viewerFollows[candidate]; ok {
			edge.Following = true
		}
		if _, ok := contacts[candidate][viewer]; ok {
			edge.FollowsYou = true
		}
		out[candidate] = edge
	}
	return out, nil
}

func (s *Store) FollowerCount(ctx context.Context, pubkey string) (uint64, error) {
	counts, err := s.BatchPubkeyStats(ctx, []string{pubkey})
	if err != nil {
		return 0, err
	}
	return counts[pubkey].Followers, nil
}

// CountEventsOfKind reports how many events of one kind are stored — the
// seed fetch's empty-store gate.
func (s *Store) CountEventsOfKind(ctx context.Context, kind int) (uint64, error) {
	var count uint64
	err := s.conn.QueryRow(ctx, "SELECT count() FROM nostr_events WHERE kind = ?", kind).Scan(&count)
	return count, err
}

const recentAuthorsBySyncGateQuery = `
		SELECT recent.pubkey
		FROM
		(
			SELECT pubkey, max(created_at) AS last_event_at
			FROM nostr_events FINAL
			-- No byte-size filter on the author column: it is FixedString(64),
			-- so the check is a tautology — and ClickHouse 26.6 returns ZERO
			-- rows for FINAL combined with such filters (observed live: it
			-- silently emptied the sync candidate set).
			WHERE created_at >= now() - INTERVAL 30 DAY
			GROUP BY pubkey
		) AS recent
		INNER JOIN
		(
			-- Follower count from the LATEST contact list per follower (NIP-02 is
			-- replaceable). The legacy uniqExact over all kind-3 history counted
			-- anyone who EVER followed you, inflating the count and wasting Vertex
			-- credits on over-qualified authors. latest_k3 is MV-fed +
			-- backfilled, so this needs no rollup bootstrap.
			SELECT follow AS pubkey, count() AS followers
			FROM (SELECT arrayJoin(refs) AS follow FROM latest_k3 FINAL)
			GROUP BY follow
			HAVING followers >= ?
		) AS follower_counts ON follower_counts.pubkey = recent.pubkey
		LEFT JOIN
		(
			SELECT pubkey, max(fetched_at) AS fetched_at
			FROM vertex_scores FINAL
			WHERE source = 'vertex'
			GROUP BY pubkey
		) AS scores ON scores.pubkey = recent.pubkey
		WHERE ifNull(scores.fetched_at, toDateTime(0)) < now() - INTERVAL ? SECOND
		ORDER BY recent.last_event_at DESC
		LIMIT ?
	`

func (s *Store) RecentAuthorPubkeysByFollowers(ctx context.Context, minFollowers uint64, staleAfter time.Duration, limit int) ([]string, error) {
	if staleAfter <= 0 {
		staleAfter = 7 * 24 * time.Hour
	}
	if limit <= 0 {
		limit = 200
	}
	rows, err := s.conn.Query(ctx, recentAuthorsBySyncGateQuery, minFollowers, int64(staleAfter/time.Second), limit)
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

func (s *Store) CachedVertexProfiles(ctx context.Context, pubkeys []string) (map[string]vertex.ProfileResult, error) {
	pubkeys = uniqueStrings(pubkeys)
	out := make(map[string]vertex.ProfileResult, len(pubkeys))
	if len(pubkeys) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT pubkey, payload
		FROM vertex_profile_cache FINAL
		WHERE pubkey IN (?)
	`, pubkeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var pubkey string
		var payload string
		if err := rows.Scan(&pubkey, &payload); err != nil {
			return nil, err
		}
		var profile vertex.ProfileResult
		if err := json.Unmarshal([]byte(payload), &profile); err != nil {
			return nil, err
		}
		if profile.PubKey == "" {
			profile.PubKey = pubkey
		}
		if profile.Npub == "" {
			profile.Npub = vertex.Npub(profile.PubKey)
		}
		out[pubkey] = profile
	}
	return out, rows.Err()
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
	`, vertex.PluginName, pubkey, *profile.Score, profile.Rank, followers, nodes, fetchedAt)
}

func (s *Store) CachedVertexSearch(ctx context.Context, args vertex.SearchArgs) ([]vertex.SearchResult, time.Time, bool, error) {
	args = vertex.NormalizeSearchArgs(args)
	queryNorm := strings.ToLower(args.Query)
	var fetchedAt sql.NullTime
	if err := s.conn.QueryRow(ctx, `
		SELECT maxOrNull(fetched_at)
		FROM vertex_search_cache FINAL
		WHERE query_norm = ? AND sort = ? AND source = ? AND requested_limit = ?
	`, queryNorm, args.Sort, args.Source, uint64(args.Limit)).Scan(&fetchedAt); err != nil {
		return nil, time.Time{}, false, err
	}
	if !fetchedAt.Valid {
		return nil, time.Time{}, false, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT pubkey, rank, score, nodes
		FROM vertex_search_cache FINAL
		WHERE query_norm = ? AND sort = ? AND source = ? AND requested_limit = ? AND fetched_at = ?
		ORDER BY position ASC
	`, queryNorm, args.Sort, args.Source, uint64(args.Limit), fetchedAt.Time)
	if err != nil {
		return nil, time.Time{}, false, err
	}
	defer rows.Close()
	out := make([]vertex.SearchResult, 0, args.Limit)
	for rows.Next() {
		var pubkey string
		var rank sql.NullFloat64
		var score sql.NullFloat64
		var nodes uint64
		if err := rows.Scan(&pubkey, &rank, &score, &nodes); err != nil {
			return nil, time.Time{}, false, err
		}
		result := vertex.SearchResult{
			PubKey: pubkey,
			Npub:   vertex.Npub(pubkey),
		}
		if rank.Valid {
			value := rank.Float64
			result.Rank = &value
		}
		if score.Valid {
			value := score.Float64
			result.Score = &value
		}
		if nodes > 0 {
			value := int(nodes)
			result.Nodes = &value
		}
		out = append(out, result)
	}
	if err := rows.Err(); err != nil {
		return nil, time.Time{}, false, err
	}
	if len(out) == 0 {
		return nil, time.Time{}, false, nil
	}
	return out, fetchedAt.Time.UTC(), true, nil
}

func (s *Store) SaveVertexSearch(ctx context.Context, args vertex.SearchArgs, results []vertex.SearchResult) error {
	args = vertex.NormalizeSearchArgs(args)
	if len(results) == 0 {
		return nil
	}
	batch, err := s.prepareInsertBatch(ctx, `
		INSERT INTO vertex_search_cache
			(query_norm, sort, source, requested_limit, position, pubkey, rank, score, nodes, fetched_at)
		VALUES
	`)
	if err != nil {
		return err
	}
	defer closeUnsentBatch(batch)
	queryNorm := strings.ToLower(args.Query)
	fetchedAt := time.Now().UTC()
	for position, row := range results {
		pubkey, ok := vertex.NormalizePubkey(row.PubKey)
		if !ok {
			continue
		}
		var rank any
		if row.Rank != nil {
			rank = *row.Rank
		}
		var score any
		if row.Score != nil {
			score = *row.Score
		}
		var nodes uint64
		if row.Nodes != nil && *row.Nodes > 0 {
			nodes = uint64(*row.Nodes)
		}
		if err := batch.Append(
			queryNorm,
			args.Sort,
			args.Source,
			uint64(args.Limit),
			uint64(position),
			pubkey,
			rank,
			score,
			nodes,
			fetchedAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (s *Store) AuthorVertexScores(ctx context.Context, pubkeys []string) (map[string]PubkeyScore, error) {
	return s.PubkeyScores(ctx, vertex.PluginName, pubkeys)
}

func (s *Store) PubkeyScores(ctx context.Context, source string, pubkeys []string) (map[string]PubkeyScore, error) {
	source = strings.TrimSpace(strings.ToLower(source))
	if source == "" {
		source = vertex.PluginName
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

func (s *Store) ViewerFeed(ctx context.Context, input ViewerFeedInput) ([]ViewerFeedRow, error) {
	input.Viewer = strings.TrimSpace(strings.ToLower(input.Viewer))
	if input.Viewer == "" {
		return []ViewerFeedRow{}, nil
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
	replyScope := strings.ToUpper(strings.TrimSpace(input.ReplyScope))
	if replyScope == "" {
		replyScope = "THREAD"
	}

	// Prefer the denormalized read-model once its watermark has caught up —
	// a keyed range scan instead of the FINAL-join + tag-scan query below.
	// While the model backfills after a fresh deploy the legacy query serves
	// (a bootstrap condition, not a fallback: it self-clears within minutes).
	if !s.notificationsLegacyRead && s.notificationsFeedReady(ctx) {
		return s.notificationsFromFeed(ctx, input, tab, policy, replyScope)
	}

	// Bound how many recent candidates we hydrate before the heavy FINAL joins
	// and reply-reference scans. The candidate table is ORDER BY (viewer,
	// created_at, ...), so taking the most-recent window for a viewer is a cheap
	// range scan; every downstream join then probes only this small set instead
	// of the viewer's entire notification history. We over-fetch so that the
	// follow dedupe, policy threshold, and reply-scope filters still leave at
	// least `limit` rows for high-volume viewers.
	overfetch := input.Limit * 8
	if overfetch < 400 {
		overfetch = 400
	}
	if overfetch > 4000 {
		overfetch = 4000
	}

	// recentFilters / recentArgs scope the candidate window itself (cheap, runs
	// before any join). tab/since/until belong here, not in the outer WHERE.
	recentFilters := ""
	recentArgs := []any{}
	if tab == "MENTIONS" {
		recentFilters += " AND kind = 1"
	}
	if input.Since > 0 {
		recentFilters += " AND created_at >= ?"
		recentArgs = append(recentArgs, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		recentFilters += " AND created_at < ?"
		recentArgs = append(recentArgs, time.Unix(input.Until, 0).UTC())
	}
	if len(input.Kinds) > 0 {
		recentFilters += " AND kind IN (?)"
		recentArgs = append(recentArgs, input.Kinds)
	}
	if len(input.ExcludeKinds) > 0 {
		recentFilters += " AND kind NOT IN (?)"
		recentArgs = append(recentArgs, input.ExcludeKinds)
	}
	if policy == "FOLLOWS" {
		// Only notifications whose actor is in the viewer's follow set: the p-tags
		// of the viewer's single latest kind-3 contact list, parsed straight from
		// tags_json. The previous form expanded the kind-3 id against event_tags
		// WHERE tag_key='p', but event_id is LAST in event_tags' sort key
		// (tag_key, tag_value, kind, created_at, event_id), so that probe
		// full-scanned the entire global p-tag range (~10s). Reading the one kind-3
		// row off nostr_events' (kind, created_at, pubkey, id) prefix and extracting
		// its p-tags is a bounded point lookup.
		// Select the single latest kind-3 event FIRST (inner LIMIT 1), then
		// arrayJoin its p-tags in the outer query — otherwise LIMIT 1 would apply
		// after arrayJoin and return only the first follow.
		recentFilters += ` AND actor_pubkey IN (
			SELECT arrayJoin(
				arrayMap(t -> t[2],
					arrayFilter(t -> length(t) >= 2 AND t[1] = 'p' AND length(t[2]) = 64,
						JSONExtract(tags_json, 'Array(Array(String))')))
			)
			FROM (
				SELECT tags_json
				FROM nostr_events
				WHERE pubkey = ? AND kind = 3
				ORDER BY created_at DESC, last_seen_at DESC
				LIMIT 1
			)
		)`
		recentArgs = append(recentArgs, input.Viewer)
	}

	where := "WHERE 1 = 1"
	actorThreshold, viewerThreshold := notificationPolicyThresholds(policy)
	policyArgs := []any{}
	policyWhere := ""
	if actorThreshold > 0 || viewerThreshold > 0 {
		policyWhere = " AND (ifNull(actor_score.score, 0) >= ? OR ifNull(viewer_score.score, 0) >= ?)"
		policyArgs = append(policyArgs, actorThreshold, viewerThreshold)
	}

	// The reply-reference subqueries are the most expensive part of the legacy
	// query because they scan all of event_tags. We bound every one of them to
	// the candidate event ids in `recent` so the work scales with the page size,
	// not the global tag table.
	replyReferenceJoin := ""
	replyArgs := []any{}
	if replyScope == "DIRECT" || replyScope == "THREAD" {
		replyReferenceJoin = `
			LEFT JOIN (
				SELECT
					event_id,
					countIf(marker IN ('', 'root', 'reply')) > 0 AS is_reply,
					coalesce(
						nullIf(argMinIf(tag_value, tag_index, marker = 'reply'), ''),
						nullIf(argMaxIf(tag_value, tag_index, marker = ''), ''),
						nullIf(argMinIf(tag_value, tag_index, marker = 'root'), '')
					) AS direct_target_id
				FROM (
					SELECT
						event_id,
						tag_value,
						tag_index,
						lower(if(length(tag_extra) >= 2, tag_extra[2], '')) AS marker
					FROM event_tags
					WHERE tag_key = 'e' AND length(tag_value) = 64
					  AND event_id IN (SELECT event_id FROM recent)
				)
				GROUP BY event_id
			) AS reply_meta ON reply_meta.event_id = e.id
			LEFT JOIN (
				SELECT id, pubkey
				FROM nostr_events
				WHERE id IN (
					SELECT tag_value FROM event_tags
					WHERE tag_key = 'e' AND length(tag_value) = 64
					  AND event_id IN (SELECT event_id FROM recent)
				)
				ORDER BY id ASC, last_seen_at DESC
				LIMIT 1 BY id
			) AS reply_parent ON reply_parent.id = reply_meta.direct_target_id
			LEFT JOIN (
				SELECT rt.event_id, count() > 0 AS has_viewer_reply_reference
				FROM event_tags AS rt
				INNER JOIN (
					SELECT id, pubkey FROM nostr_events
					WHERE id IN (
						SELECT tag_value FROM event_tags
						WHERE tag_key = 'e' AND length(tag_value) = 64
						  AND event_id IN (SELECT event_id FROM recent)
					)
					ORDER BY id ASC, last_seen_at DESC
					LIMIT 1 BY id
				) AS referenced ON referenced.id = rt.tag_value
				WHERE rt.tag_key = 'e'
				  AND length(rt.tag_value) = 64
				  AND lower(if(length(rt.tag_extra) >= 2, rt.tag_extra[2], '')) IN ('', 'root', 'reply')
				  AND referenced.pubkey = ?
				  AND rt.event_id IN (SELECT event_id FROM recent)
				GROUP BY rt.event_id
			) AS viewer_reply_refs ON viewer_reply_refs.event_id = e.id`
		replyArgs = append(replyArgs, input.Viewer)
	}
	switch replyScope {
	case "DIRECT":
		where += " AND (e.kind != 1 OR ifNull(reply_meta.is_reply, 0) = 0 OR reply_parent.pubkey = n.viewer)"
	case "THREAD":
		where += " AND (e.kind != 1 OR ifNull(reply_meta.is_reply, 0) = 0 OR ifNull(viewer_reply_refs.has_viewer_reply_reference, 0) = 1)"
	}

	// Positional args in textual order:
	//   recent CTE: viewer, [since], [until], overfetch
	//   viewer_score subquery: viewer
	//   reply join (optional): viewer
	//   policy (optional): actorThreshold, viewerThreshold
	//   final LIMIT
	args := []any{input.Viewer}
	args = append(args, recentArgs...)
	args = append(args, overfetch)
	args = append(args, input.Viewer)
	args = append(args, replyArgs...)
	args = append(args, policyArgs...)
	args = append(args, input.Limit)

	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		WITH recent AS (
			SELECT viewer, event_id, actor_pubkey, kind, created_at
			FROM (
				SELECT viewer, event_id, actor_pubkey, kind, created_at
				FROM viewer_refs
				WHERE viewer = ?%s
				ORDER BY created_at DESC, event_id DESC
				LIMIT ?
			)
			LIMIT 1 BY event_id, kind
		)
		SELECT
			id,
			pubkey,
			kind,
			created_at,
			content,
			tags_json,
			sig,
			last_seen_at,
			actor_vertex_score,
			actor_pubkey,
			notification_created_at
		FROM (
			SELECT
				e.id AS id,
				e.pubkey AS pubkey,
				e.kind AS kind,
				e.created_at AS created_at,
				e.content AS content,
				e.tags_json AS tags_json,
				e.sig AS sig,
				e.last_seen_at AS last_seen_at,
				ifNull(actor_score.score, 0) AS actor_vertex_score,
				n.actor_pubkey AS actor_pubkey,
				n.created_at AS notification_created_at,
				n.event_id AS notification_event_id
			FROM (
				SELECT viewer, event_id, actor_pubkey, kind, created_at
				FROM (
					SELECT
						viewer,
						event_id,
						actor_pubkey,
						kind,
						created_at,
						row_number() OVER (
							PARTITION BY kind, actor_pubkey
							ORDER BY created_at ASC, event_id ASC
						) AS actor_kind_rank
					FROM recent
				)
				WHERE kind != 3 OR actor_kind_rank = 1
			) AS n
			INNER JOIN (
				SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
				FROM nostr_events
				WHERE id IN (SELECT event_id FROM recent)
				ORDER BY id ASC, last_seen_at DESC
				LIMIT 1 BY id
			) AS e ON e.id = n.event_id
			LEFT JOIN (
				SELECT pubkey, argMax(score, fetched_at) AS score
				FROM vertex_scores
				WHERE source = 'vertex' AND pubkey IN (SELECT actor_pubkey FROM recent)
				GROUP BY pubkey
			) AS actor_score ON actor_score.pubkey = n.actor_pubkey
			LEFT JOIN (
				SELECT pubkey, argMax(score, fetched_at) AS score
				FROM vertex_scores
				WHERE source = 'vertex' AND pubkey = ?
				GROUP BY pubkey
			) AS viewer_score ON viewer_score.pubkey = n.viewer
			%s
			%s%s
		)
		ORDER BY notification_created_at DESC, notification_event_id DESC
		LIMIT ?
	`, recentFilters, replyReferenceJoin, where, policyWhere), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []ViewerFeedRow{}
	for rows.Next() {
		var row ViewerFeedRow
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
			&row.ActorVertexScore,
			&row.ActorPubKey,
			&row.RefCreatedAt,
		); err != nil {
			return nil, err
		}
		row.Event.Kind = int(kind)
		row.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &row.Event.Tags)
		out = append(out, row)
	}
	return out, rows.Err()
}

func notificationPolicyThresholds(policy string) (float64, float64) {
	switch strings.ToUpper(strings.TrimSpace(policy)) {
	case "RELAXED", "FOLLOWS":
		// FOLLOWS gates on the follow graph, not vertex scores, so no score
		// threshold applies (the actor-in-follows filter does the gating).
		return 0, 0
	case "MODERATE":
		return 20, 60
	default:
		return 50, 80
	}
}

func (s *Store) LatestK0(ctx context.Context, pubkeys []string) (map[string]K0Row, error) {
	pubkeys = uniqueStrings(pubkeys)
	out := make(map[string]K0Row, len(pubkeys))
	if len(pubkeys) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT pubkey, event_id, created_at, name, display_name, picture, about, nip05, lud16, lud06, banner, website, raw_json
		FROM latest_k0 FINAL
		WHERE pubkey IN (?)
	`, pubkeys)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var profile K0Row
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

func (s *Store) SearchK0(ctx context.Context, query string, limit uint64) ([]ProfileSearchRow, error) {
	query = strings.TrimSpace(query)
	if len(query) < 3 {
		return []ProfileSearchRow{}, nil
	}
	if limit == 0 || limit > 100 {
		limit = 10
	}
	if pubkey, ok := vertex.NormalizePubkey(query); ok {
		profiles, err := s.LatestK0(ctx, []string{pubkey})
		if err != nil {
			return nil, err
		}
		profile := profiles[pubkey]
		if profile.PubKey == "" {
			profile.PubKey = pubkey
		}
		return []ProfileSearchRow{{Profile: profile, Rank: 100, Score: 100}}, nil
	}

	queryLower := strings.ToLower(query)
	scoreExpr := `
		greatest(
			if(lowerUTF8(name) = ?, 100.0, 0.0),
			if(lowerUTF8(display_name) = ?, 98.0, 0.0),
			if(lowerUTF8(nip05) = ?, 96.0, 0.0),
			if(startsWith(lowerUTF8(name), ?), 90.0, 0.0),
			if(startsWith(lowerUTF8(display_name), ?), 88.0, 0.0),
			if(startsWith(lowerUTF8(nip05), ?), 86.0, 0.0),
			if(positionCaseInsensitiveUTF8(name, ?) > 0, 76.0, 0.0),
			if(positionCaseInsensitiveUTF8(display_name, ?) > 0, 74.0, 0.0),
			if(positionCaseInsensitiveUTF8(nip05, ?) > 0, 72.0, 0.0),
			if(positionCaseInsensitiveUTF8(lud16, ?) > 0, 60.0, 0.0),
			if(positionCaseInsensitiveUTF8(website, ?) > 0, 50.0, 0.0),
			if(positionCaseInsensitiveUTF8(about, ?) > 0, 30.0, 0.0),
			if(positionCaseInsensitiveUTF8(raw_json, ?) > 0, 10.0, 0.0)
		)
	`
	args := []any{
		queryLower, queryLower, queryLower,
		queryLower, queryLower, queryLower,
		query, query, query, query, query, query, query,
		query,
		limit,
	}
	rows, err := s.conn.Query(ctx, `
		SELECT pubkey, event_id, created_at, name, display_name, picture, about, nip05, lud16, lud06, banner, website, raw_json, `+scoreExpr+` AS relevance
		FROM latest_k0 FINAL
		WHERE positionCaseInsensitiveUTF8(concat(name, ' ', display_name, ' ', nip05, ' ', lud16, ' ', website, ' ', about, ' ', raw_json), ?) > 0
		ORDER BY relevance DESC, created_at DESC, pubkey ASC
		LIMIT ?
	`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]ProfileSearchRow, 0, limit)
	for rows.Next() {
		var row ProfileSearchRow
		if err := rows.Scan(
			&row.Profile.PubKey,
			&row.Profile.EventID,
			&row.Profile.CreatedAt,
			&row.Profile.Name,
			&row.Profile.DisplayName,
			&row.Profile.Picture,
			&row.Profile.About,
			&row.Profile.NIP05,
			&row.Profile.LUD16,
			&row.Profile.LUD06,
			&row.Profile.Banner,
			&row.Profile.Website,
			&row.Profile.RawJSON,
			&row.Score,
		); err != nil {
			return nil, err
		}
		row.Rank = row.Score
		out = append(out, row)
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

	// Dedup with LIMIT 1 BY id instead of FINAL: for a ReplacingMergeTree keyed by
	// (kind, created_at, pubkey, id), two rows sharing an id necessarily share the
	// whole sort key (the id is a hash of those fields), so "latest row per id"
	// (max last_seen_at) is identical to FINAL's result — without forcing a
	// read-and-merge of every part. Bounded by the follow set + created_at window.
	rows, err := s.conn.Query(ctx, fmt.Sprintf(`
		SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
		FROM (
			SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
			FROM nostr_events AS e
			WHERE e.pubkey IN (?) AND e.kind IN (1, 6, 16) AND e.created_at < ?
			ORDER BY e.id ASC, e.last_seen_at DESC
			LIMIT 1 BY e.id
		)
		ORDER BY created_at DESC, id DESC
		LIMIT %d OFFSET %d
	`, limit, offset), pubkeys, time.Unix(until, 0).UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanEventRows(rows)
}

func (s *Store) DescendantEvents(ctx context.Context, id string, limit int) (*EventView, []EventView, error) {
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

	where, args := eventWhereInput("e", input)
	if input.Since > 0 {
		where += " AND e.created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		where += " AND e.created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}

	// When the query is bounded by an author set or a tag subquery (e.g. the DM
	// envelopes authored/received reads), dedup with LIMIT 1 BY id instead of
	// FINAL: rows sharing an id share the whole ReplacingMergeTree sort key, so
	// latest-per-id equals FINAL without read-merging every part. Keep FINAL for
	// unbounded kind/search-only scans, where the dedup subquery would have to
	// materialize the whole filtered set before the outer ORDER BY/LIMIT.
	bounded := len(input.PubKeys) > 0 || len(input.Tags) > 0
	var query string
	if bounded {
		orderBy, orderArgs := eventOrderBy("created_at", "id", input.Shuffle)
		args = append(args, orderArgs...)
		args = append(args, input.Limit)
		query = `
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
	} else {
		orderBy, orderArgs := eventOrderBy("e.created_at", "e.id", input.Shuffle)
		args = append(args, orderArgs...)
		args = append(args, input.Limit)
		query = `
			SELECT e.id, e.pubkey, e.kind, e.created_at, e.content, e.tags_json, e.sig, e.last_seen_at
			FROM nostr_events AS e FINAL
			` + where + `
			` + orderBy + `
			LIMIT ?
		`
	}
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
	where, args := eventWhereInput("e", input)
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

	where, args := eventWhereInput("e", input)
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
	if clause, scoreArgs := pubkeyScoreWhere("rt", input.PubkeyScore); clause != "" {
		clauses = append(clauses, clause)
		args = append(args, scoreArgs...)
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

	where, args := eventWhereInput("e", input.Events)
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

	targetWhere, targetArgs := eventWhereInput("target", target)
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
		if clause, scoreArgs := pubkeyScoreWhere("e", input.PubkeyScore); clause != "" {
			spec.where += " AND " + clause
			args = append(args, scoreArgs...)
		}
	case "TAGS", "DERIVED_TAGS":
		spec.from = tagDatasetTable(dataset) + " t INNER JOIN nostr_events AS e FINAL ON e.id = t.event_id"
		spec.where, args = tagWhere(dataset, input.IDs, input.PubKeys, input.Kinds, input.Tags)
		if clause, scoreArgs := pubkeyScoreWhere("t", input.PubkeyScore); clause != "" {
			spec.where += " AND " + clause
			args = append(args, scoreArgs...)
		}
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
	return eventWhereWithExclusions(alias, ids, pubkeys, nil, nil, kinds, tags)
}

func eventWhereInput(alias string, input EventQueryInput) (string, []any) {
	where, args := eventWhereWithExclusions(alias, input.IDs, input.PubKeys, input.ExcludeIDs, input.ExcludePubKeys, input.Kinds, input.Tags)
	if search := strings.TrimSpace(input.Search); search != "" {
		where += fmt.Sprintf(" AND positionCaseInsensitiveUTF8(%s.content, ?) > 0", alias)
		args = append(args, search)
	}
	if clause, scoreArgs := pubkeyScoreWhere(alias, input.PubkeyScore); clause != "" {
		where += " AND " + clause
		args = append(args, scoreArgs...)
	}
	return where, args
}

func pubkeyScoreWhere(alias string, filter PubkeyScoreFilter) (string, []any) {
	source := strings.ToLower(strings.TrimSpace(filter.Source))
	if source == "" {
		return "", nil
	}
	minFollowers := filter.MinFollowers
	clause := fmt.Sprintf(`%s.pubkey IN (
		SELECT pubkey
		FROM vertex_scores
		WHERE source = ?
		GROUP BY pubkey
		HAVING max(followers) >= ?
	)`, alias)
	return clause, []any{source, minFollowers}
}

func eventWhereWithExclusions(alias string, ids, pubkeys, excludeIDs, excludePubkeys []string, kinds []int, tags []TagFilter) (string, []any) {
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
	if len(excludeIDs) > 0 {
		clauses = append(clauses, alias+".id NOT IN (?)")
		args = append(args, excludeIDs)
	}
	if len(excludePubkeys) > 0 {
		clauses = append(clauses, alias+".pubkey NOT IN (?)")
		args = append(args, excludePubkeys)
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
