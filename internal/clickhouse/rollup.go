package clickhouse

import (
	"context"
	"fmt"
	"strings"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

// rollupExecCtx caps the thread budget for the rollup's heavy INSERT…SELECTs.
// Managed ClickHouse defaults max_threads to auto(N) (e.g. 32); these multi-join
// recomputes would otherwise try to spawn that many worker threads and fail on a
// small, thread-constrained server with error 439 ("failed to start the thread").
// max_execution_time bounds a runaway tick.
func rollupExecCtx(ctx context.Context) context.Context {
	return ch.Context(ctx, ch.WithSettings(ch.Settings{
		"max_threads":        2,
		"max_insert_threads": 1,
		"max_execution_time": 600,
	}))
}

// isRetryableConnErr reports whether a rollup statement failed on a transient
// native-connection fault (the prod ClickHouse resets :9000 connections under
// load — the same fault that breaks concurrent reads; see read.go NoteStats).
// These surface BEFORE the query reaches the server, so a retry on a fresh
// pooled connection succeeds.
func isRetryableConnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "connection reset by peer") ||
		strings.Contains(msg, "broken pipe") ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "use of closed network connection")
}

// execRollup runs one rollup statement, retrying on transient connection resets
// with a small linear backoff (each retry acquires a fresh pooled connection).
// Rollup INSERTs are idempotent (ReplacingMergeTree / uniqState), so a retry is
// always safe even if a prior attempt partially landed.
func (s *Store) execRollup(ctx context.Context, label, sql string) error {
	const maxAttempts = 4
	var err error
	for attempt := 1; attempt <= maxAttempts; attempt++ {
		if err = s.conn.Exec(rollupExecCtx(ctx), sql); err == nil {
			return nil
		}
		if ctx.Err() != nil || !isRetryableConnErr(err) {
			return fmt.Errorf("%s: %w", label, err)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(attempt) * 3 * time.Second):
		}
	}
	return fmt.Errorf("%s after %d attempts: %w", label, maxAttempts, err)
}

// Thresholds parameterize the vertex-real engagement rollup. An engagement actor
// (liker / reposter / replier / quoter / zapper) counts toward a "real" count iff
// their latest saved Vertex score is >= MinActorScore. Version is stamped into the
// threshold_version column so a threshold change lands as a new logical row rather
// than silently mutating history.
type Thresholds struct {
	MinActorScore float64
	Version       string
}

// RollupState is the persisted cursor for a rollup task (mirrors enrichment_state).
type RollupState struct {
	Task            string
	CursorCreatedAt time.Time
	LastRunAt       time.Time
	Processed       uint64
}

// sanitizeVersion keeps threshold_version to a safe literal (it is formatted
// directly into SQL). The value is config-owned, never request input.
func sanitizeVersion(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		v = "v1"
	}
	var b strings.Builder
	for _, r := range v {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '-', r == '_':
			b.WriteRune(r)
		}
	}
	if b.Len() == 0 {
		return "v1"
	}
	return b.String()
}

// LoadRollupState returns the persisted cursor for task, or a zero-value state
// (with Task set) when none exists yet.
func (s *Store) LoadRollupState(ctx context.Context, task string) (RollupState, error) {
	task = strings.TrimSpace(task)
	if task == "" {
		return RollupState{}, fmt.Errorf("rollup task is required")
	}
	rows, err := s.conn.Query(ctx, `
		SELECT task, cursor_created_at, last_run_at, processed
		FROM rollup_state FINAL
		WHERE task = ?
		LIMIT 1
	`, task)
	if err != nil {
		return RollupState{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		return RollupState{Task: task}, rows.Err()
	}
	var st RollupState
	if err := rows.Scan(&st.Task, &st.CursorCreatedAt, &st.LastRunAt, &st.Processed); err != nil {
		return RollupState{}, err
	}
	return st, rows.Err()
}

// SaveRollupState upserts the cursor for a rollup task.
func (s *Store) SaveRollupState(ctx context.Context, st RollupState) error {
	if strings.TrimSpace(st.Task) == "" {
		return fmt.Errorf("rollup task is required")
	}
	return s.conn.Exec(ctx, `
		INSERT INTO rollup_state (task, cursor_created_at, last_run_at, processed)
		VALUES (?, ?, ?, ?)
	`, st.Task, st.CursorCreatedAt, st.LastRunAt, st.Processed)
}

// recentChildrenSubquery is the bounded set of recent reply candidates (kind 1 /
// 1111) used to keep the direct-reply edge/count rebuild proportional to new data.
func recentChildrenSubquery(since time.Time, limit int) string {
	return fmt.Sprintf(`
		SELECT id FROM nostr_events
		WHERE kind IN (1, 1111) AND created_at >= toDateTime(%d)
		ORDER BY created_at DESC
		LIMIT %d`, since.Unix(), limit)
}

// targetIDsSubquery is the bounded set of notes whose engagement changed since
// `since`: notes that received a reaction/repost/reply/quote (e-tag references)
// or a zap. Ordered by MOST RECENT engagement and capped at `limit`, so the
// feature rebuild always covers the freshest engaged notes — the ones For-You
// ranks — rather than an arbitrary slice of the window.
//
// It deliberately does NOT enumerate every recent post (that was ~1.7M rows/48h,
// too heavy to materialize each tick AND, with a bare LIMIT, it grabbed an
// arbitrary OLD slice that excluded recent notes). For-You ranks engaged notes,
// so a post with no engagement yet does not need a feature row; it gets one on
// the next tick once it is engaged.
func targetIDsSubquery(since time.Time, limit int) string {
	return fmt.Sprintf(`
		SELECT event_id FROM (
			SELECT event_id, max(engaged_at) AS engaged_at FROM (
				SELECT tag_value AS event_id, created_at AS engaged_at FROM event_tags
				WHERE tag_key = 'e' AND length(tag_value) = 64
				  AND created_at >= toDateTime(%d) AND kind IN (1, 6, 7, 16, 1111)
				UNION ALL
				SELECT target AS event_id, created_at AS engaged_at FROM event_refs WHERE rule = 'k9735_e'
				WHERE created_at >= toDateTime(%d)
			)
			GROUP BY event_id
			ORDER BY engaged_at DESC
			LIMIT %d
		)`, since.Unix(), since.Unix(), limit)
}

// scoredActorsSubquery is the set of pubkeys whose latest Vertex score clears the
// real-engagement threshold.
func scoredActorsSubquery(minScore float64) string {
	return fmt.Sprintf(`
		SELECT pubkey FROM (
			SELECT pubkey, argMax(score, fetched_at) AS sc
			FROM vertex_scores
			WHERE source = 'vertex'
			GROUP BY pubkey
		) WHERE sc >= %v`, minScore)
}

func buildRefEdgesSQL(since time.Time, limit int) string {
	children := recentChildrenSubquery(since, limit)
	return fmt.Sprintf(`
		INSERT INTO ref_edges
		SELECT source_id, target_id, source_pubkey, kind, created_at
		FROM (
			SELECT
				event_id AS source_id,
				any(pubkey) AS source_pubkey,
				any(kind) AS kind,
				any(created_at) AS created_at,
				coalesce(
					nullIf(argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'reply'), ''),
					nullIf(argMaxIf(tag_value, tag_index, tag_key = 'e' AND marker = ''), ''),
					nullIf(argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'root'), '')
				) AS target_id,
				groupArrayIf(tag_value, tag_key = 'q') AS quote_targets
			FROM (
				SELECT event_id, pubkey, kind, created_at, tag_key, tag_value, tag_index,
					lower(if(length(tag_extra) >= 2, tag_extra[2], '')) AS marker
				FROM event_tags
				WHERE kind IN (1, 1111) AND tag_key IN ('e', 'q') AND length(tag_value) = 64
				  AND event_id IN (%s)
			)
			GROUP BY event_id
		)
		WHERE target_id != '' AND length(target_id) = 64 AND NOT has(quote_targets, target_id)`, children)
}

func buildRefSourceCountsSQL(since time.Time, limit int) string {
	children := recentChildrenSubquery(since, limit)
	return fmt.Sprintf(`
		INSERT INTO agg_k1_1111_e_reply
		SELECT target_id AS target, uniqState(source_id) AS sources
		FROM ref_edges
		WHERE source_id IN (%s)
		GROUP BY target_id`, children)
}

// RecomputeRefEdges rebuilds the direct-reply edges and counts for recent reply
// candidates. uniqState makes the count rebuild idempotent (set union).
func (s *Store) RecomputeRefEdges(ctx context.Context, since time.Time, limit int) error {
	if err := s.execRollup(ctx, "recompute reply edges", buildRefEdgesSQL(since, limit)); err != nil {
		return err
	}
	return s.execRollup(ctx, "recompute direct reply counts", buildRefSourceCountsSQL(since, limit))
}

func buildGatedRefCountsSQL(since time.Time, limit int, th Thresholds, computedAt time.Time) string {
	targets := targetIDsSubquery(since, limit)
	scored := scoredActorsSubquery(th.MinActorScore)
	version := sanitizeVersion(th.Version)
	// Each metric subquery counts only scored actors and excludes the target's
	// own author (self-engagement). The author per target comes from `authors`.
	// Explicit column list: actors was added to gated_ref_counts via a
	// later ALTER ADD COLUMN, so it is physically LAST in the table while the SELECT
	// emits it mid-row. Without this list the positional INSERT shifts every column
	// after it (threshold_version → computed_at), failing with a DateTime parse
	// error (code 41) on every tick — which is why the feature table went stale.
	return fmt.Sprintf(`
		INSERT INTO gated_ref_counts
			(event_id, k7_e_actors, k6_16_e_actors, k1_1111_e_reply_sources, k1_q_sources, k9735_e_sources, k9735_e_value_total, actors, threshold_version, computed_at)
		WITH
			target_ids AS (%s),
			scored AS (%s),
			authors AS (SELECT id AS event_id, pubkey AS author FROM nostr_events FINAL WHERE id IN (target_ids))
		SELECT
			a.event_id,
			ifNull(l.c, 0)  AS k7_e_actors,
			ifNull(rp.c, 0) AS k6_16_e_actors,
			ifNull(re.c, 0) AS k1_1111_e_reply_sources,
			ifNull(q.c, 0)  AS k1_q_sources,
			ifNull(z.cnt, 0)  AS k9735_e_sources,
			ifNull(z.sats, 0) AS k9735_e_value_total,
			ifNull(act.c, 0)  AS actors,
			'%s' AS threshold_version,
			toDateTime(%d) AS computed_at
		FROM authors a
		LEFT JOIN (
			SELECT et.tag_value AS event_id, uniqExactIf(et.pubkey, et.pubkey != a2.author) AS c
			FROM event_tags et
			INNER JOIN authors a2 ON a2.event_id = et.tag_value
			WHERE et.kind = 7 AND et.tag_key = 'e' AND length(et.tag_value) = 64 AND et.pubkey IN (scored)
			GROUP BY event_id
		) l ON l.event_id = a.event_id
		LEFT JOIN (
			SELECT et.tag_value AS event_id, uniqExactIf(et.pubkey, et.pubkey != a2.author) AS c
			FROM event_tags et
			INNER JOIN authors a2 ON a2.event_id = et.tag_value
			WHERE et.kind IN (6, 16) AND et.tag_key = 'e' AND length(et.tag_value) = 64 AND et.pubkey IN (scored)
			GROUP BY event_id
		) rp ON rp.event_id = a.event_id
		LEFT JOIN (
			SELECT e.target_id AS event_id, uniqExactIf(e.source_pubkey, e.source_pubkey != a2.author) AS c
			FROM ref_edges e
			INNER JOIN authors a2 ON a2.event_id = e.target_id
			WHERE e.source_pubkey IN (scored)
			GROUP BY event_id
		) re ON re.event_id = a.event_id
		LEFT JOIN (
			SELECT et.tag_value AS event_id, uniqExactIf(et.pubkey, et.pubkey != a2.author) AS c
			FROM event_tags et
			INNER JOIN authors a2 ON a2.event_id = et.tag_value
			WHERE et.kind = 1 AND et.tag_key = 'q' AND length(et.tag_value) = 64 AND et.pubkey IN (scored)
			GROUP BY event_id
		) q ON q.event_id = a.event_id
		LEFT JOIN (
			SELECT z.target AS event_id,
			       uniqExact(z.pubkey) AS cnt,
			       sum(z.value) AS sats
			FROM event_refs z
			WHERE z.rule = 'k9735_e' AND z.target IN (target_ids) AND z.pubkey IN (scored)
			GROUP BY event_id
		) z ON z.event_id = a.event_id
		LEFT JOIN (
			-- Distinct scored, non-self engagers across every reaction type — the
			-- "actors" signal For-You ranks on.
			SELECT event_id, uniqExact(actor) AS c
			FROM (
				SELECT et.tag_value AS event_id, et.pubkey AS actor
				FROM event_tags et INNER JOIN authors a2 ON a2.event_id = et.tag_value
				WHERE et.kind IN (6, 7, 16) AND et.tag_key = 'e' AND length(et.tag_value) = 64
				  AND et.pubkey IN (scored) AND et.pubkey != a2.author
				UNION ALL
				SELECT et.tag_value AS event_id, et.pubkey AS actor
				FROM event_tags et INNER JOIN authors a2 ON a2.event_id = et.tag_value
				WHERE et.kind = 1 AND et.tag_key = 'q' AND length(et.tag_value) = 64
				  AND et.pubkey IN (scored) AND et.pubkey != a2.author
				UNION ALL
				SELECT e.target_id AS event_id, e.source_pubkey AS actor
				FROM ref_edges e INNER JOIN authors a2 ON a2.event_id = e.target_id
				WHERE e.source_pubkey IN (scored) AND e.source_pubkey != a2.author
				UNION ALL
				SELECT zz.target AS event_id, zz.pubkey AS actor
				FROM event_refs zz INNER JOIN authors a2 ON a2.event_id = zz.target
				WHERE zz.rule = 'k9735_e' AND zz.pubkey IN (scored) AND zz.pubkey != a2.author
			)
			GROUP BY event_id
		) act ON act.event_id = a.event_id`,
		targets, scored, version, computedAt.Unix())
}

// RecomputeGatedRefCounts recomputes vertex-real engagement counts for the bounded
// target set. ReplacingMergeTree(computed_at) overwrites, so this is idempotent.
func (s *Store) RecomputeGatedRefCounts(ctx context.Context, since time.Time, limit int, th Thresholds, computedAt time.Time) error {
	return s.execRollup(ctx, "recompute engagement real", buildGatedRefCountsSQL(since, limit, th, computedAt))
}

func touchedAuthorsSubquery(since time.Time, limit int) string {
	return fmt.Sprintf(`
		SELECT DISTINCT pubkey FROM nostr_events
		WHERE created_at >= toDateTime(%d) AND kind IN (1, 3, 1111)
		LIMIT %d`, since.Unix(), limit)
}

func buildPubkeyStatsSQL(since time.Time, limit int, computedAt time.Time) string {
	touched := touchedAuthorsSubquery(since, limit)
	// k3_out = size of the pubkey's latest kind-3 reference list; k1_1111_authored
	// = uniqMerge of the author rule aggregate; k3_in = fan-in over every
	// pubkey's LATEST kind-3 list. The fan-in scans all latest lists (anyone may
	// reference a touched pubkey) but only emits rows for touched pubkeys.
	return fmt.Sprintf(`
		INSERT INTO pubkey_stats
		WITH touched AS (%s)
		SELECT
			u.pubkey,
			length(u.refs) AS k3_out,
			ifNull(f.k3_in, 0) AS k3_in,
			ifNull(p.authored, 0) AS k1_1111_authored,
			toDateTime(%d) AS computed_at
		FROM (
			SELECT pubkey, argMax(refs, created_at) AS refs
			FROM latest_k3
			WHERE pubkey IN (touched)
			GROUP BY pubkey
		) u
		LEFT JOIN (
			SELECT ref AS pubkey, count() AS k3_in
			FROM (
				SELECT arrayJoin(refs) AS ref
				FROM latest_k3 FINAL
			)
			WHERE ref IN (touched)
			GROUP BY ref
		) f ON f.pubkey = u.pubkey
		LEFT JOIN (
			SELECT target AS pubkey, uniqMerge(sources) AS authored
			FROM agg_k1_1111_author
			WHERE target IN (touched)
			GROUP BY target
		) p ON p.pubkey = u.pubkey`, touched, computedAt.Unix())
}

// RecomputePubkeyStats recomputes the per-pubkey kind-3 in/out and authored
// counts for authors touched since `since`. Fixes the legacy fan-in bug (which
// counted all kind-3 history instead of only the latest list per referencer).
func (s *Store) RecomputePubkeyStats(ctx context.Context, since time.Time, limit int, computedAt time.Time) error {
	return s.execRollup(ctx, "recompute user stats", buildPubkeyStatsSQL(since, limit, computedAt))
}

func buildRankFeaturesSQL(since time.Time, limit int, th Thresholds, computedAt time.Time) string {
	targets := targetIDsSubquery(since, limit)
	version := sanitizeVersion(th.Version)
	// Explicit column list — see buildGatedRefCountsSQL: gated_actors is physically
	// last (ALTER-appended) but emitted mid-row, so a positional INSERT misaligns
	// every following column and fails. Mapping by name is order-independent.
	return fmt.Sprintf(`
		INSERT INTO rank_features
			(event_id, pubkey, kind, created_at, k7_e_actors, k6_16_e_actors, k1_1111_e_reply_sources, k1_q_sources, k9735_e_sources, k9735_e_value_total, gated_k7_e_actors, gated_k6_16_e_actors, gated_k1_1111_e_reply_sources, gated_k1_q_sources, gated_k9735_e_sources, gated_k9735_e_value_total, gated_actors, author_score, author_followers, contribution_quality, threshold_version, computed_at)
		WITH target_ids AS (%s)
		SELECT
			n.id AS event_id,
			n.pubkey,
			n.kind,
			n.created_at,
			ifNull(lk.v, 0) AS k7_e_actors,
			ifNull(rp.v, 0) AS k6_16_e_actors,
			ifNull(re.v, 0) AS k1_1111_e_reply_sources,
			ifNull(qt.v, 0) AS k1_q_sources,
			ifNull(zt.zaps, 0) AS k9735_e_sources,
			ifNull(zt.sats, 0) AS k9735_e_value_total,
			ifNull(er.gated_k7_e_actors, 0) AS gated_k7_e_actors,
			ifNull(er.gated_k6_16_e_actors, 0) AS gated_k6_16_e_actors,
			ifNull(er.gated_k1_1111_e_reply_sources, 0) AS gated_k1_1111_e_reply_sources,
			ifNull(er.gated_k1_q_sources, 0) AS gated_k1_q_sources,
			ifNull(er.gated_k9735_e_sources, 0) AS gated_k9735_e_sources,
			ifNull(er.gated_k9735_e_value_total, 0) AS gated_k9735_e_value_total,
			ifNull(er.gated_actors, 0) AS gated_actors,
			ifNull(vs.score, 0) AS author_score,
			ifNull(vs.followers, 0) AS author_followers,
			ifNull(dm.value, 0) AS contribution_quality,
			'%s' AS threshold_version,
			toDateTime(%d) AS computed_at
		FROM (SELECT id, pubkey, kind, created_at FROM nostr_events FINAL WHERE id IN (target_ids)) n
		LEFT JOIN (SELECT target AS id, uniqMerge(actors) AS v FROM agg_k7_e WHERE target IN (target_ids) GROUP BY id) lk ON lk.id = n.id
		LEFT JOIN (SELECT target AS id, uniqMerge(actors) AS v FROM agg_k6_16_e WHERE target IN (target_ids) GROUP BY id) rp ON rp.id = n.id
		LEFT JOIN (SELECT target AS id, uniqMerge(sources) AS v FROM agg_k1_1111_e_reply WHERE target IN (target_ids) GROUP BY id) re ON re.id = n.id
		LEFT JOIN (SELECT target AS id, uniqMerge(sources) AS v FROM agg_k1_q WHERE target IN (target_ids) GROUP BY id) qt ON qt.id = n.id
		LEFT JOIN (SELECT target AS id, sumMerge(value_total) AS sats, uniqMerge(sources) AS zaps FROM agg_k9735_e WHERE target IN (target_ids) GROUP BY id) zt ON zt.id = n.id
		LEFT JOIN (SELECT event_id AS id, argMax(k7_e_actors, computed_at) AS gated_k7_e_actors, argMax(k6_16_e_actors, computed_at) AS gated_k6_16_e_actors, argMax(k1_1111_e_reply_sources, computed_at) AS gated_k1_1111_e_reply_sources, argMax(k1_q_sources, computed_at) AS gated_k1_q_sources, argMax(k9735_e_sources, computed_at) AS gated_k9735_e_sources, argMax(k9735_e_value_total, computed_at) AS gated_k9735_e_value_total, argMax(actors, computed_at) AS gated_actors FROM gated_ref_counts WHERE event_id IN (target_ids) GROUP BY id) er ON er.id = n.id
		LEFT JOIN (SELECT pubkey, argMax(score, fetched_at) AS score, argMax(followers, fetched_at) AS followers FROM vertex_scores WHERE source = 'vertex' GROUP BY pubkey) vs ON vs.pubkey = n.pubkey
		LEFT JOIN (SELECT event_id AS id, argMax(value, computed_at) AS value FROM derived_metrics WHERE metric = 'contribution_quality' AND event_id IN (target_ids) GROUP BY id) dm ON dm.id = n.id`,
		targets, version, computedAt.Unix())
}

// RecomputeRankFeatures assembles the per-event hot-path feature row (raw + real
// counts, author score, contribution quality) for the bounded target set.
func (s *Store) RecomputeRankFeatures(ctx context.Context, since time.Time, limit int, th Thresholds, computedAt time.Time) error {
	return s.execRollup(ctx, "recompute rank features", buildRankFeaturesSQL(since, limit, th, computedAt))
}
