package clickhouse

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// Notifications read-model (docs/notifications-performance.md §9).
//
// The legacy Notifications read derived reply markers, parent authors, and
// viewer-thread references per request over event_tags + FINAL joins — the
// heaviest sanctioned read on the instance (multi-second cold for engaged
// accounts). notifications_feed holds each notification fully denormalized so
// the read is a keyed range scan with no joins.
//
// RecomputeNotificationsFeed is the single writer: a fast incremental tick
// (seconds-scale freshness) that doubles as a progressive historical catch-up
// via the rollup_state watermark, so the table needs NO deploy-time backfill.
// Store.Notifications flips from the legacy query to this table automatically
// once the watermark is close to now (see notificationsFeedReady).

const (
	// Head cursor: the near-now high-water mark, advanced every tick.
	notificationsFeedTask = "notifications_feed"
	// Backfill cursor: walks BACKWARD from the first head slice toward the
	// 30-day history target, one slice per tick.
	notificationsFeedBackfillTask = "notifications_feed_backfill"
	// How far back history extends; matches the table TTL.
	notificationsFeedHistoryWindow = 30 * 24 * time.Hour
	// Steady-state re-process overlap: late-arriving events for a window and
	// vertex-score drift converge on rewrite (ReplacingMergeTree by computed_at).
	notificationsFeedOverlap = 10 * time.Minute
	// Slice per tick. Kept small on purpose: notification_candidates is sorted
	// by viewer (created_at windows cannot prune it) and month-old event_tags
	// granules are cold — the original 6h forward slices ran ~5 MINUTES and
	// died on the ~300s infra connection ceiling (CH 394 client-cancel).
	notificationsFeedSlice = time.Hour
	// The read path flips to the model once the head is at most this far
	// behind now …
	notificationsFeedReadyLag = 30 * time.Minute
	// … AND the backward cursor has covered at least this much history —
	// enough for virtually all notification page views; deeper history keeps
	// filling in behind. Bootstrap condition, self-clearing.
	notificationsFeedReadyHistory = 72 * time.Hour
)

// RecomputeNotificationsFeed keeps the read-model current NEWEST-FIRST: the
// head slice (recent, warm data — cheap) runs every tick so reads can flip on
// quickly after a fresh deploy, and one backward history slice follows per
// tick until the 30-day target is reached. Returns whether both cursors are
// done for this tick (steady state).
func (s *Store) RecomputeNotificationsFeed(ctx context.Context, now time.Time) (bool, error) {
	head, err := s.LoadRollupState(ctx, notificationsFeedTask)
	if err != nil {
		return false, err
	}

	// Head slice: [last head − overlap, now). On the very first run cover one
	// slice back from now; history is the backfill walker's job.
	headFrom := head.CursorCreatedAt.Add(-notificationsFeedOverlap)
	firstRun := head.CursorCreatedAt.IsZero()
	if firstRun {
		headFrom = now.Add(-notificationsFeedSlice)
	}
	if err := s.execRollup(ctx, "recompute notifications feed head", buildNotificationsFeedSQL(headFrom, now, now)); err != nil {
		return false, err
	}
	if err := s.SaveRollupState(ctx, RollupState{
		Task:            notificationsFeedTask,
		CursorCreatedAt: now,
		LastRunAt:       now,
		Processed:       head.Processed + 1,
	}); err != nil {
		return false, err
	}

	// Backward history slice.
	backfill, err := s.LoadRollupState(ctx, notificationsFeedBackfillTask)
	if err != nil {
		return false, err
	}
	edge := backfill.CursorCreatedAt
	if edge.IsZero() {
		if !firstRun {
			// Pre-two-cursor deployments have a head but no backfill cursor;
			// resume history from the oldest slice the head design covered.
			edge = head.CursorCreatedAt
		} else {
			edge = headFrom
		}
	}
	target := now.Add(-notificationsFeedHistoryWindow)
	if !edge.After(target) {
		return true, nil // history complete — steady state
	}
	sliceFrom := edge.Add(-notificationsFeedSlice)
	if sliceFrom.Before(target) {
		sliceFrom = target
	}
	if err := s.execRollup(ctx, "recompute notifications feed history", buildNotificationsFeedSQL(sliceFrom, edge, now)); err != nil {
		return false, err
	}
	if err := s.SaveRollupState(ctx, RollupState{
		Task:            notificationsFeedBackfillTask,
		CursorCreatedAt: sliceFrom,
		LastRunAt:       now,
		Processed:       backfill.Processed + 1,
	}); err != nil {
		return false, err
	}
	return !sliceFrom.After(target), nil
}

// buildNotificationsFeedSQL denormalizes the candidate window [from, to) into
// notifications_feed. The reply-marker coalesce mirrors migration 007 /
// the legacy read exactly; every subquery is bounded to the window's event ids.
func buildNotificationsFeedSQL(from, to, computedAt time.Time) string {
	return fmt.Sprintf(`
		INSERT INTO notifications_feed
			(viewer, created_at, event_id, reason, actor_pubkey, event_pubkey, event_kind, event_created_at, content, tags_json, sig, event_last_seen_at, is_reply, direct_parent_author, replies_viewer_thread, actor_score, computed_at)
		WITH window_candidates AS (
			SELECT viewer, event_id, actor_pubkey, created_at, reason
			FROM notification_candidates
			WHERE created_at >= toDateTime(%[1]d) AND created_at < toDateTime(%[2]d)
			LIMIT 1 BY viewer, event_id, reason
		),
		candidate_events AS (
			SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
			FROM nostr_events
			WHERE id IN (SELECT event_id FROM window_candidates)
			ORDER BY id ASC, last_seen_at DESC
			LIMIT 1 BY id
		),
		candidate_tags AS (
			-- Tags OF the window's candidate events. event_tags rows carry
			-- their event's created_at, so the scan is granule-pruned to the
			-- window instead of walking the whole tag_key='e' range (the
			-- unbounded form read >100M rows per one-hour slice).
			SELECT
				event_id,
				tag_value,
				tag_index,
				lower(if(length(tag_extra) >= 2, tag_extra[2], '')) AS marker
			FROM event_tags
			WHERE tag_key = 'e' AND length(tag_value) = 64
			  AND created_at >= toDateTime(%[1]d) AND created_at < toDateTime(%[2]d)
			  AND event_id IN (SELECT event_id FROM window_candidates)
		),
		reply_meta AS (
			SELECT
				event_id,
				countIf(marker IN ('', 'root', 'reply')) > 0 AS is_reply,
				coalesce(
					nullIf(argMinIf(tag_value, tag_index, marker = 'reply'), ''),
					nullIf(argMaxIf(tag_value, tag_index, marker = ''), ''),
					nullIf(argMinIf(tag_value, tag_index, marker = 'root'), '')
				) AS direct_parent_id
			FROM candidate_tags
			GROUP BY event_id
		),
		referenced_events AS (
			SELECT id, pubkey
			FROM nostr_events
			WHERE id IN (SELECT tag_value FROM candidate_tags)
			ORDER BY id ASC, last_seen_at DESC
			LIMIT 1 BY id
		),
		event_ref_authors AS (
			SELECT rt.event_id AS event_id, referenced.pubkey AS ref_author
			FROM candidate_tags AS rt
			INNER JOIN referenced_events AS referenced ON referenced.id = rt.tag_value
			WHERE rt.marker IN ('', 'root', 'reply')
			GROUP BY rt.event_id, referenced.pubkey
		),
		actor_scores AS (
			SELECT pubkey, argMax(score, fetched_at) AS score
			FROM vertex_scores
			WHERE source = 'vertex' AND pubkey IN (SELECT actor_pubkey FROM window_candidates)
			GROUP BY pubkey
		)
		SELECT
			n.viewer AS viewer,
			n.created_at AS created_at,
			n.event_id AS event_id,
			n.reason AS reason,
			n.actor_pubkey AS actor_pubkey,
			e.pubkey AS event_pubkey,
			e.kind AS event_kind,
			e.created_at AS event_created_at,
			e.content AS content,
			e.tags_json AS tags_json,
			e.sig AS sig,
			e.last_seen_at AS event_last_seen_at,
			toUInt8(ifNull(rm.is_reply, 0)) AS is_reply,
			ifNull(toString(rp.pubkey), '') AS direct_parent_author,
			toUInt8(notEmpty(toString(ra.ref_author))) AS replies_viewer_thread,
			ifNull(sc.score, 0) AS actor_score,
			toDateTime(%[3]d) AS computed_at
		FROM window_candidates AS n
		INNER JOIN candidate_events AS e ON e.id = n.event_id
		LEFT JOIN reply_meta AS rm ON rm.event_id = n.event_id
		LEFT JOIN referenced_events AS rp ON rp.id = rm.direct_parent_id
		LEFT JOIN event_ref_authors AS ra ON ra.event_id = n.event_id AND ra.ref_author = n.viewer
		LEFT JOIN actor_scores AS sc ON sc.pubkey = n.actor_pubkey
		-- max_execution_time 240 (below the rollup default of 600): the
		-- nagg<->ClickHouse connection is killed by infrastructure at ~300s,
		-- so a statement allowed to run longer dies as a client-cancel
		-- (CH 394) AFTER doing all its work. Fail fast and clean instead.
		SETTINGS max_execution_time = 240
	`, from.Unix(), to.Unix(), computedAt.Unix())
}

// notificationsFeedReady reports whether the read-model can serve reads: the
// head watermark is fresh AND the backward history walker has covered the
// window users actually page. Cached briefly so the hot path doesn't query
// rollup_state per request.
func (s *Store) notificationsFeedReady(ctx context.Context) bool {
	s.feedReadyMu.Lock()
	defer s.feedReadyMu.Unlock()
	if time.Since(s.feedReadyCheckedAt) < 30*time.Second {
		return s.feedReady
	}
	head, headErr := s.LoadRollupState(ctx, notificationsFeedTask)
	backfill, backErr := s.LoadRollupState(ctx, notificationsFeedBackfillTask)
	ready := headErr == nil && backErr == nil &&
		!head.CursorCreatedAt.IsZero() &&
		time.Since(head.CursorCreatedAt) < notificationsFeedReadyLag &&
		!backfill.CursorCreatedAt.IsZero() &&
		time.Since(backfill.CursorCreatedAt) >= notificationsFeedReadyHistory
	s.feedReady = ready
	s.feedReadyCheckedAt = time.Now()
	return ready
}

// notificationsFromFeed is the read-model query: a keyed range scan over
// notifications_feed with the same follow-dedupe, reply-scope, and policy
// semantics as the legacy read — but every predicate hits a stored column.
func (s *Store) notificationsFromFeed(ctx context.Context, input NotificationInput, tab, policy, replyScope string) ([]NotificationRow, error) {
	filters := ""
	args := []any{input.Viewer}
	if tab == "MENTIONS" {
		filters += " AND reason = 'mention'"
	}
	if input.Since > 0 {
		filters += " AND created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		filters += " AND created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}
	if len(input.Reasons) > 0 {
		filters += " AND reason IN (?)"
		args = append(args, input.Reasons)
	}
	if len(input.ExcludeReasons) > 0 {
		filters += " AND reason NOT IN (?)"
		args = append(args, input.ExcludeReasons)
	}
	if policy == "FOLLOWS" {
		// Actor must be in the viewer's latest contact list — served by the
		// ingest-maintained user_contacts_latest, not a raw kind-3 read.
		filters += ` AND actor_pubkey IN (
			SELECT arrayJoin(argMax(contacts, created_at))
			FROM user_contacts_latest
			WHERE pubkey = ?
		)`
		args = append(args, input.Viewer)
	}

	outer := "WHERE (reason != 'follow' OR actor_reason_rank = 1)"
	switch replyScope {
	case "DIRECT":
		outer += " AND (event_kind != 1 OR is_reply = 0 OR direct_parent_author = viewer)"
	case "THREAD":
		outer += " AND (event_kind != 1 OR is_reply = 0 OR replies_viewer_thread = 1)"
	}

	// Policy: actor_score >= actorThreshold OR viewer_score >= viewerThreshold.
	// The viewer's score is one cheap point lookup done here in Go; when it
	// clears the viewer threshold the actor filter is moot.
	outerArgs := []any{}
	actorThreshold, viewerThreshold := notificationPolicyThresholds(policy)
	if actorThreshold > 0 || viewerThreshold > 0 {
		viewerScore, err := s.latestVertexScore(ctx, input.Viewer)
		if err != nil {
			return nil, err
		}
		if !(viewerThreshold > 0 && viewerScore >= viewerThreshold) {
			outer += " AND actor_score >= ?"
			outerArgs = append(outerArgs, actorThreshold)
		}
	}

	// Overfetch before the outer filters, mirroring the legacy read.
	overfetch := input.Limit * 8
	if overfetch < 200 {
		overfetch = 200
	}
	args = append(args, overfetch)
	args = append(args, outerArgs...)
	args = append(args, input.Limit)

	query := fmt.Sprintf(`
		SELECT
			event_id, event_pubkey, event_kind, event_created_at, content,
			tags_json, sig, event_last_seen_at, reason, actor_score,
			actor_pubkey, created_at
		FROM (
			SELECT
				*,
				row_number() OVER (
					PARTITION BY reason, actor_pubkey
					ORDER BY created_at ASC, event_id ASC
				) AS actor_reason_rank
			FROM (
				SELECT
					viewer, created_at, event_id, reason, actor_pubkey,
					event_pubkey, event_kind, event_created_at, content,
					tags_json, sig, event_last_seen_at, is_reply,
					direct_parent_author, replies_viewer_thread, actor_score
				FROM notifications_feed
				WHERE viewer = ?%s
				ORDER BY created_at DESC, event_id DESC, computed_at DESC
				LIMIT 1 BY event_id, reason
				LIMIT ?
			)
		)
		%s
		ORDER BY created_at DESC, event_id DESC
		LIMIT ?
	`, filters, outer)

	rows, err := s.conn.Query(ctx, query, args...)
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
			&row.ActorPubKey,
			&row.NotificationCreatedAt,
		); err != nil {
			return nil, err
		}
		row.Event.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &row.Event.Tags)
		row.Reason = notificationReasonForEvent(row.Event, row.Reason)
		out = append(out, row)
	}
	return out, rows.Err()
}

// latestVertexScore returns the viewer's most recent vertex score (0 when
// unknown).
func (s *Store) latestVertexScore(ctx context.Context, pubkey string) (float64, error) {
	rows, err := s.conn.Query(ctx, `
		SELECT argMax(score, fetched_at)
		FROM vertex_scores
		WHERE source = 'vertex' AND pubkey = ?
		GROUP BY pubkey
	`, pubkey)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, rows.Err()
	}
	var score float64
	if err := rows.Scan(&score); err != nil {
		return 0, err
	}
	return score, rows.Err()
}
