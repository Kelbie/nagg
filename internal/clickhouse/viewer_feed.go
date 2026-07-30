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
// accounts). viewer_feed holds each notification fully denormalized so
// the read is a keyed range scan with no joins.
//
// RecomputeNotificationsFeed is the single writer: a fast incremental tick
// (seconds-scale freshness) that doubles as a progressive historical catch-up
// via the rollup_state watermark, so the table needs NO deploy-time backfill.
// Store.Notifications flips from the legacy query to this table automatically
// once the watermark is close to now (see notificationsFeedReady).

const (
	// Head cursor: the near-now high-water mark, advanced every tick.
	//
	// Both task keys carry an "_ingest" suffix: slices window on
	// viewer_refs.ingested_at (arrival time), not event created_at. The
	// original event-time windows permanently skipped history that arrived
	// late — after a wipe-and-relisten, relay backfills delivered weeks-old
	// likes/replies hours after the backward walker had already passed their
	// created_at windows, so the read-model served follows only. The key
	// bump abandons the old cursors, forcing a fresh walk under the new
	// semantics on deploy.
	notificationsFeedTask = "viewer_feed_ingest"
	// Backfill cursor: walks BACKWARD from the first head slice toward the
	// history target, one slice per tick.
	notificationsFeedBackfillTask = "viewer_feed_ingest_backfill"
	// How far back history extends; matches the table TTL. 14 days: paging
	// deeper is rare, and history size is what blew the disk in the first
	// (denormalized) shape.
	notificationsFeedHistoryWindow = 14 * 24 * time.Hour
	// Steady-state re-process overlap. Slices window on ingested_at (arrival
	// time), so an arrival can only be missed if its row commits to
	// viewer_refs AFTER a tick already read past its ingested_at — the overlap
	// exists to absorb that insert-commit lag (seconds), not late-arriving
	// events (those get a fresh ingested_at and are picked up by the next
	// tick regardless). It used to be 10m, which re-inserted every
	// notification row ~10x across consecutive one-minute ticks: measured
	// live at 389M duplicate rows / 30 GiB inserted into viewer_feed per day,
	// with 162 GiB/day of merge reads deduping them. Vertex-score drift does
	// not need the rewrite either — the page read overlays live scores
	// (notificationsFromFeedQueryTemplate).
	notificationsFeedOverlap = 2 * time.Minute
	// Slice per tick. Kept small on purpose: viewer_refs is sorted
	// by viewer (ingested_at windows cannot prune it) and month-old event_tags
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

// buildNotificationsFeedSQL denormalizes the ARRIVAL window [from, to) —
// viewer_refs rows whose ingested_at falls inside it, whatever their event
// created_at — into viewer_feed. Windowing on arrival is what lets
// late-delivered history (relay backfills, post-wipe relistens) reach the
// read-model; the previous created_at windows lost any event older than the
// slice that had already covered its timestamp. The reply-marker coalesce
// mirrors migration 007 / the legacy read exactly; every subquery is bounded
// to the window's event ids.
func buildNotificationsFeedSQL(from, to, computedAt time.Time) string {
	return fmt.Sprintf(`
		INSERT INTO viewer_feed
			(viewer, created_at, event_id, kind, actor_pubkey, event_pubkey, event_kind, event_created_at, is_ref, target_author, in_viewer_tree, actor_score, computed_at)
		WITH window_candidates AS (
			SELECT viewer, event_id, actor_pubkey, created_at, kind
			FROM viewer_refs
			WHERE ingested_at >= toDateTime(%[1]d) AND ingested_at < toDateTime(%[2]d)
			LIMIT 1 BY viewer, event_id, kind
		),
		-- The candidates' EVENT-time span. An arrival window no longer bounds
		-- created_at, so the event_tags scans below prune on this span instead
		-- (scalar subqueries evaluate to constants before index analysis). A
		-- steady-state slice of live traffic spans minutes; a relay-backfill
		-- slice can span weeks and simply pays for the rows it recovers.
		bounds AS (
			SELECT min(created_at) AS lo, max(created_at) AS hi FROM window_candidates
		),
		candidate_events AS (
			-- NARROW columns only: event bodies are hydrated by id at read
			-- time. Storing content/tags_json per (viewer, event) row was the
			-- disk blow-up (one mention duplicated per p-tagged viewer).
			-- Bounded to the candidates' event-time span: viewer_refs rows
			-- carry their event's created_at, so the span is exact and the
			-- (kind, created_at, ...) primary index prunes what was a full
			-- narrow-column scan (~750 MiB per one-minute tick).
			SELECT id, pubkey, kind, created_at
			FROM nostr_events
			WHERE created_at >= (SELECT lo FROM bounds) AND created_at <= (SELECT hi FROM bounds)
			  AND id IN (SELECT event_id FROM window_candidates)
			ORDER BY id ASC, last_seen_at DESC
			LIMIT 1 BY id
		),
		candidate_tags AS (
			-- Tags OF the window's candidate events. event_tags rows carry
			-- their event's created_at, so the scan is granule-pruned to the
			-- candidates' event-time span instead of walking the whole
			-- tag_key='e' range (the unbounded form read >100M rows per
			-- one-hour slice).
			SELECT
				event_id,
				tag_value,
				tag_index,
				lower(if(length(tag_extra) >= 2, tag_extra[2], '')) AS marker
			FROM event_tags
			WHERE tag_key = 'e' AND length(tag_value) = 64
			  AND created_at >= (SELECT lo FROM bounds) AND created_at <= (SELECT hi FROM bounds)
			  AND event_id IN (SELECT event_id FROM window_candidates)
		),
		reply_meta AS (
			SELECT
				event_id,
				countIf(marker IN ('', 'root', 'reply')) > 0 AS is_ref,
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
			n.kind AS kind,
			n.actor_pubkey AS actor_pubkey,
			e.pubkey AS event_pubkey,
			e.kind AS event_kind,
			e.created_at AS event_created_at,
			toUInt8(ifNull(rm.is_ref, 0)) AS is_ref,
			ifNull(toString(rp.pubkey), '') AS target_author,
			toUInt8(notEmpty(toString(ra.ref_author))) AS in_viewer_tree,
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

// viewerFeedWindowLimit clamps a requested row window. The floor guards the
// zero value; the ceiling exists for the grouped handler's wide candidate
// window (bodyWindow tops out at 600 — see appview.notifications), NOT for
// external callers: the API layers cap user-supplied limits at 100 before the
// input reaches the store. Clamping to 50 here (the old behavior) silently
// shrank the grouped candidate window from 300 to 50 and made its saturation
// signal unreachable.
func viewerFeedWindowLimit(limit uint64) uint64 {
	if limit == 0 {
		return 50
	}
	if limit > 600 {
		return 600
	}
	return limit
}

// notificationsModelPageShort reports whether a read-model page came up short
// of the requested window — the signal that the rest of the viewer's history
// lives beyond the model's retention floor and the request must fall back to
// the legacy full-history read.
func notificationsModelPageShort(rowCount int, limit uint64) bool {
	return rowCount < int(limit)
}

// replyScopeApplies reports whether the DIRECT/THREAD reply-scope filter
// applies to this read. The MENTIONS tab is exempt: its meaning is "kind-1
// events that tag you", and most of those live inside other people's threads —
// the scope filter (built for the ALL tab's reply stream) dropped nearly every
// real mention, leaving the tab empty on nagg while the relay tier showed the
// full set.
func replyScopeApplies(tab, replyScope string) bool {
	if tab == "MENTIONS" {
		return false
	}
	return replyScope == "DIRECT" || replyScope == "THREAD"
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

// notificationsFromFeedQueryTemplate is the read-model page query (verbatim
// except the two %s filter slots), extracted so tests can pin its shape.
//
// actor_score is stored on viewer_feed at denormalization time, but the
// policy filter must see the CURRENT score: baked values freeze whatever the
// vertex graph knew when the slice ran, and history slices never re-run — on
// a young graph that filtered actors out forever, no matter how well they
// scored later. The page's rows (bounded by the inner LIMIT) are therefore
// overlaid with live vertex_scores; the baked value only serves actors with
// no live row.
const notificationsFromFeedQueryTemplate = `
	SELECT
		event_id, kind, actor_score, actor_pubkey, created_at
	FROM (
		SELECT
			*,
			row_number() OVER (
				PARTITION BY kind, actor_pubkey
				ORDER BY created_at ASC, event_id ASC
			) AS actor_kind_rank
		FROM (
			SELECT
				vf.viewer AS viewer, vf.created_at AS created_at,
				vf.event_id AS event_id, vf.kind AS kind,
				vf.actor_pubkey AS actor_pubkey, vf.event_pubkey AS event_pubkey,
				vf.event_kind AS event_kind, vf.event_created_at AS event_created_at,
				vf.is_ref AS is_ref, vf.target_author AS target_author,
				vf.in_viewer_tree AS in_viewer_tree,
				if(empty(sc.pubkey), vf.actor_score, sc.score) AS actor_score
			FROM (
				SELECT
					viewer, created_at, event_id, kind, actor_pubkey,
					event_pubkey, event_kind, event_created_at, is_ref,
					target_author, in_viewer_tree, actor_score
				FROM viewer_feed
				WHERE viewer = ?%s
				ORDER BY created_at DESC, event_id DESC, computed_at DESC
				LIMIT 1 BY event_id, kind
				LIMIT ?
			) AS vf
			LEFT JOIN (
				SELECT pubkey, argMax(score, fetched_at) AS score
				FROM vertex_scores
				WHERE source = 'vertex'
				GROUP BY pubkey
			) AS sc ON sc.pubkey = vf.actor_pubkey
		)
	)
	%s
	ORDER BY created_at DESC, event_id DESC
	LIMIT ?
`

// notificationsFromFeed is the read-model query: a keyed range scan over
// viewer_feed with the same follow-dedupe, reply-scope, and policy
// semantics as the legacy read — but every predicate hits a stored column.
func (s *Store) notificationsFromFeed(ctx context.Context, input ViewerFeedInput, tab, policy, replyScope string) ([]ViewerFeedRow, error) {
	filters := ""
	args := []any{input.Viewer}
	if tab == "MENTIONS" {
		filters += " AND kind = 1"
	}
	if input.Since > 0 {
		filters += " AND created_at >= ?"
		args = append(args, time.Unix(input.Since, 0).UTC())
	}
	if input.Until > 0 {
		filters += " AND created_at < ?"
		args = append(args, time.Unix(input.Until, 0).UTC())
	}
	if len(input.Kinds) > 0 {
		filters += " AND kind IN (?)"
		args = append(args, input.Kinds)
	}
	if len(input.ExcludeKinds) > 0 {
		filters += " AND kind NOT IN (?)"
		args = append(args, input.ExcludeKinds)
	}
	if policy == "FOLLOWS" {
		// Actor must be in the viewer's latest contact list — served by the
		// ingest-maintained latest_k3, not a raw kind-3 read.
		filters += ` AND actor_pubkey IN (
			SELECT arrayJoin(argMax(refs, created_at))
			FROM latest_k3
			WHERE pubkey = ?
		)`
		args = append(args, input.Viewer)
	}

	outer := "WHERE (kind != 3 OR actor_kind_rank = 1)"
	if replyScopeApplies(tab, replyScope) {
		switch replyScope {
		case "DIRECT":
			outer += " AND (event_kind != 1 OR is_ref = 0 OR target_author = viewer)"
		case "THREAD":
			outer += " AND (event_kind != 1 OR is_ref = 0 OR in_viewer_tree = 1)"
		}
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

	query := fmt.Sprintf(notificationsFromFeedQueryTemplate, filters, outer)

	rows, err := s.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	type pageRow struct {
		eventID    string
		kind       uint32
		actorScore float64
		actorPk    string
		createdAt  time.Time
	}
	page := []pageRow{}
	eventIDs := []string{}
	for rows.Next() {
		var r pageRow
		if err := rows.Scan(&r.eventID, &r.kind, &r.actorScore, &r.actorPk, &r.createdAt); err != nil {
			rows.Close()
			return nil, err
		}
		page = append(page, r)
		eventIDs = append(eventIDs, r.eventID)
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return nil, err
	}
	rows.Close()
	if len(page) == 0 {
		return []ViewerFeedRow{}, nil
	}

	// Hydrate event bodies for JUST this page — one bounded IN-list read. The
	// model deliberately stores no content/tags: denormalizing bodies per
	// (viewer, event) row multiplied every mention's content by its p-tag
	// count and filled the disk.
	events, err := s.eventsByID(ctx, eventIDs)
	if err != nil {
		return nil, err
	}

	out := make([]ViewerFeedRow, 0, len(page))
	for _, r := range page {
		event, ok := events[r.eventID]
		if !ok {
			continue
		}
		out = append(out, ViewerFeedRow{
			Event:            event,
			Kind:             int(r.kind),
			ActorVertexScore: r.actorScore,
			ActorPubKey:      r.actorPk,
			RefCreatedAt:     r.createdAt,
		})
	}
	return out, nil
}

// eventsByID fetches full event rows for a bounded id set (page-sized),
// deduping ReplacingMergeTree versions with LIMIT 1 BY id.
func (s *Store) eventsByID(ctx context.Context, ids []string) (map[string]EventView, error) {
	out := make(map[string]EventView, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	rows, err := s.conn.Query(ctx, `
		SELECT id, pubkey, kind, created_at, content, tags_json, sig, last_seen_at
		FROM nostr_events
		WHERE id IN (?)
		ORDER BY id ASC, last_seen_at DESC
		LIMIT 1 BY id
	`, ids)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var ev EventView
		var tagsJSON string
		var kind uint32
		if err := rows.Scan(&ev.ID, &ev.PubKey, &kind, &ev.CreatedAt, &ev.Content, &tagsJSON, &ev.Sig, &ev.UpdatedAt); err != nil {
			return nil, err
		}
		ev.Kind = int(kind)
		_ = json.Unmarshal([]byte(tagsJSON), &ev.Tags)
		out[ev.ID] = ev
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
