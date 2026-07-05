package appview

import (
	"context"
	"fmt"
	"net/http"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// Envelope is the one generic app-view response shape. Every route returns
// it: the server stays terminology-agnostic (events, kinds, declared
// aggregations) and the client reconstructs concepts — a "repost" is a
// kind-6/16 event in Events whose e reference is also present; a profile is
// a kind-0 event; counts are whatever aggregation rules the server declares.
//
//   - Order: the server-authoritative render order. Elements are anchor
//     event ids (for kind-6/16 entries the referenced event's id, matching
//     the previous ordering-manifest keying) and OrderBy tells the client
//     whether live items may prepend ("created_at") or not ("rank").
//   - Events: every event the response references — the ordered items plus
//     hydration (referenced originals/roots/quoted events, and each author's
//     kind-0 profile event). Deduplicated by id, raw Nostr shape.
//   - Aggregates: target id → declared rule name → metric name → value,
//     straight from the rule registry (plus vertex_-prefixed provider
//     values until the DVM plugin seam formalizes them).
//   - Cursor: opaque pagination token; echo it back to fetch the next page.
type Envelope struct {
	Order      []string                                `json:"order"`
	OrderBy    string                                  `json:"orderBy"`
	Events     []FeedEvent                             `json:"events"`
	Aggregates map[string]map[string]map[string]uint64 `json:"aggregates"`
	Cursor     *string                                 `json:"cursor,omitempty"`
}

// assembleEnvelope builds the response for an already-ordered set: it dedupes
// the referenced events, pulls every quoted event, fetches the declared
// aggregates for all embedded event ids, and appends each author's kind-0
// profile event. Callers supply Order/OrderBy/Cursor semantics.
func (h *Handler) assembleEnvelope(ctx context.Context, order []string, orderBy string, referenced []chstore.EventView, cursor *string) (Envelope, error) {
	events := make([]FeedEvent, 0, len(referenced))
	ids := make([]string, 0, len(referenced))
	pubkeys := make([]string, 0, len(referenced))
	quotedIDs := make([]string, 0)
	seenEvents := map[string]struct{}{}
	seenPubkeys := map[string]struct{}{}
	seenQuoted := map[string]struct{}{}

	appendEvent := func(event chstore.EventView) {
		if _, ok := seenEvents[event.ID]; ok {
			return
		}
		seenEvents[event.ID] = struct{}{}
		events = append(events, eventJSON(event))
		ids = append(ids, event.ID)
		pubkeys = appendUniqueString(pubkeys, seenPubkeys, event.PubKey)
	}

	for _, event := range referenced {
		appendEvent(event)
		for _, id := range quotedEventIDs(event) {
			quotedIDs = appendUniqueString(quotedIDs, seenQuoted, id)
		}
	}

	quoted, err := h.eventsByID(ctx, quotedIDs)
	if err != nil {
		return Envelope{}, err
	}
	for _, event := range quoted {
		appendEvent(event)
	}

	h.tryBackfillEnrichment(ctx, ids, pubkeys)

	var aggregates map[string]map[string]map[string]uint64
	err = recordPhase(ctx, "aggregates", func() (e error) {
		aggregates, e = h.store.EventAggregates(ctx, ids)
		return
	})
	if err != nil {
		return Envelope{}, err
	}

	latestK0Events, err := h.latestK0Events(ctx, pubkeys)
	if err != nil {
		return Envelope{}, err
	}
	for _, event := range latestK0Events {
		if _, ok := seenEvents[event.ID]; ok {
			continue
		}
		seenEvents[event.ID] = struct{}{}
		events = append(events, event)
	}

	if aggregates == nil {
		aggregates = map[string]map[string]map[string]uint64{}
	}
	return Envelope{
		Order:      order,
		OrderBy:    orderBy,
		Events:     events,
		Aggregates: aggregates,
		Cursor:     cursor,
	}, nil
}

// latestK0Events returns each author's latest kind-0 event, reconstructed from
// the profiles projection — hydration is just more events in the envelope.
func (h *Handler) latestK0Events(ctx context.Context, pubkeys []string) ([]FeedEvent, error) {
	pubkeys = normalizePubkeys(pubkeys)
	if len(pubkeys) == 0 {
		return nil, nil
	}
	rows, err := h.store.LatestK0(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	if missing := missingProfiles(pubkeys, rows); len(missing) > 0 && h.tryBackfillProfiles(ctx, missing) {
		refreshed, err := h.store.LatestK0(ctx, missing)
		if err != nil {
			return nil, err
		}
		for pubkey, row := range refreshed {
			rows[pubkey] = row
		}
	}
	out := make([]FeedEvent, 0, len(rows))
	for pubkey, row := range rows {
		if row.EventID == "" || row.RawJSON == "" {
			continue
		}
		out = append(out, FeedEvent{
			ID:        row.EventID,
			Kind:      0,
			PubKey:    pubkey,
			Content:   row.RawJSON,
			Tags:      [][]string{},
			CreatedAt: row.CreatedAt.Unix(),
		})
	}
	return out, nil
}

// feedEnvelope converts an ordered feed page into the envelope: kind-6/16
// entries anchor on their referenced event (fetched and embedded alongside),
// every entry's root is resolved and embedded, and the cursor encodes the
// until|offset continuation.
func (h *Handler) feedEnvelope(ctx context.Context, feedEvents []chstore.EventView, orderBy string) (Envelope, error) {
	originalIDs := make([]string, 0)
	for _, event := range feedEvents {
		if event.Kind == 6 || event.Kind == 16 {
			if id := firstEventTag(event); id != "" {
				originalIDs = append(originalIDs, id)
			}
		}
	}
	originals, err := h.eventsByID(ctx, originalIDs)
	if err != nil {
		return Envelope{}, err
	}

	rootSources := make([]chstore.EventView, 0, len(feedEvents)+len(originals))
	rootSources = append(rootSources, feedEvents...)
	for _, original := range originals {
		rootSources = append(rootSources, original)
	}
	roots, err := h.rootEvents(ctx, rootSources)
	if err != nil {
		return Envelope{}, err
	}

	order := make([]string, 0, len(feedEvents))
	seenOrder := map[string]struct{}{}
	referenced := make([]chstore.EventView, 0, len(feedEvents)+len(originals)+len(roots))
	var oldest int64

	for _, event := range feedEvents {
		if oldest == 0 || event.CreatedAt.Unix() < oldest {
			oldest = event.CreatedAt.Unix()
		}
		referenced = append(referenced, event)

		anchor := event.ID
		if event.Kind == 6 || event.Kind == 16 {
			if id := firstEventTag(event); id != "" {
				anchor = id
			}
			if original, ok := originals[anchor]; ok {
				referenced = append(referenced, original)
				if root, ok := roots[original.ID]; ok && root.HasEvent {
					referenced = append(referenced, root.Event)
				}
			}
		} else if root, ok := roots[event.ID]; ok && root.HasEvent {
			referenced = append(referenced, root.Event)
		}
		order = appendUniqueString(order, seenOrder, anchor)
	}

	var cursor *string
	if len(feedEvents) > 0 {
		c := fmt.Sprintf("%d|%d", oldest, len(feedEvents))
		cursor = &c
	}
	return h.assembleEnvelope(ctx, order, orderBy, referenced, cursor)
}

func (h *Handler) writeFeedEnvelope(w http.ResponseWriter, r *http.Request, events []chstore.EventView, orderBy string) {
	response, err := h.feedEnvelope(r.Context(), events, orderBy)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, response)
}

// inlineEnvelope builds an Envelope with no hydration and no aggregates — the
// privacy paths (gift wraps, DMs) use it so ephemeral authors are never
// enriched, and pubkey-centric routes use it as the base they extend.
func inlineEnvelope(order []string, orderBy string, events []chstore.EventView, cursor *string) Envelope {
	feedEvents := make([]FeedEvent, 0, len(events))
	seen := map[string]struct{}{}
	for _, event := range events {
		if _, ok := seen[event.ID]; ok {
			continue
		}
		seen[event.ID] = struct{}{}
		feedEvents = append(feedEvents, eventJSON(event))
	}
	if order == nil {
		order = []string{}
	}
	return Envelope{
		Order:      order,
		OrderBy:    orderBy,
		Events:     feedEvents,
		Aggregates: map[string]map[string]map[string]uint64{},
		Cursor:     cursor,
	}
}

// appendK0EventsTo merges the latest kind-0 events for pubkeys into an
// envelope, deduplicating by event id.
func (h *Handler) appendK0EventsTo(ctx context.Context, env *Envelope, pubkeys []string) error {
	if len(pubkeys) == 0 {
		return nil
	}
	latestK0Events, err := h.latestK0Events(ctx, pubkeys)
	if err != nil {
		return err
	}
	seen := make(map[string]struct{}, len(env.Events))
	for _, event := range env.Events {
		seen[event.ID] = struct{}{}
	}
	for _, event := range latestK0Events {
		if _, ok := seen[event.ID]; ok {
			continue
		}
		seen[event.ID] = struct{}{}
		env.Events = append(env.Events, event)
	}
	return nil
}

// setPubkeyAggregate records one pubkey-keyed aggregate value on an envelope,
// omitting zeros (matching the event-aggregate read's zero omission).
func setPubkeyAggregate(env *Envelope, pubkey, rule, metric string, value uint64) {
	if value == 0 || pubkey == "" {
		return
	}
	if env.Aggregates == nil {
		env.Aggregates = map[string]map[string]map[string]uint64{}
	}
	if env.Aggregates[pubkey] == nil {
		env.Aggregates[pubkey] = map[string]map[string]uint64{}
	}
	if env.Aggregates[pubkey][rule] == nil {
		env.Aggregates[pubkey][rule] = map[string]uint64{}
	}
	env.Aggregates[pubkey][rule][metric] = value
}

// pubkeyAggregates records a pubkey's relationship counts under the kind-based
// rule names: followers = latest kind-3 lists referencing the pubkey
// (k3_p_latest.actors), following = the pubkey's own latest kind-3 list size
// (k3_author_latest.sources), created events = k1_1111_author.sources.
func pubkeyAggregates(env *Envelope, pubkey string, counts chstore.PubkeyStats, created uint64) {
	setPubkeyAggregate(env, pubkey, "k3_p_latest", "actors", counts.Followers)
	setPubkeyAggregate(env, pubkey, "k3_author_latest", "sources", counts.Follows)
	setPubkeyAggregate(env, pubkey, "k1_1111_author", "sources", created)
}
