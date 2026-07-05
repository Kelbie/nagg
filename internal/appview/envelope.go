package appview

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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

	profileEvents, err := h.profileEvents(ctx, pubkeys)
	if err != nil {
		return Envelope{}, err
	}
	for _, event := range profileEvents {
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

// profileEvents returns each author's latest kind-0 event, reconstructed from
// the profiles projection — hydration is just more events in the envelope.
func (h *Handler) profileEvents(ctx context.Context, pubkeys []string) ([]FeedEvent, error) {
	pubkeys = normalizePubkeys(pubkeys)
	if len(pubkeys) == 0 {
		return nil, nil
	}
	rows, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	if missing := missingProfiles(pubkeys, rows); len(missing) > 0 && h.tryBackfillProfiles(ctx, missing) {
		refreshed, err := h.store.LatestProfiles(ctx, missing)
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

// parseFeedCursor decodes the feed continuation token ("<until>|<offset>").
// A missing or malformed cursor means the first page.
func parseFeedCursor(raw string) (until int64, offset uint64) {
	parts := strings.SplitN(strings.TrimSpace(raw), "|", 2)
	if len(parts) != 2 {
		return 0, 0
	}
	u, err1 := strconv.ParseInt(parts[0], 10, 64)
	o, err2 := strconv.ParseUint(parts[1], 10, 64)
	if err1 != nil || err2 != nil {
		return 0, 0
	}
	return u, o
}

func (h *Handler) writeFeedEnvelope(w http.ResponseWriter, r *http.Request, events []chstore.EventView, orderBy string) {
	response, err := h.feedEnvelope(r.Context(), events, orderBy)
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, response)
}
