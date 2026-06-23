package appview

import (
	"encoding/json"
	"log/slog"
	"net/http"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// EventsQueryRequest is the constrained filter for POST /nostr/events/query. It
// is deliberately NOT the full GraphQL EventQueryInput — it exposes only the
// fields the app-view clients need (kinds + author/tag scoping + a time window),
// so the endpoint stays a deep, bounded query rather than an arbitrary
// passthrough. At least one of Kinds/Authors/IDs/Tags must be set.
type EventsQueryRequest struct {
	IDs     []string         `json:"ids"`
	Authors []string         `json:"authors"`
	Kinds   []int            `json:"kinds"`
	Tags    []EventTagFilter `json:"tags"`
	Since   int64            `json:"since"`
	Until   int64            `json:"until"`
	Limit   int              `json:"limit"`
}

// EventTagFilter is a single tag constraint (e.g. {key:"e", values:[id]}).
type EventTagFilter struct {
	Key    string   `json:"key"`
	Values []string `json:"values"`
}

// EventsConnection is the raw-event connection envelope shared by the
// filtered-query shapes, matching the canonical NaggEventConnection.
type EventsConnection struct {
	Nodes    []chstore.EventView `json:"nodes"`
	PageInfo PageInfo            `json:"pageInfo"`
}

// EventsQueryResponse wraps the connection under `events` to match the canonical
// NaggWhitenoiseEventsData / NaggPostsRecentData shapes.
type EventsQueryResponse struct {
	Events EventsConnection `json:"events"`
}

const eventsQueryMaxLimit = 500

// eventsQuery serves a constrained, filtered event query: it backs the niche
// app-view paths (Whitenoise group messages/invites, wallpaper catalog, recent
// posts-by-pubkeys) that need kind/tag/author filtering the purpose-built feed
// endpoints don't expose. Returns raw events (the client decrypts/interprets);
// no metrics/profile hydration, mirroring the GraphQL `events` query these paths
// used. Author-list filtering for posts-by-pubkeys with engagement uses the
// dedicated feed endpoints instead.
func (h *Handler) eventsQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST /nostr/events/query only", http.StatusMethodNotAllowed)
		return
	}
	var req EventsQueryRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid JSON body", http.StatusBadRequest)
		return
	}
	input, ok := req.toQueryInput()
	if !ok {
		http.Error(w, "at least one of ids/authors/kinds/tags is required", http.StatusBadRequest)
		return
	}
	events, err := h.store.QueryEvents(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}
	slog.Info("appview.events.query", "kinds", req.Kinds, "authors", len(req.Authors), "limit", int(input.Limit), "results", len(events))
	writeJSON(w, EventsQueryResponse{Events: EventsConnection{
		Nodes:    events,
		PageInfo: PageInfo{HasNextPage: len(events) >= int(input.Limit), EndCursor: eventEndCursor(events)},
	}})
}

// toQueryInput normalizes and bounds the request into a store query, returning
// false when no constraint is provided (an unbounded scan is rejected).
func (r EventsQueryRequest) toQueryInput() (chstore.EventQueryInput, bool) {
	ids := normalizeHexIDs(r.IDs)
	authors := normalizePubkeys(r.Authors)
	tags := make([]chstore.TagFilter, 0, len(r.Tags))
	for _, t := range r.Tags {
		if t.Key == "" || len(t.Values) == 0 {
			continue
		}
		if len(t.Values) == 1 {
			tags = append(tags, chstore.TagFilter{Key: t.Key, Value: t.Values[0]})
		} else {
			tags = append(tags, chstore.TagFilter{Key: t.Key, Values: t.Values})
		}
	}
	if len(ids) == 0 && len(authors) == 0 && len(r.Kinds) == 0 && len(tags) == 0 {
		return chstore.EventQueryInput{}, false
	}
	limit := r.Limit
	if limit <= 0 || limit > eventsQueryMaxLimit {
		limit = eventsQueryMaxLimit
	}
	return chstore.EventQueryInput{
		IDs:     ids,
		PubKeys: authors,
		Kinds:   r.Kinds,
		Tags:    tags,
		Since:   r.Since,
		Until:   r.Until,
		Limit:   uint64(limit),
	}, true
}
