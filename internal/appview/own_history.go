package appview

import (
	"net/http"
	"strings"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// Own viewer-state history: the user's OWN action events for one action type,
// paged lazily — the three-tier-resolvable seed the nagg-ts facade's nagg tier
// targets (ADR-0003). nagg is the only tier that covers every action type.
//
// Served as a subtree GET /nostr/own/{type}; the type is the trailing path
// segment. Author-keyed types read over the existing QueryEvents store method
// (authors=[me], kinds per type) — no migration. A by-author materialized view
// is a paging-performance optimization, not a correctness prerequisite.

// ownActionKinds maps an action type to the Nostr kinds that back it. The bool
// reports whether the type is recognized. zaps-sent is intentionally absent: a
// zap RECEIPT (9735) is authored by the LNURL service, not the sender, so it
// can't be listed by author — that path is served by the Primal tier (which has
// the user_zaps_sent verb), and a note_zaps-backed nagg implementation is a
// follow-up.
func ownActionKinds(actionType string) ([]int, bool) {
	switch actionType {
	case "authored", "replies":
		return []int{1, 1111}, true
	case "likes":
		return []int{7}, true
	case "reposts":
		return []int{6, 16}, true
	case "bookmarks":
		return []int{10003}, true
	case "follows":
		return []int{3}, true
	case "mutes":
		return []int{10000}, true
	case "relays":
		return []int{10002}, true
	default:
		return nil, false
	}
}

func (h *Handler) ownHistory(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/own/{type} only", http.StatusMethodNotAllowed)
		return
	}
	actionType := ownActionTypeFromPath(r.URL.Path)
	if actionType == "zaps-sent" {
		// Served by the Primal tier; a note_zaps-backed nagg path is a follow-up.
		http.Error(w, "zaps-sent is not served by nagg yet", http.StatusNotImplemented)
		return
	}
	kinds, ok := ownActionKinds(actionType)
	if !ok {
		http.Error(w, "unknown own action type: "+actionType, http.StatusBadRequest)
		return
	}
	pubkey, err := h.viewerPubkeyOr(r.URL.Query().Get("pubkey"))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	input := chstore.EventQueryInput{
		PubKeys: []string{pubkey},
		Kinds:   kinds,
		Limit:   uint64(intParam(r, "limit", 100)),
	}
	if until := int64(intParam(r, "until", 0)); until > 0 {
		input.Until = until
	}
	// NIP-01 until is inclusive, so exclude the cursor's own event to avoid
	// re-returning the boundary item at the top of the next page.
	if cursorID := strings.TrimSpace(r.URL.Query().Get("cursorId")); cursorID != "" {
		input.ExcludeIDs = []string{cursorID}
	}

	events, err := h.store.QueryEvents(r.Context(), input)
	if err != nil {
		writeError(w, err)
		return
	}

	// authored vs replies share kind 1; split on the presence of an `e` reference
	// (a reply marks the note it answers). The post-filter is approximate at page
	// boundaries — acceptable for a lazy own-history scroll-back.
	events = filterOwnByActionType(events, actionType)

	order := make([]string, 0, len(events))
	for _, event := range events {
		order = append(order, event.ID)
	}
	envelope, err := h.assembleEnvelope(r.Context(), order, orderByCreatedAt, events, eventEndCursor(events))
	if err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, envelope)
}

func ownActionTypeFromPath(path string) string {
	const marker = "/own/"
	idx := strings.LastIndex(path, marker)
	if idx < 0 {
		return ""
	}
	return strings.Trim(path[idx+len(marker):], "/")
}

func filterOwnByActionType(events []chstore.EventView, actionType string) []chstore.EventView {
	if actionType != "authored" && actionType != "replies" {
		return events
	}
	wantReply := actionType == "replies"
	out := make([]chstore.EventView, 0, len(events))
	for _, event := range events {
		if hasETag(event.Tags) == wantReply {
			out = append(out, event)
		}
	}
	return out
}

func hasETag(tags [][]string) bool {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == "e" {
			return true
		}
	}
	return false
}
