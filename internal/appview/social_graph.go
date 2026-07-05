package appview

import (
	"net/http"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// Social graph: contacts / profiles / relay-lists in one bundled response — the
// "fetch all my follows, with their profiles and my relay/mute lists" seed the
// nagg-ts facade's nagg tier targets. Bundling the follow profiles inline (one
// LatestK0 call) avoids the client fanning out N kind-0 fetches on cold
// start.
//
// Reads the viewer's latest kind-3 (follows), kind-10002 (NIP-65 relay list),
// and kind-10000 (mutes) over the existing QueryEvents store method.

const (
	kindContacts  = 3
	kindRelayList = 10002
	kindMuteList  = 10000
)

func (h *Handler) socialGraph(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/social-graph only", http.StatusMethodNotAllowed)
		return
	}
	pubkey, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, err := h.store.QueryEvents(r.Context(), chstore.EventQueryInput{
		PubKeys: []string{pubkey},
		Kinds:   []int{kindContacts, kindRelayList, kindMuteList},
	})
	if err != nil {
		writeError(w, err)
		return
	}

	// The lists ARE events: the latest kind-3 (references), kind-10002
	// (NIP-65 relays), and kind-10000 (mutes) go in the envelope and the
	// client reads their tags; the referenced pubkeys' kind-0 profile events
	// ride along so cold start needs no per-profile fan-out.
	latest := latestEventByKind(events)
	order := make([]string, 0, 3)
	referenced := make([]chstore.EventView, 0, 3)
	for _, kind := range []int{kindContacts, kindRelayList, kindMuteList} {
		if event := latest[kind]; event != nil {
			order = append(order, event.ID)
			referenced = append(referenced, *event)
		}
	}
	envelope := inlineEnvelope(order, orderByCreatedAt, referenced, nil)
	if err := h.appendK0EventsTo(r.Context(), &envelope, pTagValues(latest[kindContacts])); err != nil {
		writeError(w, err)
		return
	}
	writeJSON(w, envelope)
}

// latestEventByKind keeps the newest event per kind (replaceable events: the
// viewer's current list), so stale historical copies can't override it.
func latestEventByKind(events []chstore.EventView) map[int]*chstore.EventView {
	latest := map[int]*chstore.EventView{}
	for i := range events {
		event := &events[i]
		cur, ok := latest[event.Kind]
		if !ok || event.CreatedAt.After(cur.CreatedAt) ||
			(event.CreatedAt.Equal(cur.CreatedAt) && event.ID < cur.ID) {
			latest[event.Kind] = event
		}
	}
	return latest
}

func pTagValues(event *chstore.EventView) []string {
	if event == nil {
		return []string{}
	}
	out := []string{}
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "p" && tag[1] != "" {
			out = append(out, tag[1])
		}
	}
	return out
}
