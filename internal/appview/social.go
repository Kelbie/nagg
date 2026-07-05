package appview

import (
	"log/slog"
	"net/http"
)

// ReferenceEdge reports the viewer↔candidate latest-kind-3 relationship in
// direction terms: Out = the viewer's latest contact list references the
// candidate; In = the candidate's latest list references the viewer. "Mutual"
// is Out && In — a client derivation, not a server label.
type ReferenceEdge struct {
	Out bool `json:"out"`
	In  bool `json:"in"`
}

// ReferenceEdgesEnvelope is the follow-status response: the envelope base
// plus the per-candidate edge map.
type ReferenceEdgesEnvelope struct {
	Envelope
	Edges map[string]ReferenceEdge `json:"edges"`
}

// followStatus reports, for each candidate, whether the viewer follows them and
// whether they follow the viewer — the REST app-view counterpart of the GraphQL
// followStatus resolver, reusing the same store.FollowEdges round-trip.
func (h *Handler) followStatus(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/follow-status only", http.StatusMethodNotAllowed)
		return
	}
	viewer, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	candidates := normalizePubkeys(csv(r.URL.Query().Get("candidates")))
	if len(candidates) > 500 {
		candidates = candidates[:500]
	}
	edges, err := h.store.FollowEdges(r.Context(), viewer, candidates)
	if err != nil {
		writeError(w, err)
		return
	}
	out := make(map[string]ReferenceEdge, len(candidates))
	for _, candidate := range candidates {
		edge := edges[candidate]
		out[candidate] = ReferenceEdge{Out: edge.Following, In: edge.FollowsYou}
	}
	slog.Info("appview.follow-status", "viewer", viewer, "candidates", len(candidates))
	writeJSON(w, ReferenceEdgesEnvelope{
		Envelope: inlineEnvelope(nil, orderByCreatedAt, nil, nil),
		Edges:    out,
	})
}

// ownProfiles returns metadata + follower/following counts for a small set of
// the viewer's own accounts (capped at 10) — the REST counterpart of the
// GraphQL ownProfiles resolver, reusing store.LatestK0 + BatchPubkeyStats.
// Registered at /nostr/own/profiles, which ServeMux routes to this exact path
// ahead of the /nostr/own/ history subtree.
func (h *Handler) ownProfiles(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET /nostr/own/profiles only", http.StatusMethodNotAllowed)
		return
	}
	pubkeys := normalizePubkeys(csv(r.URL.Query().Get("pubkeys")))
	if len(pubkeys) > 10 {
		pubkeys = pubkeys[:10]
	}
	if len(pubkeys) == 0 {
		writeJSON(w, inlineEnvelope(nil, orderByCreatedAt, nil, nil))
		return
	}
	ctx := r.Context()
	h.tryBackfillProfiles(ctx, pubkeys)
	counts, err := h.store.BatchPubkeyStats(ctx, pubkeys)
	if err != nil {
		writeError(w, err)
		return
	}
	envelope := inlineEnvelope(nil, orderByCreatedAt, nil, nil)
	if err := h.appendK0EventsTo(ctx, &envelope, pubkeys); err != nil {
		writeError(w, err)
		return
	}
	for _, event := range envelope.Events {
		envelope.Order = append(envelope.Order, event.ID)
	}
	for _, pubkey := range pubkeys {
		pubkeyAggregates(&envelope, pubkey, counts[pubkey], 0)
	}
	writeJSON(w, envelope)
}
