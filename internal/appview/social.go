package appview

import (
	"log/slog"
	"net/http"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// FollowStatusRow is the viewer↔candidate relationship, byte-matching the
// canonical NaggFollowStatusRow the GraphQL followStatus distilled to.
type FollowStatusRow struct {
	PubKey       string `json:"pubkey"`
	Following    bool   `json:"following"`
	FollowsYou   bool   `json:"followsYou"`
	Mutual       bool   `json:"mutual"`
	Relationship string `json:"relationship"`
}

// FollowStatusResponse wraps the rows under `followStatus` so the REST body IS
// the canonical NaggFollowStatusData shape (one parser, both transports).
type FollowStatusResponse struct {
	FollowStatus []FollowStatusRow `json:"followStatus"`
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
	rows := make([]FollowStatusRow, 0, len(candidates))
	for _, candidate := range candidates {
		rows = append(rows, followStatusRow(candidate, edges[candidate]))
	}
	slog.Info("appview.follow-status", "viewer", viewer, "candidates", len(candidates))
	writeJSON(w, FollowStatusResponse{FollowStatus: rows})
}

// followStatusRow derives the relationship label, matching the GraphQL resolver.
func followStatusRow(pubkey string, edge chstore.FollowEdge) FollowStatusRow {
	mutual := edge.Following && edge.FollowsYou
	relationship := "none"
	switch {
	case mutual:
		relationship = "mutual"
	case edge.Following:
		relationship = "following"
	case edge.FollowsYou:
		relationship = "follows_you"
	}
	return FollowStatusRow{
		PubKey:       pubkey,
		Following:    edge.Following,
		FollowsYou:   edge.FollowsYou,
		Mutual:       mutual,
		Relationship: relationship,
	}
}

// OwnProfile is metadata plus follow counts for one of the viewer's own
// accounts, byte-matching the canonical NaggOwnProfile.
type OwnProfile struct {
	PubKey      string `json:"pubkey"`
	Name        string `json:"name"`
	DisplayName string `json:"displayName"`
	Picture     string `json:"picture"`
	About       string `json:"about"`
	NIP05       string `json:"nip05"`
	LUD16       string `json:"lud16"`
	Banner      string `json:"banner"`
	Website     string `json:"website"`
	Followers   int    `json:"followers"`
	Follows     int    `json:"follows"`
	CreatedAt   *int64 `json:"createdAt"`
}

// OwnProfilesResponse wraps the rows under `ownProfiles` to match the canonical
// NaggOwnProfilesData shape.
type OwnProfilesResponse struct {
	OwnProfiles []OwnProfile `json:"ownProfiles"`
}

// ownProfiles returns metadata + follower/following counts for a small set of
// the viewer's own accounts (capped at 10) — the REST counterpart of the
// GraphQL ownProfiles resolver, reusing store.LatestProfiles + BatchFollowCounts.
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
		writeJSON(w, OwnProfilesResponse{OwnProfiles: []OwnProfile{}})
		return
	}
	ctx := r.Context()
	h.tryBackfillProfiles(ctx, pubkeys)
	profiles, err := h.store.LatestProfiles(ctx, pubkeys)
	if err != nil {
		writeError(w, err)
		return
	}
	counts, err := h.store.BatchFollowCounts(ctx, pubkeys)
	if err != nil {
		writeError(w, err)
		return
	}
	out := make([]OwnProfile, 0, len(pubkeys))
	for _, pubkey := range pubkeys {
		profile := profiles[pubkey]
		count := counts[pubkey]
		out = append(out, OwnProfile{
			PubKey:      pubkey,
			Name:        profile.Name,
			DisplayName: profile.DisplayName,
			Picture:     profile.Picture,
			About:       profile.About,
			NIP05:       profile.NIP05,
			LUD16:       profile.LUD16,
			Banner:      profile.Banner,
			Website:     profile.Website,
			Followers:   int(count.Followers),
			Follows:     int(count.Follows),
			CreatedAt:   unixPtr(profile.CreatedAt),
		})
	}
	writeJSON(w, OwnProfilesResponse{OwnProfiles: out})
}
