package appview

import (
	"net/http"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// Social graph: contacts / profiles / relay-lists in one bundled response — the
// "fetch all my follows, with their profiles and my relay/mute lists" seed the
// nagg-ts facade's nagg tier targets. Bundling the follow profiles inline (one
// LatestProfiles call) avoids the client fanning out N kind-0 fetches on cold
// start.
//
// Reads the viewer's latest kind-3 (follows), kind-10002 (NIP-65 relay list),
// and kind-10000 (mutes) over the existing QueryEvents store method.

const (
	kindContacts  = 3
	kindRelayList = 10002
	kindMuteList  = 10000
)

// RelayListEntry is one NIP-65 relay with its read/write direction.
type RelayListEntry struct {
	URL   string `json:"url"`
	Read  bool   `json:"read"`
	Write bool   `json:"write"`
}

// SocialGraphResponse matches the nagg-ts SocialGraphResponseSchema.
type SocialGraphResponse struct {
	Pubkey   string                 `json:"pubkey"`
	Follows  []string               `json:"follows"`
	Profiles map[string]ProfileInfo `json:"profiles"`
	Relays   []RelayListEntry       `json:"relays"`
	Mutes    []string               `json:"mutes"`
}

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

	latest := latestEventByKind(events)
	follows := pTagValues(latest[kindContacts])
	mutes := pTagValues(latest[kindMuteList])
	relays := parseRelayList(latest[kindRelayList])

	profiles, err := h.profileInfos(r.Context(), follows)
	if err != nil {
		writeError(w, err)
		return
	}

	writeJSON(w, SocialGraphResponse{
		Pubkey:   pubkey,
		Follows:  follows,
		Profiles: profiles,
		Relays:   relays,
		Mutes:    mutes,
	})
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

func parseRelayList(event *chstore.EventView) []RelayListEntry {
	if event == nil {
		return []RelayListEntry{}
	}
	out := []RelayListEntry{}
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "r" || tag[1] == "" {
			continue
		}
		marker := ""
		if len(tag) >= 3 {
			marker = tag[2]
		}
		// NIP-65: no marker → read+write; "read"/"write" → that direction only.
		out = append(out, RelayListEntry{
			URL:   tag[1],
			Read:  marker != "write",
			Write: marker != "read",
		})
	}
	return out
}
