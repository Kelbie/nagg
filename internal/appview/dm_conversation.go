package appview

import (
	"log/slog"
	"net/http"

	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// dmConversation is the REST app-view counterpart of the GraphQL dmConversation
// resolver: the viewer's DM events optionally scoped to one counterparty. Legacy
// NIP-04 (kind 4) is scoped to the pair when a counterparty is given; gift wraps
// (and other opaque kinds) are returned as the full viewer inbox because nagg
// can't see inside them. nagg never decrypts — the client does. Mirrors
// resolver.dmConversation so both transports surface the same conversation.
func (h *Handler) dmConversation(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet && r.Method != http.MethodPost {
		http.Error(w, "GET or POST /nostr/dm/conversation only", http.StatusMethodNotAllowed)
		return
	}
	viewer, err := h.viewerPubkeyOr(queryViewerParam(r))
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	counterparty := ""
	if raw := normalizePubkeys([]string{r.URL.Query().Get("counterparty")}); len(raw) == 1 {
		counterparty = raw[0]
	}
	kinds := parseDmKinds(r.URL.Query().Get("kinds"))
	limit := clampDmLimit(intParam(r, "limit", 50))
	until := int64(intParam(r, "until", 0))

	h.tryBackfillDMEnvelopes(r.Context(), viewer, kinds, until, uint64(limit))

	// kind 4 can be scoped to the pair; gift wraps are opaque → full viewer inbox.
	var directKinds, opaqueKinds []int
	for _, k := range kinds {
		if k == 4 {
			directKinds = append(directKinds, k)
		} else {
			opaqueKinds = append(opaqueKinds, k)
		}
	}

	ctx := r.Context()
	var collected []chstore.EventView
	if len(directKinds) > 0 {
		if counterparty != "" {
			sent, e := h.store.QueryEvents(ctx, chstore.EventQueryInput{
				PubKeys: []string{viewer}, Kinds: directKinds,
				Tags: []chstore.TagFilter{{Key: "p", Value: counterparty}}, Until: until, Limit: uint64(limit),
			})
			if e != nil {
				writeError(w, e)
				return
			}
			got, e := h.store.QueryEvents(ctx, chstore.EventQueryInput{
				PubKeys: []string{counterparty}, Kinds: directKinds,
				Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Until: until, Limit: uint64(limit),
			})
			if e != nil {
				writeError(w, e)
				return
			}
			collected = append(collected, sent...)
			collected = append(collected, got...)
		} else {
			sent, e := h.store.QueryEvents(ctx, chstore.EventQueryInput{
				PubKeys: []string{viewer}, Kinds: directKinds, Until: until, Limit: uint64(limit),
			})
			if e != nil {
				writeError(w, e)
				return
			}
			got, e := h.store.QueryEvents(ctx, chstore.EventQueryInput{
				Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Kinds: directKinds, Until: until, Limit: uint64(limit),
			})
			if e != nil {
				writeError(w, e)
				return
			}
			collected = append(collected, sent...)
			collected = append(collected, got...)
		}
	}
	if len(opaqueKinds) > 0 {
		wraps, e := h.store.QueryEvents(ctx, chstore.EventQueryInput{
			Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Kinds: opaqueKinds, Until: until, Limit: uint64(limit),
		})
		if e != nil {
			writeError(w, e)
			return
		}
		collected = append(collected, wraps...)
	}

	merged := mergeDmEnvelopes(limit, collected)
	slog.Info("appview.dm.conversation", "viewer", viewer, "scoped", counterparty != "", "results", len(merged))
	// PRIVACY: served bare, like dmEnvelopes — never enrich DM authors.
	order := make([]string, 0, len(merged))
	for _, event := range merged {
		order = append(order, event.ID)
	}
	writeJSON(w, inlineEnvelope(order, orderByCreatedAt, merged, eventEndCursor(merged)))
}
