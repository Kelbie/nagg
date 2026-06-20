package appview

// Server-authoritative ordering manifest. A feed/thread response is rendered
// strictly by Elements (a structural defense against the list reshuffling under
// out-of-order delivery or late enrichment). OrderBy carries the order SEMANTIC
// the client can't infer from the ids alone: "rank" (algorithmic — never merge
// live items above the fold) vs "created_at" (chronological — live items may
// prepend). Matches the nagg-ts OrderingManifest contract.

const (
	orderByRank      = "rank"
	orderByCreatedAt = "created_at"
)

// OrderingManifest is the render order + its semantic.
type OrderingManifest struct {
	OrderBy  string   `json:"orderBy"`
	Elements []string `json:"elements"`
}

// feedItemsOrdering builds the manifest from feed items in their final order.
func feedItemsOrdering(items []FeedItem, orderBy string) OrderingManifest {
	elements := make([]string, 0, len(items))
	for _, item := range items {
		if id := feedItemID(item); id != "" {
			elements = append(elements, id)
		}
	}
	return OrderingManifest{OrderBy: orderBy, Elements: elements}
}

// feedItemID is the stable render key for an item: the note's id, or a repost's
// original (anchor) id. Mirrors the nagg-ts feedItemId so the manifest the server
// emits and the one the client would derive use identical keys.
func feedItemID(item FeedItem) string {
	if item.Type == "repost" {
		if item.OriginalEventID != "" {
			return item.OriginalEventID
		}
		if item.RepostEvent != nil {
			return item.RepostEvent.ID
		}
		return ""
	}
	if item.Event != nil {
		return item.Event.ID
	}
	return ""
}

// eventsOrdering builds the manifest from a flat event list (thread replies).
func eventsOrdering(events []FeedEvent, orderBy string) OrderingManifest {
	elements := make([]string, 0, len(events))
	for _, event := range events {
		elements = append(elements, event.ID)
	}
	return OrderingManifest{OrderBy: orderBy, Elements: elements}
}
