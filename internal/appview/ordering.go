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

// eventsOrdering builds the manifest from a flat event list (thread replies).
func eventsOrdering(events []FeedEvent, orderBy string) OrderingManifest {
	elements := make([]string, 0, len(events))
	for _, event := range events {
		elements = append(elements, event.ID)
	}
	return OrderingManifest{OrderBy: orderBy, Elements: elements}
}
