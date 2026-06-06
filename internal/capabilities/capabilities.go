package capabilities

import (
	"net/http"
	"strings"
)

const (
	GraphQLSchemaVersion = "2026-06-06"
	AppViewVersion       = "v1"
)

var Names = []string{
	"graphql.events",
	"graphql.relayHydration",
	"graphql.events.search",
	"graphql.aggregateEvents",
	"graphql.rankedEvents",
	"graphql.references",
	"graphql.selectedReferences",
	"graphql.referencedBy",
	"graphql.rankedReferencedBy",
	"graphql.authoredReplyChain",
	"graphql.aggregateReferencedBy",
	"graphql.pubkeyEvents",
	"graphql.pubkeysFrom.latestEventTags",
	"graphql.pubkeysFrom.sourceEventAuthor",
	"graphql.events.shuffle",
	"graphql.events.excludeIdsPubkeys",
	"graphql.aggregateEvents.shuffle",
	"graphql.rank.weightedTerms",
	"graphql.rank.candidatePubkeyBoosts",
	"graphql.rank.pubkeyScoreTerms",
	"graphql.rank.candidateFieldTerms",
	"graphql.rank.derivedMetricTerms",
	"graphql.rank.shuffle",
	"graphql.tags.derivedDataset",
	"graphql.tags.excludeValues",
	"graphql.notifications",
	"graphql.profileSearch",
	"graphql.dmEnvelopes",
	"graphql.dmConversation",
	"graphql.followStatus",
	"graphql.ownProfiles",
	"appview.dmEnvelopes",
	"appview.relayHydration",
	"appview.v1",
}

var AppViewRoutes = []string{
	"/nostr/capabilities",
	"/nostr/feed",
	"/nostr/feed/user",
	"/nostr/notes/stats",
	"/nostr/thread",
	"/nostr/follows",
	"/nostr/events",
	"/nostr/dm/envelopes",
	"/nostr/profiles",
	"/nostr/profile",
	"/nostr/search",
	"/nostr/recommended",
}

func ServiceInfo() map[string]any {
	return map[string]any{
		"graphqlSchemaVersion": GraphQLSchemaVersion,
		"appViewVersion":       AppViewVersion,
		"capabilities":         append([]string(nil), Names...),
		"appViews": []map[string]any{{
			"version": AppViewVersion,
			"routes":  append([]string(nil), AppViewRoutes...),
		}},
	}
}

func HeaderValue() string {
	return strings.Join(Names, ",")
}

func WriteHeaders(w http.ResponseWriter) {
	w.Header().Set("X-Nagg-Capabilities", HeaderValue())
	w.Header().Set("X-Nagg-GraphQL-Schema-Version", GraphQLSchemaVersion)
	w.Header().Set("X-Nagg-App-View-Version", AppViewVersion)
}
