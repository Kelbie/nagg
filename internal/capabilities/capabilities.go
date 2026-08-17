package capabilities

import (
	"net/http"
	"strings"
)

const (
	GraphQLSchemaVersion = "2026-06-18"
	AppViewVersion       = "v2"
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
	"graphql.events.maxContentLength",
	"graphql.aggregateEvents.shuffle",
	"graphql.rank.weightedTerms",
	"graphql.rank.candidatePubkeyBoosts",
	"graphql.rank.pubkeyScoreTerms",
	"graphql.rank.pubkeyScoreFilters",
	"graphql.rank.candidateFieldTerms",
	"graphql.rank.derivedMetricTerms",
	"graphql.rank.shuffle",
	"graphql.rank.viewerPubkey",
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
	"appview.thread.total",
	"appview.v2",
}

// AppViewRoutes is the advertised REST surface. It MUST match the routes
// appview's Register actually mounts — clients feature-gate on this manifest,
// and it had silently drifted to under-report seven live routes.
// appview's TestCapabilitiesRouteParity enforces the match.
var AppViewRoutes = []string{
	"/nostr/capabilities",
	"/nostr/feed",
	"/nostr/feed/user",
	"/nostr/feed/ranked",
	"/nostr/notifications",
	"/nostr/events/aggregates",
	"/nostr/thread",
	"/nostr/follows",
	"/nostr/events",
	"/nostr/events/query",
	"/nostr/dm/envelopes",
	"/nostr/dm/conversation",
	"/nostr/follow-status",
	"/nostr/mint/reviews",
	"/nostr/mint/discover",
	"/nostr/mint/history",
	"/nostr/mint/changes",
	"/nostr/social-graph",
	"/nostr/own/profiles",
	"/nostr/own/",
	"/nostr/notifications/seen",
	"/nostr/profiles",
	"/nostr/profile",
	"/nostr/search",
	"/nostr/recommended",
	"/app/latest-version",
	"/app/ai-lineup",
}

// ServiceInfo advertises the full declared surface — every route a deployment
// running every module serves.
func ServiceInfo() map[string]any {
	return ServiceInfoFor(AppViewRoutes)
}

// ServiceInfoFor advertises a specific route list. A deployment running a subset
// of modules (NAGG_MODULES) mounts a subset of AppViewRoutes and must advertise
// exactly what it mounts: clients feature-gate on this manifest, so promising a
// route that 404s is worse than not promising it.
func ServiceInfoFor(routes []string) map[string]any {
	return map[string]any{
		"graphqlSchemaVersion": GraphQLSchemaVersion,
		"appViewVersion":       AppViewVersion,
		"capabilities":         append([]string(nil), Names...),
		"appViews": []map[string]any{{
			"version": AppViewVersion,
			"routes":  append([]string(nil), routes...),
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
