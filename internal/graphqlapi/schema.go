package graphqlapi

import (
	"context"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/gqlerrors"
	"github.com/graphql-go/graphql/language/ast"
	"github.com/vertex-lab/nagg/internal/capabilities"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
	"github.com/vertex-lab/nagg/internal/dvm"
	"github.com/vertex-lab/nagg/internal/vertex"
	"golang.org/x/sync/singleflight"
)

type Store interface {
	EventByID(context.Context, string) (*chstore.EventView, error)
	QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error)
	QueryEventsByTagTargets(context.Context, chstore.EventQueryInput, chstore.TagFilter, []string, uint64) (map[string][]chstore.EventView, error)
	QueryLatestEventsByPubKeys(context.Context, []string, []int, uint64) (map[string][]chstore.EventView, error)
	AggregateEvents(context.Context, chstore.AggregateInput) ([]chstore.AggregateRow, error)
	AggregateEventReferencesToTargets(context.Context, chstore.AggregateInput, chstore.EventQueryInput) ([]chstore.AggregateRow, error)
	LatestK0(context.Context, []string) (map[string]chstore.K0Row, error)
	BatchPubkeyStats(context.Context, []string) (map[string]chstore.PubkeyStats, error)
	FollowEdges(context.Context, string, []string) (map[string]chstore.FollowEdge, error)
	SearchK0(context.Context, string, uint64) ([]chstore.ProfileSearchRow, error)
	CachedVertexProfiles(context.Context, []string) (map[string]vertex.ProfileResult, error)
	PubkeyScores(context.Context, string, []string) (map[string]chstore.PubkeyScore, error)
	DerivedMetricValues(context.Context, string, []string) (map[string]float64, error)
	ViewerFeed(context.Context, chstore.ViewerFeedInput) ([]chstore.ViewerFeedRow, error)
	DescendantEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error)
	RankedEventsByFeatures(context.Context, chstore.FeatureRankInput) ([]chstore.RankedFeatureRow, error)
	RefSourceIDs(context.Context, string) ([]string, error)
	RankedRefSources(ctx context.Context, parentID, sort string, limit, offset int) ([]string, error)
	AuthoredRefChain(ctx context.Context, rootID, author string, maxDepth int) ([]string, error)
	FollowedRefs(context.Context, string, []string) (map[string]string, error)
}

var graphqlOperationNamePattern = regexp.MustCompile(`\b(?:query|mutation)\s+([A-Za-z0-9_]+)`)

const defaultPubkeyScoreMinFollowers uint64 = 500

type referenceAggregateStore interface {
	AggregateEventsByTagTargets(context.Context, chstore.ReferenceAggregateInput) (map[string][]chstore.AggregateRow, bool, error)
}

var hex64Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type resolver struct {
	store                   Store
	userBackfiller          UserFeedBackfiller
	dmEnvelopeBackfiller    DMEnvelopeBackfiller
	relayEventBackfiller    RelayEventBackfiller
	profileSearcher         ProfileSearcher
	pubkeyScoreMinFollowers uint64
	basePool                *basePoolCache
	dvm                     *dvm.Registry
	mintInfo                MintHistoryProvider
}

// defaultScoreSource is the pubkey-score provider rank terms use when a term
// names none: the first registered DVM plugin, falling back to the Vertex
// name so an unwired schema (tests) keeps today's behavior.
func (r *resolver) defaultScoreSource() string {
	if r.dvm != nil && len(r.dvm.Names()) > 0 {
		return r.dvm.Names()[0]
	}
	return vertex.PluginName
}

type ProfileSearcher interface {
	Search(context.Context, vertex.SearchArgs) ([]vertex.SearchResult, bool, error)
}

type UserFeedBackfiller interface {
	BackfillUserFeed(context.Context, string, uint64) error
}

type UserFeedHydrator interface {
	HydrateUserFeed(context.Context, string, uint64) (bool, error)
}

type UserFeedsHydrator interface {
	HydrateUserFeeds(context.Context, []string, uint64) (bool, error)
}

type DMEnvelopeBackfiller interface {
	BackfillDMEnvelopes(context.Context, string, []int, int64, uint64) error
}

type DMEnvelopeHydrator interface {
	HydrateDMEnvelopes(context.Context, string, []int, int64, uint64) (bool, error)
}

type RelayEventBackfiller interface {
	BackfillRelayEvents(context.Context, chstore.EventQueryInput, string) error
}

type RelayEventHydrator interface {
	HydrateRelayEvents(context.Context, chstore.EventQueryInput, string) (bool, error)
}

type Option func(*resolver)

func WithUserFeedBackfill(backfiller UserFeedBackfiller) Option {
	return func(r *resolver) {
		r.userBackfiller = backfiller
		if b, ok := backfiller.(DMEnvelopeBackfiller); ok {
			r.dmEnvelopeBackfiller = b
		}
		if b, ok := backfiller.(RelayEventBackfiller); ok {
			r.relayEventBackfiller = b
		}
	}
}

// WithDVM installs the DVM plugin registry: rank-term score sources resolve
// against registered plugin names instead of a hardcoded vendor string.
// WithMintHistory wires the cashu mint-info history reader behind the
// mintInfoHistory query. Without it the field resolves null.
func WithMintHistory(provider MintHistoryProvider) Option {
	return func(r *resolver) {
		r.mintInfo = provider
	}
}

func WithDVM(reg *dvm.Registry) Option {
	return func(r *resolver) {
		r.dvm = reg
	}
}

func WithProfileSearch(searcher ProfileSearcher) Option {
	return func(r *resolver) {
		r.profileSearcher = searcher
	}
}

func WithPubkeyScoreMinFollowers(minFollowers int) Option {
	return func(r *resolver) {
		if minFollowers >= 0 {
			r.pubkeyScoreMinFollowers = uint64(minFollowers)
		}
	}
}

type HandlerOption func(*handlerConfig)

type handlerConfig struct {
	requestTimeout        time.Duration
	relayHydrationMaxJobs int
}

func WithRequestTimeout(timeout time.Duration) HandlerOption {
	return func(c *handlerConfig) {
		if timeout > 0 {
			c.requestTimeout = timeout
		}
	}
}

func WithRelayHydrationMaxJobs(maxJobs int) HandlerOption {
	return func(c *handlerConfig) {
		if maxJobs >= 0 {
			c.relayHydrationMaxJobs = maxJobs
		}
	}
}

type relayHydrationBudgetKey struct{}

type relayHydrationBudget struct {
	mu        sync.Mutex
	remaining int
}

func consumeRelayHydrationBudget(ctx context.Context) bool {
	budget, _ := ctx.Value(relayHydrationBudgetKey{}).(*relayHydrationBudget)
	if budget == nil {
		return true
	}
	budget.mu.Lock()
	defer budget.mu.Unlock()
	if budget.remaining <= 0 {
		return false
	}
	budget.remaining--
	return true
}

type eventNode struct {
	event          chstore.EventView
	relations      *pubkeyRelationCache
	eventRelations *eventRelationCache
}

type notificationNode struct {
	row   chstore.ViewerFeedRow
	event eventNode
}

type notificationConnectionSource struct {
	rows  []chstore.ViewerFeedRow
	nodes []notificationNode
}

type profileSearchResultNode struct {
	row     vertex.SearchResult
	profile chstore.K0Row
	vertex  *vertex.ProfileResult
}

type profileSearchConnectionSource struct {
	query     string
	limit     int
	sort      string
	source    string
	fromCache bool
	nodes     []profileSearchResultNode
}

type eventConnectionSource struct {
	raw   []chstore.EventView
	nodes []eventNode
}

type eventRelationCache struct {
	store                   Store
	events                  []chstore.EventView
	pubkeyScoreMinFollowers uint64

	group singleflight.Group

	mu                  sync.Mutex
	latestEventTags     map[string][]string
	followedReplies     map[string]map[string][]chstore.EventView
	followedConnections map[string]eventConnectionCaches
}

type eventConnectionCaches struct {
	relations      *pubkeyRelationCache
	eventRelations *eventRelationCache
}

type pubkeyRelationKey struct {
	kinds string
	limit int
}

type pubkeyRelationCache struct {
	store   Store
	pubkeys []string

	mu    sync.Mutex
	cache map[pubkeyRelationKey]map[string][]chstore.EventView
}

func NewSchema(store Store, opts ...Option) (graphql.Schema, error) {
	r := &resolver{store: store, pubkeyScoreMinFollowers: defaultPubkeyScoreMinFollowers}
	r.basePool = newBasePoolCache(basePoolTTL, time.Now)
	for _, opt := range opts {
		opt(r)
	}
	jsonType := jsonScalar("JSON")

	var eventConnectionType *graphql.Object
	var aggregationResultType *graphql.Object
	var tagFilterType *graphql.InputObject
	var latestEventTagPubkeySourceInputType *graphql.InputObject
	var pubkeySourceInputType *graphql.InputObject
	var eventQueryInputType *graphql.InputObject
	var referenceInputType *graphql.InputObject
	var reverseReferenceInputType *graphql.InputObject
	var pubkeyScoreRankInputType *graphql.InputObject
	var shuffleInputType *graphql.InputObject
	var weightedRankTermInputType *graphql.InputObject
	var candidatePubkeyBoostInputType *graphql.InputObject
	var rankedEventsInputType *graphql.InputObject
	var notificationInputType *graphql.InputObject
	var profileSearchInputType *graphql.InputObject

	var eventType *graphql.Object
	eventType = graphql.NewObject(graphql.ObjectConfig{
		Name: "NostrEvent",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":        &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: eventField(func(ev chstore.EventView) any { return ev.ID })},
				"pubkey":    &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: eventField(func(ev chstore.EventView) any { return ev.PubKey })},
				"kind":      &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: eventField(func(ev chstore.EventView) any { return ev.Kind })},
				"createdAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime), Resolve: eventField(func(ev chstore.EventView) any { return ev.CreatedAt })},
				"content":   &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: eventField(func(ev chstore.EventView) any { return ev.Content })},
				"tags":      &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String))))), Resolve: eventField(func(ev chstore.EventView) any { return ev.Tags })},
				"sig":       &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: eventField(func(ev chstore.EventView) any { return ev.Sig })},
				"updatedAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime), Resolve: eventField(func(ev chstore.EventView) any { return ev.UpdatedAt })},
				"pubkeyEvents": &graphql.Field{
					Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(eventType))),
					Args: graphql.FieldConfigArgument{
						"kinds": &graphql.ArgumentConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
						"limit": &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 1},
					},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						node, ok := asEventNode(p.Source)
						if !ok || node.relations == nil {
							return []eventNode{}, nil
						}
						return node.relations.load(p.Context, node.event.PubKey, intList(p.Args["kinds"]), intValue(p.Args["limit"], 1))
					},
				},
				"references": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: referenceInputType}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						event, ok := eventFromSource(p.Source)
						if !ok {
							return eventConnectionSource{}, nil
						}
						return r.eventReferences(p.Context, event, p.Args["input"])
					},
				},
				"referencedBy": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(reverseReferenceInputType)}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						event, ok := eventFromSource(p.Source)
						if !ok {
							return eventConnectionSource{}, nil
						}
						return r.eventReferencedBy(p.Context, event, p.Args["input"])
					},
				},
				// followedReply is the precomputed, BATCHED "a person you follow
				// replied" preview: the single most-liked direct reply to this event
				// authored by someone `viewer` follows, served from the reply-edge and
				// k7_e aggregate tables in one round-trip for the whole page.
				"followedReply": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"viewer": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						viewer, _ := p.Args["viewer"].(string)
						node, ok := asEventNode(p.Source)
						if ok && node.eventRelations != nil {
							return node.eventRelations.loadFollowedReply(p.Context, r, node.event, viewer)
						}
						event, ok := eventFromSource(p.Source)
						if !ok {
							return eventConnectionSource{}, nil
						}
						return r.eventFollowedReply(p.Context, event, viewer)
					},
				},
			}
		}),
	})

	pageInfoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "PageInfo",
		Fields: graphql.Fields{
			"hasNextPage": &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"endCursor":   &graphql.Field{Type: graphql.String},
		},
	})

	eventConnectionType = graphql.NewObject(graphql.ObjectConfig{
		Name: "EventConnection",
		Fields: graphql.Fields{
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(eventType))),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source, _ := p.Source.(eventConnectionSource)
					return source.nodes, nil
				},
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfoType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source, _ := p.Source.(eventConnectionSource)
					return map[string]any{"hasNextPage": false, "endCursor": eventEndCursor(source.raw)}, nil
				},
			},
		},
	})

	tagFilterType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TagFilterInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"key":    &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"value":  &graphql.InputObjectFieldConfig{Type: graphql.String},
			"values": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"excludeValues": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.NewNonNull(graphql.String)),
			},
			"dataset": &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "TAGS"},
			"marker":  &graphql.InputObjectFieldConfig{Type: graphql.String},
			"markers": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.NewNonNull(graphql.String)),
			},
			"excludeMarkers": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.NewNonNull(graphql.String)),
			},
			"index": &graphql.InputObjectFieldConfig{Type: graphql.Int},
			// directReplies restricts an 'e'-tag reverse reference to NIP-10/22
			// direct replies (via ref_edges), excluding grandchildren and
			// quotes. Used by the thread view.
			"directReplies": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
		},
	})

	latestEventTagPubkeySourceInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "LatestEventTagPubkeySourceInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"pubkey":    &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"kinds":     &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.Int)))},
			"tag":       &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			"limit":     &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 1},
			"maxValues": &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 2000},
		},
	})
	pubkeySourceInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PubkeySourceInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"latestEventTags":   &graphql.InputObjectFieldConfig{Type: latestEventTagPubkeySourceInputType},
			"sourceEventAuthor": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
		},
	})

	shuffleInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ShuffleInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"seed":     &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"counter":  &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 0},
			"strength": &graphql.InputObjectFieldConfig{Type: graphql.Float, DefaultValue: 0.15},
		},
	})

	pubkeyScoreFilterInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PubkeyScoreFilterInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"source":       &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: r.defaultScoreSource()},
			"minFollowers": &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 0},
		},
	})

	eventQueryInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "EventQueryInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"ids":         &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"pubkeys":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"pubkeysFrom": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(pubkeySourceInputType))},
			"excludeIds":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"excludePubkeys": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.NewNonNull(graphql.String)),
			},
			"kinds":   &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
			"tags":    &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(tagFilterType))},
			"search":  &graphql.InputObjectFieldConfig{Type: graphql.String},
			"since":   &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"until":   &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"limit":   &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 50},
			"offset":  &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"shuffle": &graphql.InputObjectFieldConfig{Type: shuffleInputType},
			"pubkeyScore": &graphql.InputObjectFieldConfig{
				Type:        pubkeyScoreFilterInputType,
				Description: "When set, only events authored by pubkeys with a matching cached score row are included.",
			},
			"maxContentLength": &graphql.InputObjectFieldConfig{
				Type:        graphql.Int,
				Description: "When set (>0), excludes text events (kinds 1/1111) whose content exceeds this many UTF-8 code points. Non-text kinds pass untouched.",
			},
		},
	})

	referenceInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "EventReferenceInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"tags":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(tagFilterType))},
			"limit": &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 20},
		},
	})
	reverseReferenceInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ReverseEventReferenceInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"events": &graphql.InputObjectFieldConfig{Type: eventQueryInputType},
			"via":    &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			"target": &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "EVENT_ID"},
			"limit":  &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 50},
			"offset": &graphql.InputObjectFieldConfig{Type: graphql.Int},
		},
	})
	metricInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "GenericMetricInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"name":          &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"op":            &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"field":         &graphql.InputObjectFieldConfig{Type: graphql.String},
			"tagKey":        &graphql.InputObjectFieldConfig{Type: graphql.String},
			"tagIndex":      &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 1},
			"derived":       &graphql.InputObjectFieldConfig{Type: graphql.String},
			"distinctField": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})
	pubkeyScoreRankInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PubkeyScoreRankInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"source":       &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: r.defaultScoreSource()},
			"target":       &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "AUTHOR"},
			"minFollowers": &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"fallback":     &graphql.InputObjectFieldConfig{Type: graphql.Float, DefaultValue: 0.0},
		},
	})
	weightedRankTermInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "WeightedRankTermInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"references":     &graphql.InputObjectFieldConfig{Type: eventQueryInputType},
			"via":            &graphql.InputObjectFieldConfig{Type: tagFilterType},
			"metric":         &graphql.InputObjectFieldConfig{Type: metricInputType},
			"pubkeyScore":    &graphql.InputObjectFieldConfig{Type: pubkeyScoreRankInputType},
			"candidateField": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"derivedMetric":  &graphql.InputObjectFieldConfig{Type: graphql.String},
			"weight":         &graphql.InputObjectFieldConfig{Type: graphql.Float, DefaultValue: 1.0},
			"transform":      &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "IDENTITY"},
			"halfLifeSeconds": &graphql.InputObjectFieldConfig{
				Type:         graphql.Int,
				DefaultValue: 86400,
				Description:  "Used by RECENCY_HALFLIFE transforms on candidate time fields.",
			},
		},
	})
	candidatePubkeyBoostInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "CandidatePubkeyBoostInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"pubkeys":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"pubkeysFrom": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(pubkeySourceInputType))},
			"weight":      &graphql.InputObjectFieldConfig{Type: graphql.Float, DefaultValue: 1.0},
		},
	})
	aggregateRowType := graphql.NewObject(graphql.ObjectConfig{
		Name: "AggregationRow",
		Fields: graphql.Fields{
			"dimensions": &graphql.Field{Type: graphql.NewNonNull(jsonType)},
			"metrics":    &graphql.Field{Type: graphql.NewNonNull(jsonType)},
		},
	})
	aggregationResultType = graphql.NewObject(graphql.ObjectConfig{
		Name: "AggregationResult",
		Fields: graphql.Fields{
			"rows": &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(aggregateRowType)))},
		},
	})
	aggregationInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "EventAggregationInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"dataset": &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "EVENTS"},
			"groupBy": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))},
			"metrics": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"ids":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"pubkeys": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"kinds":   &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
			"tags":    &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(tagFilterType))},
			"since":   &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"until":   &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"limit":   &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 100},
			"shuffle": &graphql.InputObjectFieldConfig{Type: shuffleInputType},
		},
	})
	rankedEventsInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "RankedEventsInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"references": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(eventQueryInputType)},
			"via":        &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			// Optional requesting account, carried for forward-compatible
			// personalization (future viewer-specific ranking/mutes).
			"pubkey": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"target": &graphql.InputObjectFieldConfig{Type: eventQueryInputType},
			"metric": &graphql.InputObjectFieldConfig{Type: metricInputType},
			"terms":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(weightedRankTermInputType))},
			"candidatePubkeyBoosts": &graphql.InputObjectFieldConfig{
				Type: graphql.NewList(graphql.NewNonNull(candidatePubkeyBoostInputType)),
			},
			"shuffle": &graphql.InputObjectFieldConfig{Type: shuffleInputType},
			"limit":   &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 30},
			"offset":  &graphql.InputObjectFieldConfig{Type: graphql.Int},
		},
	})

	appViewCapabilityType := graphql.NewObject(graphql.ObjectConfig{
		Name: "AppViewCapability",
		Fields: graphql.Fields{
			"version": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"routes":  &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))},
		},
	})
	serviceInfoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ServiceInfo",
		Fields: graphql.Fields{
			"graphqlSchemaVersion": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"appViewVersion":       &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"capabilities":         &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))},
			"appViews":             &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(appViewCapabilityType)))},
		},
	})
	notificationTabEnumType := graphql.NewEnum(graphql.EnumConfig{
		Name: "NotificationTab",
		Values: graphql.EnumValueConfigMap{
			"ALL":      &graphql.EnumValueConfig{Value: "ALL"},
			"MENTIONS": &graphql.EnumValueConfig{Value: "MENTIONS"},
		},
	})
	notificationPolicyEnumType := graphql.NewEnum(graphql.EnumConfig{
		Name: "NotificationPolicy",
		Values: graphql.EnumValueConfigMap{
			"RELAXED":  &graphql.EnumValueConfig{Value: "RELAXED"},
			"MODERATE": &graphql.EnumValueConfig{Value: "MODERATE"},
			"STRICT":   &graphql.EnumValueConfig{Value: "STRICT"},
			"FOLLOWS":  &graphql.EnumValueConfig{Value: "FOLLOWS"},
		},
	})
	notificationReplyScopeEnumType := graphql.NewEnum(graphql.EnumConfig{
		Name: "NotificationReplyScope",
		Values: graphql.EnumValueConfigMap{
			"DIRECT": &graphql.EnumValueConfig{Value: "DIRECT"},
			"THREAD": &graphql.EnumValueConfig{Value: "THREAD"},
		},
	})
	notificationInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ViewerFeedInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"pubkey":     &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"tab":        &graphql.InputObjectFieldConfig{Type: notificationTabEnumType, DefaultValue: "ALL"},
			"policy":     &graphql.InputObjectFieldConfig{Type: notificationPolicyEnumType, DefaultValue: "STRICT"},
			"replyScope": &graphql.InputObjectFieldConfig{Type: notificationReplyScopeEnumType, DefaultValue: "THREAD"},
			"since":      &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"until":      &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"limit":      &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 50},
		},
	})
	profileSearchInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ProfileSearchInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"query":  &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"limit":  &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 10},
			"sort":   &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: vertex.DefaultSearchSort},
			"source": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})
	profileSearchResultType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ProfileSearchResult",
		Fields: graphql.Fields{
			"pubkey":      &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return n.row.PubKey })},
			"npub":        &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return n.row.Npub })},
			"rank":        &graphql.Field{Type: graphql.Float, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return profileSearchLegacyRank(n) })},
			"score":       &graphql.Field{Type: graphql.Float, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return profileSearchLegacyScore(n) })},
			"searchRank":  &graphql.Field{Type: graphql.Float, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return floatPtrValue(n.row.Rank) })},
			"searchScore": &graphql.Field{Type: graphql.Float, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return floatPtrValue(n.row.Score) })},
			"profileRank": &graphql.Field{Type: graphql.Float, Resolve: profileSearchResultField(func(n profileSearchResultNode) any {
				if n.vertex == nil {
					return nil
				}
				return n.vertex.Rank
			})},
			"profileScore": &graphql.Field{Type: graphql.Float, Resolve: profileSearchResultField(func(n profileSearchResultNode) any {
				if n.vertex == nil {
					return nil
				}
				return floatPtrValue(n.vertex.Score)
			})},
			"followers": &graphql.Field{Type: graphql.Int, Resolve: profileSearchResultField(func(n profileSearchResultNode) any {
				if n.vertex == nil {
					return nil
				}
				return uintPtrIntValue(n.vertex.Followers)
			})},
			"follows": &graphql.Field{Type: graphql.Int, Resolve: profileSearchResultField(func(n profileSearchResultNode) any {
				if n.vertex == nil {
					return nil
				}
				return uintPtrIntValue(n.vertex.Follows)
			})},
			"createdAt":   &graphql.Field{Type: graphql.DateTime, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return profileSearchCreatedAt(n) })},
			"name":        &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.Name) })},
			"displayName": &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.DisplayName) })},
			"picture":     &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.Picture) })},
			"image":       &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.Picture) })},
			"banner":      &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.Banner) })},
			"about":       &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.About) })},
			"nip05":       &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.NIP05) })},
			"nip05Valid":  &graphql.Field{Type: graphql.Boolean, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return nil })},
			"website":     &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.Website) })},
			"lud16":       &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.LUD16) })},
			"lud06":       &graphql.Field{Type: graphql.String, Resolve: profileSearchResultField(func(n profileSearchResultNode) any { return emptyStringNil(n.profile.LUD06) })},
		},
	})
	profileSearchConnectionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ProfileSearchConnection",
		Fields: graphql.Fields{
			"query":     &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: profileSearchConnectionField(func(s profileSearchConnectionSource) any { return s.query })},
			"limit":     &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: profileSearchConnectionField(func(s profileSearchConnectionSource) any { return s.limit })},
			"sort":      &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: profileSearchConnectionField(func(s profileSearchConnectionSource) any { return s.sort })},
			"source":    &graphql.Field{Type: graphql.String, Resolve: profileSearchConnectionField(func(s profileSearchConnectionSource) any { return emptyStringNil(s.source) })},
			"fromCache": &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean), Resolve: profileSearchConnectionField(func(s profileSearchConnectionSource) any { return s.fromCache })},
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(profileSearchResultType))),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source, _ := p.Source.(profileSearchConnectionSource)
					return source.nodes, nil
				},
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfoType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source, _ := p.Source.(profileSearchConnectionSource)
					endCursor := ""
					if len(source.nodes) > 0 {
						endCursor = source.nodes[len(source.nodes)-1].row.PubKey
					}
					return map[string]any{"hasNextPage": false, "endCursor": endCursor}, nil
				},
			},
		},
	})
	notificationType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Notification",
		Fields: graphql.Fields{
			"event": &graphql.Field{
				Type: graphql.NewNonNull(eventType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					node, _ := p.Source.(notificationNode)
					return node.event, nil
				},
			},
			"reason": &graphql.Field{
				// Display adapter: the store speaks kinds; this route-aligned
				// GraphQL surface derives its label from the kind + tags.
				Type: graphql.NewNonNull(graphql.String),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					node, _ := p.Source.(notificationNode)
					return displayReasonForEvent(node.row.Event, node.row.Kind), nil
				},
			},
			"actorVertexScore": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Float),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					node, _ := p.Source.(notificationNode)
					return node.row.ActorVertexScore, nil
				},
			},
		},
	})
	notificationConnectionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "NotificationConnection",
		Fields: graphql.Fields{
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(notificationType))),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source, _ := p.Source.(notificationConnectionSource)
					return source.nodes, nil
				},
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfoType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					source, _ := p.Source.(notificationConnectionSource)
					events := make([]chstore.EventView, 0, len(source.rows))
					for _, row := range source.rows {
						events = append(events, row.Event)
					}
					return map[string]any{"hasNextPage": false, "endCursor": eventEndCursor(events)}, nil
				},
			},
		},
	})

	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
			"serviceInfo": &graphql.Field{
				Type: graphql.NewNonNull(serviceInfoType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					return serviceInfo(), nil
				},
			},
			"event": &graphql.Field{
				Type: eventType,
				Args: graphql.FieldConfigArgument{"id": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					id := p.Args["id"].(string)
					if err := validateHex64(id); err != nil {
						return nil, err
					}
					r.hydrateRelayEventQuery(p.Context, chstore.EventQueryInput{IDs: []string{id}, Limit: 1}, "event")
					event, err := r.store.EventByID(p.Context, id)
					if err != nil {
						return nil, err
					}
					relations := newPubkeyRelationCache(r.store, []chstore.EventView{*event})
					eventRelations := newEventRelationCacheWithPubkeyScoreMinFollowers(r.store, []chstore.EventView{*event}, r.pubkeyScoreMinFollowers)
					return wrapEvent(*event, relations, eventRelations), nil
				},
			},
			"events": &graphql.Field{
				Type: graphql.NewNonNull(eventConnectionType),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(eventQueryInputType)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					input, err := r.parseEventQueryInput(p.Context, p.Args["input"].(map[string]any))
					if err != nil {
						return nil, err
					}
					events, err := r.queryEvents(p.Context, input)
					if err != nil {
						return nil, err
					}
					if r.relayEventBackfiller == nil && r.shouldBackfillAuthorQuery(input.PubKeys, input.IDs, input.Tags, input.Kinds, len(events), input.Limit) {
						completed, err := r.hydrateAuthors(p.Context, input.PubKeys, input.Limit)
						if err != nil {
							slog.Warn("graphql author backfill failed", "pubkeys", input.PubKeys, "error", err)
						} else if completed {
							events, err = r.queryEvents(p.Context, input)
							if err != nil {
								return nil, err
							}
						}
					}
					return r.newEventConnection(events), nil
				},
			},
			"aggregateEvents": &graphql.Field{
				Type: graphql.NewNonNull(aggregationResultType),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(aggregationInputType)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					input, err := parseAggregateInput(p.Args["input"].(map[string]any))
					if err != nil {
						return nil, err
					}
					r.hydrateAggregateInput(p.Context, input, "aggregateEvents")
					if r.relayEventBackfiller == nil && r.shouldBackfillAuthorQuery(input.PubKeys, input.IDs, input.Tags, input.Kinds, 0, 1) {
						if _, err := r.hydrateAuthors(p.Context, input.PubKeys, 100); err != nil {
							slog.Warn("graphql aggregate author backfill failed", "pubkeys", input.PubKeys, "error", err)
						}
					}
					rows, err := r.store.AggregateEvents(p.Context, input)
					return map[string]any{"rows": rows}, err
				},
			},
			"profileSearch": &graphql.Field{
				Type: graphql.NewNonNull(profileSearchConnectionType),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(profileSearchInputType)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					input, err := parseProfileSearchInput(p.Args["input"].(map[string]any))
					if err != nil {
						return nil, err
					}
					return r.profileSearch(p.Context, input)
				},
			},
			"notifications": &graphql.Field{
				Type: graphql.NewNonNull(notificationConnectionType),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(notificationInputType)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					input, err := parseNotificationInput(p.Args["input"].(map[string]any))
					if err != nil {
						return nil, err
					}
					r.hydrateNotificationInput(p.Context, input)
					rows, err := r.store.ViewerFeed(p.Context, input)
					if err != nil {
						return nil, err
					}
					return newNotificationConnection(r.store, rows, r.pubkeyScoreMinFollowers), nil
				},
			},
			"rankedEvents": &graphql.Field{
				Type: graphql.NewNonNull(eventConnectionType),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(rankedEventsInputType)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					return r.rankedEvents(p.Context, p.Args["input"])
				},
			},
		},
	})

	for name, field := range socialQueryFields(r, eventConnectionType) {
		queryType.AddFieldConfig(name, field)
	}
	for name, field := range mintQueryFields(r, jsonType) {
		queryType.AddFieldConfig(name, field)
	}

	return graphql.NewSchema(graphql.SchemaConfig{Query: queryType})
}

func (r *resolver) shouldBackfillAuthorQuery(pubkeys []string, ids []string, tags []chstore.TagFilter, kinds []int, currentLen int, limit uint64) bool {
	if r.userBackfiller == nil || len(pubkeys) == 0 || len(ids) > 0 || len(tags) > 0 {
		return false
	}
	if limit == 0 || limit > 500 {
		limit = 50
	}
	if currentLen >= int(limit) {
		return false
	}
	for _, kind := range kinds {
		switch kind {
		case 0, 1, 6, 16:
		default:
			return false
		}
	}
	return true
}

func (r *resolver) hydrateAuthors(ctx context.Context, pubkeys []string, limit uint64) (bool, error) {
	if limit == 0 {
		limit = 50
	}
	if hydrator, ok := r.userBackfiller.(UserFeedsHydrator); ok {
		return hydrator.HydrateUserFeeds(ctx, pubkeys, limit)
	}
	if hydrator, ok := r.userBackfiller.(UserFeedHydrator); ok {
		completed := true
		for _, pubkey := range pubkeys {
			ok, err := hydrator.HydrateUserFeed(ctx, pubkey, limit)
			if err != nil {
				return false, err
			}
			if !ok {
				completed = false
			}
		}
		return completed, nil
	}
	for _, pubkey := range pubkeys {
		if err := r.userBackfiller.BackfillUserFeed(ctx, pubkey, limit); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *resolver) profileSearch(ctx context.Context, input vertex.SearchArgs) (profileSearchConnectionSource, error) {
	args := vertex.NormalizeSearchArgs(input)
	rows := make([]vertex.SearchResult, 0, args.Limit)
	profiles := make(map[string]chstore.K0Row)
	seen := make(map[string]struct{})

	fromCache := true
	if r.profileSearcher != nil {
		vertexRows, vertexFromCache, err := r.profileSearcher.Search(ctx, args)
		if err != nil {
			slog.Warn("graphql profile search vertex lookup failed", "query", args.Query, "sort", args.Sort, "source", args.Source, "error", err)
		} else {
			fromCache = vertexFromCache
			for _, row := range vertexRows {
				pubkey, ok := vertex.NormalizePubkey(row.PubKey)
				if !ok {
					continue
				}
				if _, ok := seen[pubkey]; ok {
					continue
				}
				row.PubKey = pubkey
				if row.Npub == "" {
					row.Npub = vertex.Npub(pubkey)
				}
				rows = append(rows, row)
				seen[pubkey] = struct{}{}
				if len(rows) >= args.Limit {
					break
				}
			}
		}
	}

	if len(rows) < args.Limit {
		remaining := args.Limit - len(rows)
		localRows, err := r.store.SearchK0(ctx, args.Query, uint64(remaining))
		if err != nil {
			return profileSearchConnectionSource{}, err
		}
		for _, local := range localRows {
			pubkey, ok := vertex.NormalizePubkey(local.Profile.PubKey)
			if !ok {
				continue
			}
			if _, ok := seen[pubkey]; ok {
				continue
			}
			profile := local.Profile
			profile.PubKey = pubkey
			rank := local.Rank
			score := local.Score
			rows = append(rows, vertex.SearchResult{
				PubKey: pubkey,
				Npub:   vertex.Npub(pubkey),
				Rank:   &rank,
				Score:  &score,
			})
			profiles[pubkey] = profile
			seen[pubkey] = struct{}{}
			if len(rows) >= args.Limit {
				break
			}
		}
	}

	pubkeys := make([]string, 0, len(rows))
	for _, row := range rows {
		pubkeys = append(pubkeys, row.PubKey)
	}
	if r.userBackfiller != nil && len(pubkeys) > 0 {
		if _, err := r.hydrateAuthors(ctx, pubkeys, 1); err != nil {
			slog.Warn("graphql profile search profile backfill failed", "pubkeys", len(pubkeys), "error", err)
		}
	}
	latestProfiles, err := r.store.LatestK0(ctx, pubkeys)
	if err != nil {
		return profileSearchConnectionSource{}, err
	}
	for pubkey, profile := range latestProfiles {
		if profile.PubKey != "" {
			profiles[pubkey] = profile
		}
	}
	vertexProfiles, err := r.store.CachedVertexProfiles(ctx, pubkeys)
	if err != nil {
		return profileSearchConnectionSource{}, err
	}
	nodes := make([]profileSearchResultNode, 0, len(rows))
	for _, row := range rows {
		node := profileSearchResultNode{
			row:     row,
			profile: profiles[row.PubKey],
		}
		if profile, ok := vertexProfiles[row.PubKey]; ok {
			node.vertex = &profile
		}
		nodes = append(nodes, node)
	}
	return profileSearchConnectionSource{
		query:     args.Query,
		limit:     args.Limit,
		sort:      args.Sort,
		source:    args.Source,
		fromCache: fromCache,
		nodes:     nodes,
	}, nil
}

type graphTagPredicate struct {
	Key            string
	Value          string
	Values         []string
	Marker         string
	Markers        []string
	ExcludeMarkers []string
	Index          int
	// DirectReplies restricts an 'e'-tag reverse reference to NIP-10/22 DIRECT
	// replies (via the ref_edges table) instead of every event that
	// e-tags the target. Used by the thread view so grandchildren and quotes do
	// not leak in.
	DirectReplies bool
}

type genericMetric struct {
	Name          string
	Op            string
	Field         string
	TagKey        string
	TagIndex      int
	Derived       string
	DistinctField string
}

func (r *resolver) eventReferences(ctx context.Context, event chstore.EventView, raw any) (eventConnectionSource, error) {
	input := parseReferenceInput(raw)
	ids := make([]string, 0)
	seen := map[string]struct{}{}
	for _, tag := range event.Tags {
		for _, predicate := range input.Tags {
			if !sourceTagMatches(tag, predicate) || len(tag) < 2 || !hex64Pattern.MatchString(tag[1]) {
				continue
			}
			if _, ok := seen[tag[1]]; ok {
				continue
			}
			seen[tag[1]] = struct{}{}
			ids = append(ids, tag[1])
			if len(ids) >= input.Limit {
				break
			}
		}
		if len(ids) >= input.Limit {
			break
		}
	}
	if len(ids) == 0 {
		return eventConnectionSource{}, nil
	}
	events, err := r.store.QueryEvents(ctx, chstore.EventQueryInput{IDs: ids, Limit: uint64(input.Limit)})
	if err != nil {
		return eventConnectionSource{}, err
	}
	return r.newEventConnection(events), nil
}

func (r *resolver) eventReferencedBy(ctx context.Context, event chstore.EventView, raw any) (eventConnectionSource, error) {
	input, err := r.reverseReferenceQuery(ctx, event, raw)
	if err != nil {
		return eventConnectionSource{}, err
	}
	events, err := r.queryEvents(ctx, input)
	if err != nil {
		return eventConnectionSource{}, err
	}
	return r.newEventConnection(events), nil
}

func (r *resolver) eventFollowedReply(ctx context.Context, event chstore.EventView, viewer string) (eventConnectionSource, error) {
	if viewer == "" {
		return eventConnectionSource{}, nil
	}
	byParent, err := r.store.FollowedRefs(ctx, viewer, []string{event.ID})
	if err != nil {
		return eventConnectionSource{}, err
	}
	replyID, ok := byParent[event.ID]
	if !ok {
		return eventConnectionSource{}, nil
	}
	events, err := r.queryEvents(ctx, chstore.EventQueryInput{IDs: []string{replyID}, Limit: 1})
	if err != nil {
		return eventConnectionSource{}, err
	}
	return newEventConnection(r.store, events), nil
}

func (r *resolver) rankedEvents(ctx context.Context, raw any) (eventConnectionSource, error) {
	input, err := r.parseRankedEventsInput(ctx, raw)
	if err != nil {
		return eventConnectionSource{}, err
	}
	ordered, err := r.rankedEventViews(ctx, input)
	if err != nil {
		return eventConnectionSource{}, err
	}
	if len(ordered) == 0 {
		return eventConnectionSource{}, nil
	}
	return r.newEventConnection(ordered), nil
}

// rankedEventViews runs the ranking core for an already-parsed input and returns
// the ordered events. It is the single source of truth for the ranking
// pipeline (aggregate rows -> ranked target IDs -> queryEvents -> weighted or
// simple ordering) shared by the GraphQL rankedEvents resolver and the REST
// ranked-feed handler. Callers wrap the result however they like (GraphQL wraps
// with newEventConnection; REST enriches into a FeedResponse).
func (r *resolver) rankedEventViews(ctx context.Context, input rankedEventsInput) ([]chstore.EventView, error) {
	// For You-style requests (weighted terms, no explicit candidate id set) share
	// a viewer-independent base pool that is computed once and cached, then
	// finalized per request with the viewer follow-boost and shuffle. This keeps
	// the expensive aggregation off the hot path for every viewer in the TTL
	// window. Everything else uses the direct path.
	// Database-first path: when the weighted terms map cleanly to the precomputed
	// rank_features columns (the For-You / trending recipe terms do), the whole
	// weighted top-N is one indexed ClickHouse scan — no per-request live
	// aggregation. The per-viewer follow-boost + shuffle are then applied cheaply by
	// finalizeRankedIDs. Falls through to the base-pool/direct path when the feature
	// table is cold (before the first rollup tick) or the terms are not recognized.
	if len(input.WeightedTerms) > 0 && featureRankTargetIsGlobal(input.Target) {
		if weights, halfLife, minFollowers, ok := featureWeightsFromTerms(input.WeightedTerms, r.defaultScoreSource()); ok {
			views, served, err := r.rankedEventViewsFromFeatures(ctx, input, weights, halfLife, minFollowers)
			if err != nil {
				return nil, err
			}
			if served {
				return views, nil
			}
		}
	}

	if r.basePool != nil && len(input.WeightedTerms) > 0 && len(input.Target.IDs) == 0 {
		pool, baseScores, err := r.basePool.getOrCompute(ctx, r, input)
		if err != nil {
			return nil, err
		}
		if len(pool) == 0 {
			return nil, nil
		}
		ids := finalizeRankedIDs(pool, baseScores, input.CandidateBoosts, input.Shuffle, input.Offset, input.Limit)
		return orderEventViewsByID(pool, ids), nil
	}
	return r.rankedEventViewsDirect(ctx, input)
}

// featureRankWindow bounds the recent-events window the feature scan reads. It
// matches the rollup's maintained window (RecentWindow, 48h): rows older than that
// are not refreshed, and the recency half-life makes them score negligibly anyway.
// A tighter bound is also a real speedup — rank_features carries a row per
// (event, rollup tick) until background merges collapse the ReplacingMergeTree, so
// the FINAL scan cost grows with the window.
const featureRankWindow = 48 * time.Hour

// featureRankTargetIsGlobal reports whether a ranked request targets a global
// (kind-only) candidate pool — the only shape rank_features can serve. The
// feature scan honors only kinds (+ exclusions, forwarded separately); a request
// that scopes candidates by pubkey, tag, search, or a time window must fall
// through to the live aggregation path, which applies those filters. Without this
// gate an author-scoped "popular posts by X" would silently return global trending.
func featureRankTargetIsGlobal(t chstore.EventQueryInput) bool {
	return len(t.IDs) == 0 &&
		len(t.PubKeys) == 0 &&
		len(t.Tags) == 0 &&
		t.Since == 0 &&
		t.Until == 0 &&
		t.Search == ""
}

// featureWeightsFromTerms maps the weighted rank terms onto the precomputed
// feature columns. It returns ok=false (so the caller falls back to the live
// aggregation path) unless EVERY term maps cleanly, keeping ranking correct for
// any term shape the feature table doesn't represent. Weights ACCUMULATE so
// duplicate terms behave like the live path (which sums every term). Engagement
// weights apply to the vertex-real columns, so an engagement term must (a) use the
// LOG1P transform baked into the feature SQL and (b) gate its engagers by a vertex
// pubkey score — an ungated term (counts ALL engagers, e.g. trending) has no
// matching feature column and falls back.
func featureWeightsFromTerms(terms []weightedRankTerm, scoreSource string) (chstore.FeatureWeights, float64, uint64, bool) {
	var w chstore.FeatureWeights
	var halfLife float64
	var minFollowers uint64
	if len(terms) == 0 {
		return w, 0, 0, false
	}
	bail := func() (chstore.FeatureWeights, float64, uint64, bool) {
		return chstore.FeatureWeights{}, 0, 0, false
	}
	for _, t := range terms {
		switch t.Kind {
		case weightedRankTermPubkeyScore:
			// Only the configured score plugin's AUTHOR score with a zero
			// fallback is represented by the precomputed author score column; a
			// foreign source, non-AUTHOR target, or non-zero fallback would
			// score differently here than on the live path.
			if t.PubkeyScore.Source != scoreSource ||
				t.PubkeyScore.Fallback != 0 ||
				(t.PubkeyScore.Target != "" && t.PubkeyScore.Target != "AUTHOR") {
				return bail()
			}
			w.AuthorVertexScore += t.Weight
			if t.PubkeyScore.MinFollowers > minFollowers {
				minFollowers = t.PubkeyScore.MinFollowers
			}
		case weightedRankTermCandidateField:
			if t.CandidateField != "CREATED_AT" || t.Transform != "RECENCY_HALFLIFE" {
				return bail()
			}
			w.Recency += t.Weight
			halfLife = t.HalfLifeSeconds
		case weightedRankTermDerivedMetric:
			if t.DerivedMetric != "contribution_quality" {
				return bail()
			}
			w.ContributionQuality += t.Weight
		case weightedRankTermReferences:
			// Engagement terms must gate engagers by a vertex pubkey score (the
			// feature columns count only vertex-scored actors); an ungated term
			// has no feature column. The gate is what makes a "rule.metric"
			// term here rank on the score-filtered variant of the declared
			// aggregation rather than its raw envelope value.
			if t.References.PubkeyScore.Source == "" {
				return bail()
			}
			// Metric names are declared-aggregation identifiers ("rule.metric",
			// matching the envelope's aggregates keys), plus "actors" for the
			// cross-rule distinct-engager count. "actors" is un-logged; the
			// per-rule counts use LOG1P. Match the transform per metric so the
			// feature SQL (which bakes the transform into each column) stays
			// exact.
			isLog1p := t.Transform == "LOG1P"
			isIdentity := t.Transform == "" || t.Transform == "IDENTITY"
			switch t.Metric.Name {
			case "actors":
				if !isIdentity {
					return bail()
				}
				w.Actors += t.Weight
			case "k7_e.actors":
				if !isLog1p {
					return bail()
				}
				w.Likes += t.Weight
			case "k1_1111_e_reply.sources":
				if !isLog1p {
					return bail()
				}
				w.Replies += t.Weight
			case "k6_16_e.actors":
				if !isLog1p {
					return bail()
				}
				w.Reposts += t.Weight
			case "k1_q.sources":
				if !isLog1p {
					return bail()
				}
				w.Quotes += t.Weight
			case "k9735_e.value_total":
				if !isLog1p {
					return bail()
				}
				w.ZapSats += t.Weight
			default:
				return bail()
			}
		default:
			return bail()
		}
	}
	return w, halfLife, minFollowers, true
}

// rankedEventViewsFromFeatures runs the DB-side weighted top-N over
// rank_features, hydrates the pool, and applies the per-viewer follow-boost +
// shuffle. served=false signals a cold feature table so the caller can fall back.
func (r *resolver) rankedEventViewsFromFeatures(ctx context.Context, input rankedEventsInput, weights chstore.FeatureWeights, halfLife float64, minFollowers uint64) ([]chstore.EventView, bool, error) {
	rows, err := r.store.RankedEventsByFeatures(ctx, chstore.FeatureRankInput{
		Kinds:              input.Target.Kinds,
		Since:              time.Now().Add(-featureRankWindow).Unix(),
		HalfLifeSeconds:    halfLife,
		Weights:            weights,
		MinAuthorFollowers: minFollowers,
		Limit:              basePoolDepth,
		ExcludeIDs:         input.Target.ExcludeIDs,
		ExcludePubKeys:     input.Target.ExcludePubKeys,
	})
	if err != nil {
		return nil, false, err
	}
	if len(rows) == 0 {
		return nil, false, nil
	}

	ids := make([]string, 0, len(rows))
	scores := make(map[string]float64, len(rows))
	for _, row := range rows {
		ids = append(ids, row.EventID)
		scores[row.EventID] = row.Score
	}

	target := input.Target
	target.IDs = ids
	target.Limit = uint64(len(ids))
	events, err := r.queryEvents(ctx, target)
	if err != nil {
		return nil, false, err
	}
	if len(events) == 0 {
		return nil, false, nil
	}
	finalIDs := finalizeRankedIDs(events, scores, input.CandidateBoosts, input.Shuffle, input.Offset, input.Limit)
	return orderEventViewsByID(events, finalIDs), true, nil
}

// rankedEventViewsDirect is the non-cached ranking path: it sizes the candidate
// set to the requested page, hydrates those events and ranks them in-line.
func (r *resolver) rankedEventViewsDirect(ctx context.Context, input rankedEventsInput) ([]chstore.EventView, error) {
	rows, err := r.rankedEventRows(ctx, input)
	if err != nil {
		return nil, err
	}
	targetIDs := rankedTargetIDs(rows, 0, max(input.Limit+input.Offset, input.Limit))
	if len(targetIDs) == 0 {
		return nil, nil
	}
	if len(input.Target.IDs) > 0 {
		targetIDs = intersectRankedIDs(targetIDs, input.Target.IDs)
	}
	if len(targetIDs) == 0 {
		return nil, nil
	}
	input.Target.IDs = targetIDs
	input.Target.Limit = uint64(len(targetIDs))

	events, err := r.queryEvents(ctx, input.Target)
	if err != nil {
		return nil, err
	}
	if useWeightedRanking(input.WeightedTerms, input.CandidateBoosts, input.Shuffle) {
		targetIDs, err = weightedRankCandidateIDs(ctx, r.store, events, input.WeightedTerms, input.CandidateBoosts, input.Shuffle, input.Offset, input.Limit)
		if err != nil {
			return nil, err
		}
	} else {
		targetIDs = rankedCandidateIDs(rows, events, input.Offset, input.Limit)
	}
	return orderEventViewsByID(events, targetIDs), nil
}

// orderEventViewsByID returns events in the order of ids, skipping ids absent
// from events.
func orderEventViewsByID(events []chstore.EventView, ids []string) []chstore.EventView {
	eventsByID := make(map[string]chstore.EventView, len(events))
	for _, event := range events {
		eventsByID[event.ID] = event
	}
	ordered := make([]chstore.EventView, 0, len(ids))
	for _, id := range ids {
		if event, ok := eventsByID[id]; ok {
			ordered = append(ordered, event)
		}
	}
	return ordered
}

// basePoolDepth is how many top candidates (by reference aggregate) are scored
// and cached as the shared base pool. It must be deep enough that a followed
// author with real engagement can be lifted into the visible page by the
// per-viewer follow-boost — the visible page is at most a few dozen rows, so a
// few hundred candidates leaves ample headroom. Validate against real
// followed-author rank positions before tuning.
const basePoolDepth = 250

// basePoolTTL is how long a computed base pool stays fresh. Engagement freshness
// comes from the continuously-updated aggregate MVs, not from recomputing the
// ranking, so a short window amortizes the expensive aggregation across every
// viewer without staleness that a reader would notice. The HTTP response cache
// adds stale-while-revalidate on top of this.
const basePoolTTL = 90 * time.Second

// basePoolCacheMaxEntries bounds the number of distinct viewer-free pools held
// at once. For You has very few viewer-free key variants, so this is a safety
// cap, not a working-set limit.
const basePoolCacheMaxEntries = 64

type basePoolEntry struct {
	pool      []chstore.EventView
	scores    map[string]float64
	expiresAt time.Time
}

// basePoolCache holds viewer-independent ranked base pools keyed by the
// viewer-free request signature. It is created per schema (NewSchema), never a
// package global, so it can never serve one store's pool to another.
type basePoolCache struct {
	mu      sync.Mutex
	entries map[string]*basePoolEntry
	group   singleflight.Group
	ttl     time.Duration
	now     func() time.Time
}

func newBasePoolCache(ttl time.Duration, now func() time.Time) *basePoolCache {
	return &basePoolCache{
		entries: map[string]*basePoolEntry{},
		ttl:     ttl,
		now:     now,
	}
}

// getOrCompute returns the cached base pool for input's viewer-free signature,
// computing and caching it (once, via singleflight) on a miss or after the TTL.
func (c *basePoolCache) getOrCompute(ctx context.Context, r *resolver, input rankedEventsInput) ([]chstore.EventView, map[string]float64, error) {
	key := basePoolKey(input)

	c.mu.Lock()
	if entry, ok := c.entries[key]; ok && c.now().Before(entry.expiresAt) {
		c.mu.Unlock()
		return entry.pool, entry.scores, nil
	}
	c.mu.Unlock()

	value, err, _ := c.group.Do(key, func() (any, error) {
		pool, scores, err := r.computeBasePool(ctx, input)
		if err != nil {
			return nil, err
		}
		entry := &basePoolEntry{pool: pool, scores: scores, expiresAt: c.now().Add(c.ttl)}
		c.mu.Lock()
		c.sweepExpiredLocked()
		c.entries[key] = entry
		c.mu.Unlock()
		return entry, nil
	})
	if err != nil {
		return nil, nil, err
	}
	entry := value.(*basePoolEntry)
	return entry.pool, entry.scores, nil
}

// sweepExpiredLocked drops expired entries, and if the cache is still at the cap
// clears it entirely (cheap and correct: a dropped pool is just recomputed).
// Caller holds c.mu.
func (c *basePoolCache) sweepExpiredLocked() {
	now := c.now()
	for key, entry := range c.entries {
		if !now.Before(entry.expiresAt) {
			delete(c.entries, key)
		}
	}
	if len(c.entries) >= basePoolCacheMaxEntries {
		c.entries = map[string]*basePoolEntry{}
	}
}

// computeBasePool builds the viewer-independent ranked base pool: the top
// basePoolDepth candidates by reference aggregate, hydrated and scored by the
// weighted terms only. No follow-boost, shuffle, offset or limit is applied —
// those are per-request and handled by finalizeRankedIDs.
func (r *resolver) computeBasePool(ctx context.Context, input rankedEventsInput) ([]chstore.EventView, map[string]float64, error) {
	poolInput := input
	poolInput.Offset = 0
	poolInput.Limit = basePoolDepth
	poolInput.CandidateBoosts = nil
	poolInput.Shuffle = shuffleSpec{}

	rows, err := r.rankedEventRows(ctx, poolInput)
	if err != nil {
		return nil, nil, err
	}
	targetIDs := rankedTargetIDs(rows, 0, basePoolDepth)
	if len(targetIDs) == 0 {
		return nil, map[string]float64{}, nil
	}
	poolInput.Target.IDs = targetIDs
	poolInput.Target.Limit = uint64(len(targetIDs))

	events, err := r.queryEvents(ctx, poolInput.Target)
	if err != nil {
		return nil, nil, err
	}
	scores, err := weightedRankBaseScores(ctx, r.store, events, input.WeightedTerms)
	if err != nil {
		return nil, nil, err
	}
	return events, scores, nil
}

// basePoolKey is the viewer-free cache signature: it covers everything that
// shapes the base pool (references, target filters, rank terms, depth) and
// deliberately excludes the per-viewer/per-request follow-boost, shuffle, offset
// and limit. The For You rank terms are themselves viewer-free, so two viewers
// requesting the same spec collapse to one key.
func basePoolKey(input rankedEventsInput) string {
	keyData := struct {
		References    chstore.AggregateInput
		Target        chstore.EventQueryInput
		RankVia       graphTagPredicate
		WeightedTerms []weightedRankTerm
		Depth         int
	}{
		References:    input.References,
		Target:        input.Target,
		RankVia:       input.RankVia,
		WeightedTerms: input.WeightedTerms,
		Depth:         basePoolDepth,
	}
	encoded, err := json.Marshal(keyData)
	if err != nil {
		// A non-marshalable input should never reach here; fall back to a
		// non-colliding unique key so we degrade to "never cache" rather than
		// serving the wrong pool.
		return "uncacheable:" + input.ViewerPubkey
	}
	return string(encoded)
}

func (r *resolver) rankedEventRows(ctx context.Context, input rankedEventsInput) ([]chstore.AggregateRow, error) {
	if rankedTargetHasFilters(input.Target) {
		r.hydrateAggregateInput(ctx, input.References, "rankedEvents.references")
		r.hydrateRelayEventQuery(ctx, input.Target, "rankedEvents.target")
		return r.store.AggregateEventReferencesToTargets(ctx, input.References, input.Target)
	}
	r.hydrateAggregateInput(ctx, input.References, "rankedEvents.references")
	return r.store.AggregateEvents(ctx, input.References)
}

func rankedTargetHasFilters(input chstore.EventQueryInput) bool {
	// Kind-only targets are post-filtered during event hydration so global
	// kind-only queries keep the cheaper aggregate path.
	return len(input.IDs) > 0 ||
		len(input.PubKeys) > 0 ||
		len(input.Tags) > 0 ||
		input.Since > 0 ||
		input.Until > 0 ||
		input.Empty
}

type referenceInput struct {
	Tags  []graphTagPredicate
	Limit int
}

type rankedEventsInput struct {
	References      chstore.AggregateInput
	Candidates      chstore.EventQueryInput
	Target          chstore.EventQueryInput
	RankVia         graphTagPredicate
	WeightedTerms   []weightedRankTerm
	CandidateBoosts []candidatePubkeyBoost
	Shuffle         shuffleSpec
	// ViewerPubkey is the requesting account, carried on every ranked request so
	// future personalization (viewer-specific boosts, mutes, vertex weighting)
	// can read it without a client/schema change. Optional and currently advisory.
	ViewerPubkey string
	Limit        int
	Offset       int
}

type weightedRankTermKind int

const (
	weightedRankTermReferences weightedRankTermKind = iota
	weightedRankTermPubkeyScore
	weightedRankTermCandidateField
	weightedRankTermDerivedMetric
)

type weightedRankTerm struct {
	Kind            weightedRankTermKind
	References      chstore.EventQueryInput
	Via             graphTagPredicate
	Metric          genericMetric
	PubkeyScore     pubkeyScoreRankTerm
	CandidateField  string
	DerivedMetric   string
	Weight          float64
	Transform       string
	HalfLifeSeconds float64
}

type candidatePubkeyBoost struct {
	PubKeys map[string]struct{}
	Weight  float64
}

type pubkeyScoreRankTerm struct {
	Source       string
	Target       string
	MinFollowers uint64
	Fallback     float64
}

type shuffleSpec struct {
	Seed     string
	Counter  int
	Strength float64
}

func parseReferenceInput(raw any) referenceInput {
	input := referenceInput{Limit: 20}
	if m, ok := raw.(map[string]any); ok {
		input.Tags = graphTagPredicates(m["tags"])
		input.Limit = intValue(m["limit"], 20)
	}
	if len(input.Tags) == 0 {
		input.Tags = []graphTagPredicate{{Key: "e", Index: -1}, {Key: "q", Index: -1}}
	}
	if input.Limit <= 0 || input.Limit > 100 {
		input.Limit = 20
	}
	return input
}

func (r *resolver) reverseReferenceQuery(ctx context.Context, event chstore.EventView, raw any) (chstore.EventQueryInput, error) {
	m, _ := raw.(map[string]any)
	var input chstore.EventQueryInput
	if eventsRaw, ok := m["events"].(map[string]any); ok {
		parsed, err := r.parseEventQueryInputForSourceEvent(ctx, eventsRaw, event)
		if err != nil {
			return input, err
		}
		input = parsed
	}
	via := graphTagPredicateFrom(m["via"])
	target := targetValue(event, stringValue(m["target"]))
	if via.Value != "" {
		target = via.Value
	}
	if target == "" {
		input.Limit = 0
		return input, nil
	}
	if via.Key == "" {
		via.Key = "e"
	}
	input.Limit = uint64(intValue(m["limit"], int(input.Limit)))
	if input.Limit == 0 || input.Limit > 500 {
		input.Limit = 50
	}
	input.Offset = uint64(intValue(m["offset"], int(input.Offset)))
	if via.DirectReplies {
		childIDs, err := r.store.RefSourceIDs(ctx, target)
		if err != nil {
			return input, err
		}
		if len(childIDs) == 0 {
			input.Empty = true
			input.Limit = 0
			return input, nil
		}
		input.IDs = childIDs
		return input, nil
	}
	input.Tags = append(input.Tags, chstore.TagFilter{Key: via.Key, Value: target})
	return input, nil
}

func (r *resolver) parseRankedEventsInput(ctx context.Context, raw any) (rankedEventsInput, error) {
	m, _ := raw.(map[string]any)
	var out rankedEventsInput
	referencesRaw, ok := m["references"].(map[string]any)
	if !ok {
		return out, fmt.Errorf("references input is required")
	}
	references, err := r.parseEventQueryInput(ctx, referencesRaw)
	if err != nil {
		return out, err
	}
	via := graphTagPredicateFrom(m["via"])
	if via.Key == "" {
		return out, fmt.Errorf("via.key is required")
	}
	limit := intValue(m["limit"], 30)
	if limit <= 0 || limit > 100 {
		limit = 30
	}
	offset := intValue(m["offset"], 0)
	if offset < 0 {
		offset = 0
	}
	aggregateLimit := limit + offset
	if aggregateLimit <= 0 || aggregateLimit > 1000 {
		aggregateLimit = limit
	}
	out = rankedEventsInput{
		References: chstore.AggregateInput{
			Dataset: "TAGS",
			GroupBy: []string{"TAG_VALUE"},
			Metrics: []string{rankMetricName(m["metric"])},
			IDs:     references.IDs,
			PubKeys: references.PubKeys,
			Kinds:   references.Kinds,
			Tags: append(references.Tags, chstore.TagFilter{
				Key: via.Key, Value: via.Value, Values: via.Values,
			}),
			Since:       references.Since,
			Until:       references.Until,
			Limit:       uint64(aggregateLimit),
			PubkeyScore: references.PubkeyScore,
			Empty:       references.Empty,
		},
		Candidates: chstore.EventQueryInput{
			Kinds: references.Kinds,
			Limit: uint64(aggregateLimit),
		},
		Target:  chstore.EventQueryInput{Limit: uint64(limit)},
		RankVia: via,
		Shuffle: shuffleInput(m["shuffle"]),
		Limit:   limit,
		Offset:  offset,
	}
	if targetRaw, ok := m["target"].(map[string]any); ok {
		target, err := r.parseEventQueryInput(ctx, targetRaw)
		if err != nil {
			return out, err
		}
		out.Target = target
	}
	out.Target.Limit = uint64(limit)
	out.WeightedTerms, err = weightedRankTerms(ctx, m, r.parseEventQueryInput, r.pubkeyScoreMinFollowers)
	if err != nil {
		return out, err
	}
	out.CandidateBoosts, err = r.candidatePubkeyBoosts(ctx, m["candidatePubkeyBoosts"])
	if err != nil {
		return out, err
	}
	// Optional viewer pubkey, carried for forward-compatible personalization. We
	// accept and normalize it but never fail the request on it.
	if pubkey := strings.ToLower(strings.TrimSpace(stringValue(m["pubkey"]))); pubkey != "" && validateHex64(pubkey) == nil {
		out.ViewerPubkey = pubkey
	}
	return out, nil
}

func rankMetricName(raw any) string {
	metric := genericMetric{}
	if m, ok := raw.(map[string]any); ok {
		metric = genericMetric{
			Op:            strings.ToUpper(stringValue(m["op"])),
			Field:         stringValue(m["field"]),
			DistinctField: stringValue(m["distinctField"]),
		}
	}
	if metric.Op == "" {
		return "UNIQUE_PUBKEYS"
	}
	switch metric.Op {
	case "COUNT":
		return "COUNT"
	case "COUNT_DISTINCT":
		field := strings.ToUpper(metric.DistinctField)
		if field == "" {
			field = strings.ToUpper(metric.Field)
		}
		switch field {
		case "ID", "EVENT_ID":
			return "UNIQUE_EVENTS"
		default:
			return "UNIQUE_PUBKEYS"
		}
	default:
		return "UNIQUE_PUBKEYS"
	}
}

func serviceInfo() map[string]any {
	return capabilities.ServiceInfo()
}

func legacyRankMetricSupported(raw any) bool {
	metric := genericMetricFromRaw(raw, "value")
	switch metric.Op {
	case "", "COUNT", "COUNT_DISTINCT":
		return true
	default:
		return false
	}
}

func weightedRankTerms(
	ctx context.Context,
	rankRaw map[string]any,
	parseEventQuery func(context.Context, map[string]any) (chstore.EventQueryInput, error),
	defaultPubkeyScoreMinFollowers uint64,
) ([]weightedRankTerm, error) {
	useWeighted := len(anyList(rankRaw["terms"])) > 0 ||
		len(anyList(rankRaw["candidatePubkeyBoosts"])) > 0 ||
		shuffleInput(rankRaw["shuffle"]).Seed != "" ||
		rankRaw["pubkeyScore"] != nil ||
		stringValue(rankRaw["candidateField"]) != "" ||
		stringValue(rankRaw["derivedMetric"]) != "" ||
		!legacyRankMetricSupported(rankRaw["metric"]) ||
		floatValue(rankRaw["weight"], 1) != 1 ||
		rankTransform(rankRaw["transform"]) != "IDENTITY"
	if !useWeighted {
		return nil, nil
	}

	base, err := weightedRankTermFrom(ctx, rankRaw, parseEventQuery, defaultPubkeyScoreMinFollowers)
	if err != nil {
		return nil, err
	}
	terms := []weightedRankTerm{base}
	for _, value := range anyList(rankRaw["terms"]) {
		raw, ok := value.(map[string]any)
		if !ok {
			continue
		}
		term, err := weightedRankTermFrom(ctx, raw, parseEventQuery, defaultPubkeyScoreMinFollowers)
		if err != nil {
			return nil, err
		}
		terms = append(terms, term)
	}
	return terms, nil
}

func weightedRankTermFrom(
	ctx context.Context,
	raw map[string]any,
	parseEventQuery func(context.Context, map[string]any) (chstore.EventQueryInput, error),
	defaultPubkeyScoreMinFollowers uint64,
) (weightedRankTerm, error) {
	out := weightedRankTerm{
		Kind:            weightedRankTermReferences,
		Weight:          floatValue(raw["weight"], 1),
		Transform:       rankTransform(raw["transform"]),
		HalfLifeSeconds: floatValue(raw["halfLifeSeconds"], 86400),
	}
	if out.HalfLifeSeconds <= 0 {
		out.HalfLifeSeconds = 86400
	}
	if pubkeyScoreRaw, ok := raw["pubkeyScore"].(map[string]any); ok {
		out.Kind = weightedRankTermPubkeyScore
		out.PubkeyScore = pubkeyScoreRankTermFrom(pubkeyScoreRaw, defaultPubkeyScoreMinFollowers)
		return out, nil
	}
	if candidateField := strings.ToUpper(stringValue(raw["candidateField"])); candidateField != "" {
		out.Kind = weightedRankTermCandidateField
		out.CandidateField = candidateField
		return out, nil
	}
	if derivedMetric := strings.TrimSpace(stringValue(raw["derivedMetric"])); derivedMetric != "" {
		out.Kind = weightedRankTermDerivedMetric
		out.DerivedMetric = derivedMetric
		return out, nil
	}
	referencesRaw, ok := raw["references"].(map[string]any)
	if !ok {
		return out, fmt.Errorf("rank term references input is required")
	}
	references, err := parseEventQuery(ctx, referencesRaw)
	if err != nil {
		return out, err
	}
	via := graphTagPredicateFrom(raw["via"])
	if via.Key == "" {
		return out, fmt.Errorf("rank term via.key is required")
	}
	out.References = references
	out.Via = via
	out.Metric = genericMetricFromRaw(raw["metric"], "value")
	return out, nil
}

func pubkeyScoreRankTermFrom(raw map[string]any, defaultMinFollowers uint64) pubkeyScoreRankTerm {
	source := strings.ToLower(strings.TrimSpace(stringValue(raw["source"])))
	if source == "" {
		source = vertex.PluginName
	}
	target := strings.ToUpper(strings.TrimSpace(stringValue(raw["target"])))
	if target == "" {
		target = "AUTHOR"
	}
	minFollowers := intValue(raw["minFollowers"], int(defaultMinFollowers))
	if minFollowers < 0 {
		minFollowers = 0
	}
	return pubkeyScoreRankTerm{
		Source:       source,
		Target:       target,
		MinFollowers: uint64(minFollowers),
		Fallback:     floatValue(raw["fallback"], 0),
	}
}

func pubkeyScoreFilterFromRaw(raw any) chstore.PubkeyScoreFilter {
	m, ok := raw.(map[string]any)
	if !ok {
		return chstore.PubkeyScoreFilter{}
	}
	source := strings.ToLower(strings.TrimSpace(stringValue(m["source"])))
	if source == "" {
		source = vertex.PluginName
	}
	minFollowers := intValue(m["minFollowers"], 0)
	if minFollowers < 0 {
		minFollowers = 0
	}
	return chstore.PubkeyScoreFilter{
		Source:       source,
		MinFollowers: uint64(minFollowers),
	}
}

func genericMetricFromRaw(raw any, fallbackName string) genericMetric {
	m, _ := raw.(map[string]any)
	name := stringValue(m["name"])
	if name == "" {
		name = fallbackName
	}
	metric := genericMetric{
		Name:          name,
		Op:            strings.ToUpper(stringValue(m["op"])),
		Field:         stringValue(m["field"]),
		TagKey:        stringValue(m["tagKey"]),
		TagIndex:      intValue(m["tagIndex"], 1),
		Derived:       stringValue(m["derived"]),
		DistinctField: stringValue(m["distinctField"]),
	}
	if metric.Op == "" {
		metric.Op = "COUNT_DISTINCT"
	}
	if metric.Op == "COUNT_DISTINCT" && metric.DistinctField == "" && metric.Field == "" && metric.TagKey == "" && metric.Derived == "" {
		metric.DistinctField = "PUBKEY"
	}
	return metric
}

func rankTransform(raw any) string {
	switch strings.ToUpper(stringValue(raw)) {
	case "LOG1P":
		return "LOG1P"
	case "RECENCY_HALFLIFE":
		return "RECENCY_HALFLIFE"
	default:
		return "IDENTITY"
	}
}

func shuffleInput(raw any) shuffleSpec {
	m, _ := raw.(map[string]any)
	if len(m) == 0 {
		return shuffleSpec{}
	}
	strength := floatValue(m["strength"], 0.15)
	if strength < 0 {
		strength = 0
	}
	if strength > 1 {
		strength = 1
	}
	return shuffleSpec{
		Seed:     stringValue(m["seed"]),
		Counter:  intValue(m["counter"], 0),
		Strength: strength,
	}
}

func chstoreShuffleInput(raw any) chstore.ShuffleInput {
	shuffle := shuffleInput(raw)
	return chstore.ShuffleInput{
		Seed:     shuffle.Seed,
		Counter:  shuffle.Counter,
		Strength: shuffle.Strength,
	}
}

func rankedTargetIDs(rows []chstore.AggregateRow, offset, limit int) []string {
	if offset < 0 {
		offset = 0
	}
	if limit <= 0 {
		return nil
	}
	out := make([]string, 0, limit)
	for _, row := range rows {
		id := row.Dimensions["tag_value"]
		if id == "" {
			id = row.Dimensions["TAG_VALUE"]
		}
		if !hex64Pattern.MatchString(id) {
			continue
		}
		if offset > 0 {
			offset--
			continue
		}
		out = append(out, id)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func intersectRankedIDs(rankedIDs, allowedIDs []string) []string {
	allowed := make(map[string]struct{}, len(allowedIDs))
	for _, id := range allowedIDs {
		allowed[id] = struct{}{}
	}
	out := make([]string, 0, len(rankedIDs))
	for _, id := range rankedIDs {
		if _, ok := allowed[id]; ok {
			out = append(out, id)
		}
	}
	return out
}

func rankedCandidateIDs(rows []chstore.AggregateRow, candidates []chstore.EventView, offset, limit int) []string {
	if limit <= 0 {
		return nil
	}
	allowed := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		allowed[candidate.ID] = struct{}{}
	}
	seen := map[string]struct{}{}
	ordered := make([]string, 0, len(candidates))
	for _, id := range rankedTargetIDs(rows, 0, len(candidates)) {
		if _, ok := allowed[id]; !ok {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ordered = append(ordered, id)
	}
	for _, candidate := range candidates {
		if _, ok := seen[candidate.ID]; ok {
			continue
		}
		seen[candidate.ID] = struct{}{}
		ordered = append(ordered, candidate.ID)
	}
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ordered) {
		return nil
	}
	ordered = ordered[offset:]
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	return ordered
}

func useWeightedRanking(terms []weightedRankTerm, boosts []candidatePubkeyBoost, shuffle shuffleSpec) bool {
	return len(terms) > 0 || len(boosts) > 0 || shuffle.Seed != ""
}

type candidateRank struct {
	event chstore.EventView
	score float64
}

func weightedRankCandidateIDs(
	ctx context.Context,
	store Store,
	candidates []chstore.EventView,
	terms []weightedRankTerm,
	boosts []candidatePubkeyBoost,
	shuffle shuffleSpec,
	offset int,
	limit int,
) ([]string, error) {
	if limit <= 0 || len(candidates) == 0 {
		return nil, nil
	}
	baseScores, err := weightedRankBaseScores(ctx, store, candidates, terms)
	if err != nil {
		return nil, err
	}
	if len(baseScores) == 0 {
		return nil, nil
	}
	return finalizeRankedIDs(candidates, baseScores, boosts, shuffle, offset, limit), nil
}

// weightedRankBaseScores computes the per-candidate base score from the weighted
// rank terms only. This is the expensive part of ranking (engagement
// aggregation, pubkey scores, derived metrics) and is independent of the
// requesting viewer and of the shuffle seed, so the result is safe to cache and
// reuse across viewers — the viewer follow-boost and shuffle jitter are applied
// later in finalizeRankedIDs. Returns a score per distinct candidate id (0 when
// no term contributes).
func weightedRankBaseScores(
	ctx context.Context,
	store Store,
	candidates []chstore.EventView,
	terms []weightedRankTerm,
) (map[string]float64, error) {
	scores := make(map[string]float64, len(candidates))
	candidateIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		if candidate.ID == "" {
			continue
		}
		if _, ok := scores[candidate.ID]; ok {
			continue
		}
		scores[candidate.ID] = 0
		candidateIDs = append(candidateIDs, candidate.ID)
	}
	if len(candidateIDs) == 0 {
		return scores, nil
	}
	for _, term := range terms {
		switch term.Kind {
		case weightedRankTermPubkeyScore:
			if err := applyPubkeyScoreRankTerm(ctx, store, candidates, candidateIDs, scores, term); err != nil {
				return nil, err
			}
			continue
		case weightedRankTermCandidateField:
			applyCandidateFieldRankTerm(candidates, scores, term)
			continue
		case weightedRankTermDerivedMetric:
			if err := applyDerivedMetricRankTerm(ctx, store, candidateIDs, scores, term); err != nil {
				return nil, err
			}
			continue
		}
		if term.References.Empty {
			continue
		}
		rowsByCandidate, err := weightedRankRowsByCandidate(ctx, store, candidateIDs, term)
		if err != nil {
			return nil, err
		}
		for _, candidateID := range candidateIDs {
			rows := rowsByCandidate[candidateID]
			if len(rows) == 0 {
				continue
			}
			value := rows[0].Metrics[term.Metric.Name]
			scores[candidateID] += transformedRankValue(value, term.Transform) * term.Weight
		}
	}
	return scores, nil
}

// finalizeRankedIDs applies the per-request layer on top of cached base scores:
// the viewer follow-boost and the deterministic shuffle jitter, then sorts and
// slices to the requested page. Boost and terms both only add to the score, so
// applying the boost here yields an identical final score to folding it into the
// base loop.
func finalizeRankedIDs(
	candidates []chstore.EventView,
	baseScores map[string]float64,
	boosts []candidatePubkeyBoost,
	shuffle shuffleSpec,
	offset int,
	limit int,
) []string {
	if limit <= 0 {
		return nil
	}
	ranked := make([]candidateRank, 0, len(candidates))
	added := map[string]struct{}{}
	for _, candidate := range candidates {
		base, ok := baseScores[candidate.ID]
		if !ok {
			continue
		}
		if _, ok := added[candidate.ID]; ok {
			continue
		}
		added[candidate.ID] = struct{}{}
		score := base
		for _, boost := range boosts {
			if _, ok := boost.PubKeys[candidate.PubKey]; ok {
				score += boost.Weight
			}
		}
		ranked = append(ranked, candidateRank{event: candidate, score: score})
	}
	sort.SliceStable(ranked, func(i, j int) bool {
		scoreI := ranked[i].score + shuffleJitter(ranked[i].event.ID, shuffle)
		scoreJ := ranked[j].score + shuffleJitter(ranked[j].event.ID, shuffle)
		if scoreI != scoreJ {
			return scoreI > scoreJ
		}
		if !ranked[i].event.CreatedAt.Equal(ranked[j].event.CreatedAt) {
			return ranked[i].event.CreatedAt.After(ranked[j].event.CreatedAt)
		}
		return ranked[i].event.ID > ranked[j].event.ID
	})
	if offset < 0 {
		offset = 0
	}
	if offset >= len(ranked) {
		return nil
	}
	ranked = ranked[offset:]
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	out := make([]string, 0, len(ranked))
	for _, entry := range ranked {
		out = append(out, entry.event.ID)
	}
	return out
}

func applyPubkeyScoreRankTerm(
	ctx context.Context,
	store Store,
	candidates []chstore.EventView,
	candidateIDs []string,
	scores map[string]float64,
	term weightedRankTerm,
) error {
	pubkeys := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		pubkeys = append(pubkeys, candidate.PubKey)
	}
	scoreRows, err := store.PubkeyScores(ctx, term.PubkeyScore.Source, pubkeys)
	if err != nil {
		return err
	}
	candidateSet := make(map[string]struct{}, len(candidateIDs))
	for _, id := range candidateIDs {
		candidateSet[id] = struct{}{}
	}
	for _, candidate := range candidates {
		if _, ok := candidateSet[candidate.ID]; !ok {
			continue
		}
		value := term.PubkeyScore.Fallback
		if row, ok := scoreRows[candidate.PubKey]; ok && row.Followers >= term.PubkeyScore.MinFollowers {
			value = row.Score
		}
		scores[candidate.ID] += value * term.Weight
	}
	return nil
}

func applyCandidateFieldRankTerm(candidates []chstore.EventView, scores map[string]float64, term weightedRankTerm) {
	now := time.Now().UTC()
	for _, candidate := range candidates {
		value, ok := candidateFieldValue(candidate, term, now)
		if !ok {
			continue
		}
		scores[candidate.ID] += value * term.Weight
	}
}

func applyDerivedMetricRankTerm(
	ctx context.Context,
	store Store,
	candidateIDs []string,
	scores map[string]float64,
	term weightedRankTerm,
) error {
	values, err := store.DerivedMetricValues(ctx, term.DerivedMetric, candidateIDs)
	if err != nil {
		return err
	}
	for _, candidateID := range candidateIDs {
		value, ok := values[candidateID]
		if !ok {
			continue
		}
		scores[candidateID] += transformedFloatRankValue(value, term.Transform) * term.Weight
	}
	return nil
}

func candidateFieldValue(candidate chstore.EventView, term weightedRankTerm, now time.Time) (float64, bool) {
	switch term.CandidateField {
	case "CREATED_AT":
		if term.Transform == "RECENCY_HALFLIFE" {
			ageSeconds := now.Sub(candidate.CreatedAt).Seconds()
			if ageSeconds < 0 {
				ageSeconds = 0
			}
			return math.Pow(0.5, ageSeconds/term.HalfLifeSeconds), true
		}
		return transformedFloatRankValue(float64(candidate.CreatedAt.Unix()), term.Transform), true
	default:
		return 0, false
	}
}

func weightedRankRowsByCandidate(
	ctx context.Context,
	store Store,
	candidateIDs []string,
	term weightedRankTerm,
) (map[string][]chstore.AggregateRow, error) {
	tag := chstore.TagFilter{Key: term.Via.Key, Value: term.Via.Value, Values: term.Via.Values}
	if aggregateStore, ok := store.(referenceAggregateStore); ok {
		rowsByCandidate, supported, err := aggregateStore.AggregateEventsByTagTargets(ctx, chstore.ReferenceAggregateInput{
			Events:         term.References,
			Tag:            tag,
			Targets:        candidateIDs,
			LimitPerTarget: term.References.Limit,
			Metrics:        referenceAggregateMetrics([]genericMetric{term.Metric}),
			First:          1,
			OrderBy:        term.Metric.Name,
		})
		if err != nil {
			return nil, err
		}
		if supported {
			return rowsByCandidate, nil
		}
	}

	eventsByCandidate, err := store.QueryEventsByTagTargets(ctx, term.References, tag, candidateIDs, term.References.Limit)
	if err != nil {
		return nil, err
	}
	out := make(map[string][]chstore.AggregateRow, len(eventsByCandidate))
	for candidateID, events := range eventsByCandidate {
		out[candidateID] = []chstore.AggregateRow{{
			Dimensions: map[string]string{},
			Metrics:    map[string]uint64{term.Metric.Name: computeMetric(events, term.Metric)},
		}}
	}
	return out, nil
}

func transformedRankValue(value uint64, transform string) float64 {
	return transformedFloatRankValue(float64(value), transform)
}

func transformedFloatRankValue(value float64, transform string) float64 {
	switch transform {
	case "LOG1P":
		return math.Log1p(value)
	default:
		return value
	}
}

func shuffleJitter(id string, shuffle shuffleSpec) float64 {
	if shuffle.Seed == "" || shuffle.Strength <= 0 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write([]byte(shuffle.Seed))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.Itoa(shuffle.Counter)))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(id))
	const maxUint64AsFloat = float64(^uint64(0))
	return (float64(h.Sum64()) / maxUint64AsFloat) * shuffle.Strength
}

func graphTagPredicates(v any) []graphTagPredicate {
	values := anyList(v)
	out := make([]graphTagPredicate, 0, len(values))
	for _, value := range values {
		predicate := graphTagPredicateFrom(value)
		if predicate.Key != "" {
			out = append(out, predicate)
		}
	}
	return out
}

func graphTagPredicateFrom(v any) graphTagPredicate {
	raw, _ := v.(map[string]any)
	return graphTagPredicate{
		Key:            stringValue(raw["key"]),
		Value:          stringValue(raw["value"]),
		Values:         stringList(raw["values"]),
		Marker:         stringValue(raw["marker"]),
		Markers:        stringList(raw["markers"]),
		ExcludeMarkers: stringList(raw["excludeMarkers"]),
		Index:          intValue(raw["index"], -1),
		DirectReplies:  boolValue(raw["directReplies"], false),
	}
}

func referenceAggregateMetrics(values []genericMetric) []chstore.ReferenceAggregateMetric {
	out := make([]chstore.ReferenceAggregateMetric, 0, len(values))
	for _, value := range values {
		out = append(out, chstore.ReferenceAggregateMetric{
			Name:          value.Name,
			Op:            value.Op,
			Field:         value.Field,
			TagKey:        value.TagKey,
			TagIndex:      value.TagIndex,
			Derived:       value.Derived,
			DistinctField: value.DistinctField,
		})
	}
	return out
}

func sourceTagMatches(tag []string, predicate graphTagPredicate) bool {
	if len(tag) == 0 || tag[0] != predicate.Key {
		return false
	}
	if predicate.Index >= 0 && predicate.Index >= len(tag) {
		return false
	}
	if predicate.Value != "" && (len(tag) < 2 || tag[1] != predicate.Value) {
		return false
	}
	if len(predicate.Values) > 0 {
		if len(tag) < 2 {
			return false
		}
		found := false
		for _, value := range predicate.Values {
			if tag[1] == value {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if predicate.Marker != "" && (len(tag) < 4 || tag[3] != predicate.Marker) {
		return false
	}
	if len(predicate.Markers) > 0 {
		if len(tag) < 4 {
			return false
		}
		found := false
		for _, marker := range predicate.Markers {
			if tag[3] == marker {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	if len(predicate.ExcludeMarkers) > 0 && len(tag) >= 4 {
		for _, marker := range predicate.ExcludeMarkers {
			if tag[3] == marker {
				return false
			}
		}
	}
	return true
}

func targetValue(event chstore.EventView, target string) string {
	switch strings.ToUpper(target) {
	case "PUBKEY":
		return event.PubKey
	case "ADDRESS":
		return eventAddress(event)
	default:
		return event.ID
	}
}

func eventAddress(event chstore.EventView) string {
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "d" {
			return fmt.Sprintf("%d:%s:%s", event.Kind, event.PubKey, tag[1])
		}
	}
	if event.Kind == 0 || event.Kind == 3 || (event.Kind >= 10000 && event.Kind < 20000) {
		return fmt.Sprintf("%d:%s:", event.Kind, event.PubKey)
	}
	return ""
}

type selectorParts struct {
	field    string
	tagKey   string
	tagIndex int
	derived  string
}

func computeMetric(events []chstore.EventView, metric genericMetric) uint64 {
	switch metric.Op {
	case "COUNT":
		return uint64(len(events))
	case "COUNT_DISTINCT":
		seen := map[string]struct{}{}
		selector := selectorParts{field: metric.DistinctField}
		if selector.field == "" {
			selector.field = metric.Field
		}
		if selector.field == "" && metric.Derived == "" && metric.TagKey == "" {
			selector.field = "PUBKEY"
		}
		selector.tagKey = metric.TagKey
		selector.tagIndex = metric.TagIndex
		selector.derived = metric.Derived
		for _, event := range events {
			value := selectorString(event, selector)
			if value != "" {
				seen[value] = struct{}{}
			}
		}
		return uint64(len(seen))
	case "SUM", "AVG", "MIN", "MAX":
		var sum uint64
		var minValue uint64
		var maxValue uint64
		var count uint64
		selector := selectorParts{field: metric.Field, tagKey: metric.TagKey, tagIndex: metric.TagIndex, derived: metric.Derived}
		for _, event := range events {
			value := selectorUint(event, selector)
			if metric.Op == "MIN" && (count == 0 || value < minValue) {
				minValue = value
			}
			if metric.Op == "MAX" && value > maxValue {
				maxValue = value
			}
			sum += value
			count++
		}
		switch metric.Op {
		case "AVG":
			if count == 0 {
				return 0
			}
			return sum / count
		case "MIN":
			return minValue
		case "MAX":
			return maxValue
		default:
			return sum
		}
	default:
		return 0
	}
}

func selectorUint(event chstore.EventView, selector selectorParts) uint64 {
	value := selectorString(event, selector)
	if value == "" {
		return 0
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func selectorString(event chstore.EventView, selector selectorParts) string {
	if selector.derived != "" {
		return derivedValue(event, selector.derived)
	}
	if selector.tagKey != "" {
		return tagItem(event.Tags, selector.tagKey, selector.tagIndex)
	}
	switch strings.ToUpper(selector.field) {
	case "ID", "EVENT_ID":
		return event.ID
	case "PUBKEY", "AUTHOR":
		return event.PubKey
	case "KIND":
		return strconv.Itoa(event.Kind)
	case "CREATED_AT":
		return strconv.FormatInt(event.CreatedAt.Unix(), 10)
	case "CONTENT":
		return event.Content
	default:
		return ""
	}
}

func tagItem(tags [][]string, key string, index int) string {
	if index < 0 {
		index = 1
	}
	for _, tag := range tags {
		if len(tag) > index && tag[0] == key {
			return tag[index]
		}
	}
	return ""
}

func derivedValue(event chstore.EventView, key string) string {
	switch strings.ToLower(key) {
	case "nip57.amount_msat":
		return strconv.FormatUint(zapAmountMSats(event), 10)
	case "nip57.amount_sats":
		return strconv.FormatUint(zapAmountMSats(event)/1000, 10)
	case "nip57.sender_pubkey":
		if pubkey := tagItem(event.Tags, "P", 1); pubkey != "" {
			return pubkey
		}
		return zapRequestPubkey(event)
	default:
		return ""
	}
}

func zapAmountMSats(event chstore.EventView) uint64 {
	if event.Kind != 9735 {
		return 0
	}
	if amount := zapRequestAmount(event); amount > 0 {
		return amount
	}
	value := tagItem(event.Tags, "amount", 1)
	if value == "" {
		return 0
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func zapRequestAmount(event chstore.EventView) uint64 {
	var req struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(tagItem(event.Tags, "description", 1)), &req); err != nil {
		return 0
	}
	value := tagItem(req.Tags, "amount", 1)
	if value == "" {
		return 0
	}
	n, err := strconv.ParseUint(value, 10, 64)
	if err != nil {
		return 0
	}
	return n
}

func zapRequestPubkey(event chstore.EventView) string {
	var req struct {
		PubKey string `json:"pubkey"`
	}
	if err := json.Unmarshal([]byte(tagItem(event.Tags, "description", 1)), &req); err != nil {
		return event.PubKey
	}
	if req.PubKey == "" {
		return event.PubKey
	}
	return req.PubKey
}

type pubkeySource struct {
	latestEventTags   *latestEventTagPubkeySource
	sourceEventAuthor bool
}

type latestEventTagPubkeySource struct {
	PubKey    string
	Kinds     []int
	Tag       graphTagPredicate
	Limit     int
	MaxValues int
}

func (r *resolver) queryEvents(ctx context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	if input.Empty {
		return []chstore.EventView{}, nil
	}
	r.hydrateRelayEventQuery(ctx, input, "events")
	return r.store.QueryEvents(ctx, input)
}

func (r *resolver) hydrateAggregateInput(ctx context.Context, input chstore.AggregateInput, label string) bool {
	eventInput := chstore.EventQueryInput{
		IDs:         input.IDs,
		PubKeys:     input.PubKeys,
		Kinds:       input.Kinds,
		Tags:        input.Tags,
		Since:       input.Since,
		Until:       input.Until,
		Limit:       input.Limit,
		Shuffle:     input.Shuffle,
		PubkeyScore: input.PubkeyScore,
		Empty:       input.Empty,
	}
	return r.hydrateRelayEventQuery(ctx, eventInput, label)
}

func (r *resolver) hydrateNotificationInput(ctx context.Context, input chstore.ViewerFeedInput) bool {
	if input.Viewer == "" {
		return false
	}
	return r.hydrateRelayEventQuery(ctx, chstore.EventQueryInput{
		Tags:  []chstore.TagFilter{{Key: "p", Value: input.Viewer}},
		Kinds: []int{1, 3, 6, 7, 16, 9735},
		Since: input.Since,
		Until: input.Until,
		Limit: input.Limit,
	}, "notifications")
}

func (r *resolver) hydrateRelayEventQuery(ctx context.Context, input chstore.EventQueryInput, label string) bool {
	if r.relayEventBackfiller == nil || input.Empty {
		return false
	}
	if !consumeRelayHydrationBudget(ctx) {
		return false
	}
	if hydrator, ok := r.relayEventBackfiller.(RelayEventHydrator); ok {
		completed, err := hydrator.HydrateRelayEvents(ctx, input, label)
		if err != nil {
			slog.Warn("graphql relay hydration failed", "label", label, "error", err)
			return false
		}
		return completed
	}
	if err := r.relayEventBackfiller.BackfillRelayEvents(ctx, input, label); err != nil {
		slog.Warn("graphql relay backfill failed", "label", label, "error", err)
		return false
	}
	return true
}

func (r *resolver) parseEventQueryInput(ctx context.Context, raw map[string]any) (chstore.EventQueryInput, error) {
	input, err := parseEventQueryInput(raw)
	if err != nil {
		return input, err
	}
	sources := pubkeySources(raw["pubkeysFrom"])
	if len(sources) == 0 {
		return input, nil
	}
	derived, err := r.resolvePubkeySources(ctx, sources)
	if err != nil {
		return input, err
	}
	input.PubKeys = uniqueStrings(append(input.PubKeys, derived...))
	if len(input.PubKeys) == 0 {
		input.Empty = true
	}
	return input, nil
}

func (r *resolver) parseEventQueryInputForSourceEvent(ctx context.Context, raw map[string]any, sourceEvent chstore.EventView) (chstore.EventQueryInput, error) {
	input, err := r.parseEventQueryInput(ctx, raw)
	if err != nil {
		return input, err
	}
	if !pubkeySourcesUseSourceEventAuthor(raw["pubkeysFrom"]) {
		return input, nil
	}
	input.PubKeys = uniqueStrings(append(input.PubKeys, sourceEvent.PubKey))
	if len(input.PubKeys) == 0 {
		input.Empty = true
	} else {
		input.Empty = false
	}
	return input, nil
}

func (r *resolver) resolvePubkeySources(ctx context.Context, sources []pubkeySource) ([]string, error) {
	var out []string
	for _, source := range sources {
		if source.latestEventTags == nil {
			continue
		}
		values, err := r.resolveLatestEventTagPubkeys(ctx, *source.latestEventTags)
		if err != nil {
			return nil, err
		}
		out = append(out, values...)
	}
	return uniqueStrings(out), nil
}

func (r *resolver) candidatePubkeyBoosts(ctx context.Context, raw any) ([]candidatePubkeyBoost, error) {
	return candidatePubkeyBoosts(ctx, raw, r.resolvePubkeySources)
}

func (r *resolver) resolveLatestEventTagPubkeys(ctx context.Context, source latestEventTagPubkeySource) ([]string, error) {
	normalized, err := normalizeLatestEventTagPubkeySource(source)
	if err != nil {
		return nil, err
	}
	events, err := r.store.QueryLatestEventsByPubKeys(ctx, []string{normalized.PubKey}, normalized.Kinds, uint64(normalized.Limit))
	if err != nil {
		return nil, err
	}
	return latestEventTagPubkeyValues(events[normalized.PubKey], normalized), nil
}

func normalizeLatestEventTagPubkeySource(source latestEventTagPubkeySource) (latestEventTagPubkeySource, error) {
	if err := validateHex64(source.PubKey); err != nil {
		return source, fmt.Errorf("pubkeysFrom.latestEventTags.pubkey: %w", err)
	}
	if len(source.Kinds) == 0 {
		return source, fmt.Errorf("pubkeysFrom.latestEventTags.kinds is required")
	}
	if source.Tag.Key == "" {
		return source, fmt.Errorf("pubkeysFrom.latestEventTags.tag.key is required")
	}
	if source.Limit <= 0 || source.Limit > 20 {
		source.Limit = 1
	}
	if source.MaxValues <= 0 || source.MaxValues > 5000 {
		source.MaxValues = 2000
	}
	source.Kinds = uniqueInts(source.Kinds)
	source.Tag.Values = uniqueStrings(source.Tag.Values)
	return source, nil
}

func latestEventTagPubkeyValues(latest []chstore.EventView, source latestEventTagPubkeySource) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, min(source.MaxValues, 64))
	for _, event := range latest {
		for _, tag := range event.Tags {
			if !sourceTagMatches(tag, source.Tag) || len(tag) < 2 || !hex64Pattern.MatchString(tag[1]) {
				continue
			}
			if _, ok := seen[tag[1]]; ok {
				continue
			}
			seen[tag[1]] = struct{}{}
			out = append(out, tag[1])
			if len(out) >= source.MaxValues {
				return out
			}
		}
	}
	return out
}

func pubkeySources(v any) []pubkeySource {
	values := anyList(v)
	out := make([]pubkeySource, 0, len(values))
	for _, value := range values {
		raw, ok := value.(map[string]any)
		if !ok {
			continue
		}
		source := pubkeySource{}
		if latestRaw, ok := raw["latestEventTags"].(map[string]any); ok {
			source.latestEventTags = &latestEventTagPubkeySource{
				PubKey:    stringValue(latestRaw["pubkey"]),
				Kinds:     intList(latestRaw["kinds"]),
				Tag:       graphTagPredicateFrom(latestRaw["tag"]),
				Limit:     intValue(latestRaw["limit"], 1),
				MaxValues: intValue(latestRaw["maxValues"], 2000),
			}
		}
		source.sourceEventAuthor = boolValue(raw["sourceEventAuthor"], false)
		if source.latestEventTags != nil || source.sourceEventAuthor {
			out = append(out, source)
		}
	}
	return out
}

func pubkeySourcesUseSourceEventAuthor(v any) bool {
	for _, source := range pubkeySources(v) {
		if source.sourceEventAuthor {
			return true
		}
	}
	return false
}

func Handler(schema graphql.Schema, opts ...HandlerOption) http.HandlerFunc {
	cfg := handlerConfig{requestTimeout: 10 * time.Second, relayHydrationMaxJobs: 4}
	for _, opt := range opts {
		opt(&cfg)
	}
	type request struct {
		Query         string         `json:"query"`
		OperationName string         `json:"operationName"`
		Variables     map[string]any `json:"variables"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		capabilities.WriteHeaders(w)
		if r.Method != http.MethodPost {
			http.Error(w, "POST /graphql only", http.StatusMethodNotAllowed)
			return
		}
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		operation := graphqlRequestOperationName(req.OperationName, req.Query)
		started := time.Now()
		slog.Info(
			"graphql request started",
			"operation", operation,
			"variables", len(req.Variables),
			"query_bytes", len(req.Query),
		)
		ctx, cancel := context.WithTimeout(r.Context(), cfg.requestTimeout)
		defer cancel()
		ctx = context.WithValue(ctx, relayHydrationBudgetKey{}, &relayHydrationBudget{remaining: cfg.relayHydrationMaxJobs})
		result := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  req.Query,
			OperationName:  req.OperationName,
			VariableValues: req.Variables,
			Context:        ctx,
		})
		duration := time.Since(started)
		attrs := []any{
			"operation", operation,
			"duration_ms", duration.Milliseconds(),
			"errors", len(result.Errors),
			"variables", len(req.Variables),
			"query_bytes", len(req.Query),
		}
		if ctx.Err() != nil {
			attrs = append(attrs, "context_error", ctx.Err().Error())
		}
		if len(result.Errors) > 0 {
			attrs = append(attrs, "messages", graphqlErrorMessages(result.Errors, 3))
			slog.Warn("graphql request completed with errors", attrs...)
		} else if ctx.Err() != nil || duration >= 3*time.Second {
			slog.Warn("graphql request completed slowly", attrs...)
		} else {
			slog.Info("graphql request completed", attrs...)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
}

func graphqlRequestOperationName(operationName, query string) string {
	if strings.TrimSpace(operationName) != "" {
		return operationName
	}
	match := graphqlOperationNamePattern.FindStringSubmatch(query)
	if len(match) >= 2 {
		return match[1]
	}
	return "anonymous"
}

func graphqlErrorMessages(errors []gqlerrors.FormattedError, limit int) []string {
	if limit <= 0 || len(errors) == 0 {
		return nil
	}
	if len(errors) < limit {
		limit = len(errors)
	}
	messages := make([]string, 0, limit)
	for _, err := range errors[:limit] {
		messages = append(messages, err.Message)
	}
	return messages
}

func parseEventQueryInput(raw map[string]any) (chstore.EventQueryInput, error) {
	input := chstore.EventQueryInput{
		IDs:            stringList(raw["ids"]),
		PubKeys:        stringList(raw["pubkeys"]),
		ExcludeIDs:     stringList(raw["excludeIds"]),
		ExcludePubKeys: stringList(raw["excludePubkeys"]),
		Kinds:          intList(raw["kinds"]),
		Tags:           tagFilters(raw["tags"]),
		Search:         strings.TrimSpace(stringValue(raw["search"])),
		Since:          int64(intValue(raw["since"], 0)),
		Until:          int64(intValue(raw["until"], 0)),
		Limit:          uint64(intValue(raw["limit"], 50)),
		Offset:         uint64(intValue(raw["offset"], 0)),
		Shuffle:        chstoreShuffleInput(raw["shuffle"]),
		PubkeyScore:    pubkeyScoreFilterFromRaw(raw["pubkeyScore"]),
	}
	if max := intValue(raw["maxContentLength"], 0); max > 0 {
		input.MaxContentLength = uint64(max)
	}
	if input.Search != "" && len(input.Search) < 3 {
		return input, fmt.Errorf("events search must be at least 3 characters")
	}
	return input, validateHexFilters(
		append(append([]string(nil), input.IDs...), input.ExcludeIDs...),
		append(append([]string(nil), input.PubKeys...), input.ExcludePubKeys...),
	)
}

func parseNotificationInput(raw map[string]any) (chstore.ViewerFeedInput, error) {
	input := chstore.ViewerFeedInput{
		Tab:        "ALL",
		Policy:     "STRICT",
		ReplyScope: "THREAD",
		Limit:      50,
	}
	if raw == nil {
		return input, fmt.Errorf("notification pubkey is required")
	}
	input.Viewer = strings.ToLower(strings.TrimSpace(stringValue(raw["pubkey"])))
	if err := validateHex64(input.Viewer); err != nil {
		return input, fmt.Errorf("notification pubkey: %w", err)
	}
	if tab := strings.ToUpper(strings.TrimSpace(stringValue(raw["tab"]))); tab == "ALL" || tab == "MENTIONS" {
		input.Tab = tab
	}
	if policy := strings.ToUpper(strings.TrimSpace(stringValue(raw["policy"]))); policy == "RELAXED" || policy == "MODERATE" || policy == "STRICT" || policy == "FOLLOWS" {
		input.Policy = policy
	}
	if replyScope := strings.ToUpper(strings.TrimSpace(stringValue(raw["replyScope"]))); replyScope == "DIRECT" || replyScope == "THREAD" {
		input.ReplyScope = replyScope
	}
	input.Since = int64(intValue(raw["since"], 0))
	input.Until = int64(intValue(raw["until"], 0))
	if limit := intValue(raw["limit"], 50); limit > 0 {
		input.Limit = uint64(limit)
	}
	return input, nil
}

func parseProfileSearchInput(raw map[string]any) (vertex.SearchArgs, error) {
	input := vertex.NormalizeSearchArgs(vertex.SearchArgs{
		Query:  stringValue(raw["query"]),
		Limit:  intValue(raw["limit"], 10),
		Sort:   stringValue(raw["sort"]),
		Source: stringValue(raw["source"]),
	})
	if len(input.Query) < 3 {
		return input, fmt.Errorf("profileSearch query must be at least 3 characters")
	}
	return input, nil
}

func parseAggregateInput(raw map[string]any) (chstore.AggregateInput, error) {
	input := chstore.AggregateInput{
		Dataset:     fmt.Sprint(raw["dataset"]),
		GroupBy:     stringList(raw["groupBy"]),
		Metrics:     stringList(raw["metrics"]),
		IDs:         stringList(raw["ids"]),
		PubKeys:     stringList(raw["pubkeys"]),
		Kinds:       intList(raw["kinds"]),
		Tags:        tagFilters(raw["tags"]),
		Since:       int64(intValue(raw["since"], 0)),
		Until:       int64(intValue(raw["until"], 0)),
		Limit:       uint64(intValue(raw["limit"], 100)),
		Shuffle:     chstoreShuffleInput(raw["shuffle"]),
		PubkeyScore: pubkeyScoreFilterFromRaw(raw["pubkeyScore"]),
	}
	return input, validateHexFilters(input.IDs, input.PubKeys)
}

func validateHexFilters(ids, pubkeys []string) error {
	for _, id := range ids {
		if err := validateHex64(id); err != nil {
			return fmt.Errorf("ids: %w", err)
		}
	}
	for _, pubkey := range pubkeys {
		if err := validateHex64(pubkey); err != nil {
			return fmt.Errorf("pubkeys: %w", err)
		}
	}
	return nil
}

func candidatePubkeyBoosts(
	ctx context.Context,
	raw any,
	resolveSources func(context.Context, []pubkeySource) ([]string, error),
) ([]candidatePubkeyBoost, error) {
	values := anyList(raw)
	out := make([]candidatePubkeyBoost, 0, len(values))
	for _, value := range values {
		m, ok := value.(map[string]any)
		if !ok {
			continue
		}
		pubkeys := stringList(m["pubkeys"])
		if sources := pubkeySources(m["pubkeysFrom"]); len(sources) > 0 {
			derived, err := resolveSources(ctx, sources)
			if err != nil {
				return nil, err
			}
			pubkeys = append(pubkeys, derived...)
		}
		pubkeys = uniqueStrings(pubkeys)
		if len(pubkeys) == 0 {
			continue
		}
		if err := validateHexFilters(nil, pubkeys); err != nil {
			return nil, err
		}
		set := make(map[string]struct{}, len(pubkeys))
		for _, pubkey := range pubkeys {
			set[pubkey] = struct{}{}
		}
		out = append(out, candidatePubkeyBoost{
			PubKeys: set,
			Weight:  floatValue(m["weight"], 1),
		})
	}
	return out, nil
}

func tagFilters(v any) []chstore.TagFilter {
	values := anyList(v)
	out := make([]chstore.TagFilter, 0, len(values))
	for _, value := range values {
		raw, ok := value.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, chstore.TagFilter{
			Key:           fmt.Sprint(raw["key"]),
			Value:         stringValue(raw["value"]),
			Values:        stringList(raw["values"]),
			ExcludeValues: stringList(raw["excludeValues"]),
			Dataset:       strings.ToUpper(strings.TrimSpace(stringValue(raw["dataset"]))),
		})
	}
	return out
}

func stringList(v any) []string {
	values := anyList(v)
	out := make([]string, 0, len(values))
	for _, value := range values {
		if s := stringValue(value); s != "" {
			out = append(out, s)
		}
	}
	return out
}

func intList(v any) []int {
	values := anyList(v)
	out := make([]int, 0, len(values))
	for _, value := range values {
		switch n := value.(type) {
		case int:
			out = append(out, n)
		case int32:
			out = append(out, int(n))
		case int64:
			out = append(out, int(n))
		case float64:
			out = append(out, int(n))
		}
	}
	return out
}

func anyList(v any) []any {
	switch values := v.(type) {
	case []any:
		return values
	case nil:
		return nil
	default:
		return nil
	}
}

func stringValue(v any) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func intValue(v any, fallback int) int {
	switch n := v.(type) {
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	case float64:
		return int(n)
	default:
		return fallback
	}
}

func floatValue(v any, fallback float64) float64 {
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return fallback
	}
}

func boolValue(v any, fallback bool) bool {
	if b, ok := v.(bool); ok {
		return b
	}
	return fallback
}

func eventEndCursor(events []chstore.EventView) any {
	if len(events) == 0 {
		return nil
	}
	last := events[len(events)-1]
	return last.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + last.ID
}

func eventField(fn func(chstore.EventView) any) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		event, ok := eventFromSource(p.Source)
		if !ok {
			return nil, nil
		}
		return fn(event), nil
	}
}

func profileSearchResultField(fn func(profileSearchResultNode) any) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		switch node := p.Source.(type) {
		case profileSearchResultNode:
			return fn(node), nil
		case *profileSearchResultNode:
			if node != nil {
				return fn(*node), nil
			}
		}
		return nil, nil
	}
}

func profileSearchConnectionField(fn func(profileSearchConnectionSource) any) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		source, ok := p.Source.(profileSearchConnectionSource)
		if !ok {
			return nil, nil
		}
		return fn(source), nil
	}
}

func profileSearchLegacyRank(node profileSearchResultNode) any {
	if node.vertex != nil {
		return node.vertex.Rank
	}
	return floatPtrValue(node.row.Rank)
}

func profileSearchLegacyScore(node profileSearchResultNode) any {
	if node.vertex != nil && node.vertex.Score != nil {
		return *node.vertex.Score
	}
	return floatPtrValue(node.row.Score)
}

func profileSearchCreatedAt(node profileSearchResultNode) any {
	if node.vertex != nil && node.vertex.CreatedAt != nil {
		return time.Unix(*node.vertex.CreatedAt, 0).UTC()
	}
	if !node.profile.CreatedAt.IsZero() {
		return node.profile.CreatedAt.UTC()
	}
	return nil
}

func floatPtrValue(value *float64) any {
	if value == nil {
		return nil
	}
	return *value
}

func uintPtrIntValue(value *uint64) any {
	if value == nil {
		return nil
	}
	maxInt := int(^uint(0) >> 1)
	if *value > uint64(maxInt) {
		return maxInt
	}
	return int(*value)
}

func emptyStringNil(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func eventFromSource(source any) (chstore.EventView, bool) {
	switch value := source.(type) {
	case eventNode:
		return value.event, true
	case *eventNode:
		return value.event, true
	case chstore.EventView:
		return value, true
	case *chstore.EventView:
		if value != nil {
			return *value, true
		}
	}
	return chstore.EventView{}, false
}

func asEventNode(source any) (eventNode, bool) {
	switch value := source.(type) {
	case eventNode:
		return value, true
	case *eventNode:
		if value != nil {
			return *value, true
		}
	}
	return eventNode{}, false
}

func (r *resolver) newEventConnection(events []chstore.EventView) eventConnectionSource {
	return newEventConnectionWithPubkeyScoreMinFollowers(r.store, events, r.pubkeyScoreMinFollowers)
}

func newEventConnection(store Store, events []chstore.EventView) eventConnectionSource {
	return newEventConnectionWithPubkeyScoreMinFollowers(store, events, defaultPubkeyScoreMinFollowers)
}

func newEventConnectionWithPubkeyScoreMinFollowers(store Store, events []chstore.EventView, pubkeyScoreMinFollowers uint64) eventConnectionSource {
	relations := newPubkeyRelationCache(store, events)
	eventRelations := newEventRelationCacheWithPubkeyScoreMinFollowers(store, events, pubkeyScoreMinFollowers)
	return newEventConnectionWithCaches(events, relations, eventRelations)
}

func newEventConnectionWithCaches(events []chstore.EventView, relations *pubkeyRelationCache, eventRelations *eventRelationCache) eventConnectionSource {
	return eventConnectionSource{raw: events, nodes: wrapEvents(events, relations, eventRelations)}
}

func newNotificationConnection(store Store, rows []chstore.ViewerFeedRow, pubkeyScoreMinFollowers uint64) notificationConnectionSource {
	events := make([]chstore.EventView, 0, len(rows))
	for _, row := range rows {
		events = append(events, row.Event)
	}
	relations := newPubkeyRelationCache(store, events)
	eventRelations := newEventRelationCacheWithPubkeyScoreMinFollowers(store, events, pubkeyScoreMinFollowers)
	nodes := make([]notificationNode, 0, len(rows))
	for _, row := range rows {
		nodes = append(nodes, notificationNode{
			row:   row,
			event: wrapEvent(row.Event, relations, eventRelations),
		})
	}
	return notificationConnectionSource{rows: rows, nodes: nodes}
}

func newEventConnectionCachesWithPubkeyScoreMinFollowers(store Store, grouped map[string][]chstore.EventView, pubkeyScoreMinFollowers uint64) eventConnectionCaches {
	events := uniqueGroupedEvents(grouped)
	return eventConnectionCaches{
		relations:      newPubkeyRelationCache(store, events),
		eventRelations: newEventRelationCacheWithPubkeyScoreMinFollowers(store, events, pubkeyScoreMinFollowers),
	}
}

func uniqueGroupedEvents(grouped map[string][]chstore.EventView) []chstore.EventView {
	events := make([]chstore.EventView, 0)
	seen := map[string]struct{}{}
	for _, group := range grouped {
		for _, event := range group {
			if event.ID == "" {
				continue
			}
			if _, ok := seen[event.ID]; ok {
				continue
			}
			seen[event.ID] = struct{}{}
			events = append(events, event)
		}
	}
	return events
}

func wrapEvent(event chstore.EventView, relations *pubkeyRelationCache, eventRelations *eventRelationCache) eventNode {
	return eventNode{event: event, relations: relations, eventRelations: eventRelations}
}

func wrapEvents(events []chstore.EventView, relations *pubkeyRelationCache, eventRelations *eventRelationCache) []eventNode {
	out := make([]eventNode, 0, len(events))
	for _, event := range events {
		out = append(out, wrapEvent(event, relations, eventRelations))
	}
	return out
}

func newEventRelationCacheWithPubkeyScoreMinFollowers(store Store, events []chstore.EventView, pubkeyScoreMinFollowers uint64) *eventRelationCache {
	return &eventRelationCache{
		store:                   store,
		events:                  append([]chstore.EventView(nil), events...),
		pubkeyScoreMinFollowers: pubkeyScoreMinFollowers,
		latestEventTags:         map[string][]string{},
		followedReplies:         map[string]map[string][]chstore.EventView{},
		followedConnections:     map[string]eventConnectionCaches{},
	}
}

func (c *eventRelationCache) loadFollowedReply(ctx context.Context, r *resolver, event chstore.EventView, viewer string) (eventConnectionSource, error) {
	if c == nil {
		return r.eventFollowedReply(ctx, event, viewer)
	}
	key := "followedReply:" + viewer

	c.mu.Lock()
	cached, ok := c.followedReplies[key]
	connectionCaches := c.followedConnections[key]
	c.mu.Unlock()
	if !ok {
		value, err, _ := c.group.Do(key, func() (any, error) {
			c.mu.Lock()
			if existing, exists := c.followedReplies[key]; exists {
				connectionCaches = c.followedConnections[key]
				c.mu.Unlock()
				return existing, nil
			}
			c.mu.Unlock()

			started := time.Now()
			loaded, err := c.loadFollowedReplyBatch(ctx, r, viewer)
			if err != nil {
				return nil, err
			}

			c.mu.Lock()
			c.followedReplies[key] = loaded
			c.followedConnections[key] = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, loaded, c.pubkeyScoreMinFollowers)
			c.mu.Unlock()

			slog.Debug(
				"graphql batched followed reply loaded",
				"parents", len(c.events),
				"results", eventViewMapLen(loaded),
				"duration_ms", time.Since(started).Milliseconds(),
			)
			return loaded, nil
		})
		if err != nil {
			return eventConnectionSource{}, err
		}
		cached, _ = value.(map[string][]chstore.EventView)
		c.mu.Lock()
		connectionCaches = c.followedConnections[key]
		c.mu.Unlock()
	}
	if connectionCaches.relations == nil || connectionCaches.eventRelations == nil {
		connectionCaches = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, cached, c.pubkeyScoreMinFollowers)
		c.mu.Lock()
		c.followedConnections[key] = connectionCaches
		c.mu.Unlock()
	}
	return newEventConnectionWithCaches(cached[event.ID], connectionCaches.relations, connectionCaches.eventRelations), nil
}

func (c *eventRelationCache) loadFollowedReplyBatch(ctx context.Context, r *resolver, viewer string) (map[string][]chstore.EventView, error) {
	out := make(map[string][]chstore.EventView, len(c.events))
	for _, event := range c.events {
		out[event.ID] = nil
	}
	if viewer == "" {
		return out, nil
	}
	parentIDs := make([]string, 0, len(c.events))
	for _, event := range c.events {
		parentIDs = append(parentIDs, event.ID)
	}
	byParent, err := c.store.FollowedRefs(ctx, viewer, parentIDs)
	if err != nil {
		return nil, err
	}
	if len(byParent) == 0 {
		return out, nil
	}
	replyIDs := make([]string, 0, len(byParent))
	for _, replyID := range byParent {
		replyIDs = append(replyIDs, replyID)
	}
	fetched, err := c.queryEventsByIDs(ctx, r, replyIDs)
	if err != nil {
		return nil, err
	}
	for parentID, replyID := range byParent {
		if reply, ok := fetched[replyID]; ok {
			out[parentID] = []chstore.EventView{reply}
		}
	}
	return out, nil
}

func (c *eventRelationCache) queryEventsByIDs(ctx context.Context, r *resolver, ids []string) (map[string]chstore.EventView, error) {
	ids = uniqueStrings(ids)
	out := make(map[string]chstore.EventView, len(ids))
	for start := 0; start < len(ids); start += 500 {
		end := start + 500
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		query := chstore.EventQueryInput{IDs: batch, Limit: uint64(len(batch))}
		var events []chstore.EventView
		var err error
		if r != nil {
			events, err = r.queryEvents(ctx, query)
		} else {
			events, err = c.store.QueryEvents(ctx, query)
		}
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			out[event.ID] = event
		}
	}
	return out, nil
}

func newPubkeyRelationCache(store Store, events []chstore.EventView) *pubkeyRelationCache {
	pubkeys := make([]string, 0, len(events))
	seen := map[string]struct{}{}
	for _, event := range events {
		if event.PubKey == "" {
			continue
		}
		if _, ok := seen[event.PubKey]; ok {
			continue
		}
		seen[event.PubKey] = struct{}{}
		pubkeys = append(pubkeys, event.PubKey)
	}
	sort.Strings(pubkeys)
	return &pubkeyRelationCache{
		store:   store,
		pubkeys: pubkeys,
		cache:   map[pubkeyRelationKey]map[string][]chstore.EventView{},
	}
}

func (c *pubkeyRelationCache) load(ctx context.Context, pubkey string, kinds []int, limit int) ([]eventNode, error) {
	if c == nil || pubkey == "" {
		return []eventNode{}, nil
	}
	if limit <= 0 || limit > 20 {
		limit = 1
	}
	key := pubkeyRelationKey{kindSignature(uniqueInts(kinds)), limit}

	c.mu.Lock()
	results, ok := c.cache[key]
	c.mu.Unlock()
	if !ok {
		loaded, err := c.store.QueryLatestEventsByPubKeys(ctx, c.pubkeys, uniqueInts(kinds), uint64(limit))
		if err != nil {
			return nil, err
		}
		c.mu.Lock()
		if existing, exists := c.cache[key]; exists {
			results = existing
		} else {
			c.cache[key] = loaded
			results = loaded
		}
		c.mu.Unlock()
	}
	related := results[pubkey]
	return newEventConnection(c.store, related).nodes, nil
}

func uniqueInts(values []int) []int {
	seen := map[int]struct{}{}
	out := make([]int, 0, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Ints(out)
	return out
}

func uniqueStrings(values []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(values))
	for _, value := range values {
		if value == "" {
			continue
		}
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		out = append(out, value)
	}
	sort.Strings(out)
	return out
}

func eventViewMapLen(values map[string][]chstore.EventView) int {
	var total int
	for _, events := range values {
		total += len(events)
	}
	return total
}

func kindSignature(values []int) string {
	if len(values) == 0 {
		return "*"
	}
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, fmt.Sprintf("%d", value))
	}
	return strings.Join(parts, ",")
}

func validateHex64(value string) error {
	if !hex64Pattern.MatchString(value) {
		return fmt.Errorf("expected lowercase 64-char hex")
	}
	return nil
}

func jsonScalar(name string) *graphql.Scalar {
	return graphql.NewScalar(graphql.ScalarConfig{
		Name: name,
		Serialize: func(value any) any {
			return value
		},
		ParseValue: func(value any) any {
			return value
		},
		ParseLiteral: func(valueAST ast.Value) any {
			return nil
		},
	})
}

func displayReasonForEvent(event chstore.EventView, kind int) string {
	if event.Kind == 0 && kind != 0 {
		event.Kind = kind
	}
	switch event.Kind {
	case 3:
		return "follow"
	case 1:
		if notificationHasReplyReference(event.Tags) {
			return "reply"
		}
		if notificationHasQuoteReference(event.Tags) {
			return "quote"
		}
		return "mention"
	case 6, 16:
		return "repost"
	case 7:
		return "reaction"
	case 9735:
		return "zap"
	default:
		return "mention"
	}
}

func notificationHasReplyReference(tags [][]string) bool {
	for _, tag := range tags {
		if len(tag) <= 1 || tag[0] != "e" || len(tag[1]) != 64 {
			continue
		}
		if len(tag) < 4 || tag[3] == "" || tag[3] == "root" || tag[3] == "reply" {
			return true
		}
	}
	return false
}

func notificationHasQuoteReference(tags [][]string) bool {
	for _, tag := range tags {
		if len(tag) <= 1 || len(tag[1]) != 64 {
			continue
		}
		if tag[0] == "q" {
			return true
		}
		if tag[0] == "e" && len(tag) >= 4 && tag[3] == "mention" {
			return true
		}
	}
	return false
}
