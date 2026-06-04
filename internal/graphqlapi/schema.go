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
	"golang.org/x/sync/singleflight"
)

type Store interface {
	EventByID(context.Context, string) (*chstore.EventView, error)
	QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error)
	QueryEventsByTagTargets(context.Context, chstore.EventQueryInput, chstore.TagFilter, []string, uint64) (map[string][]chstore.EventView, error)
	QueryLatestEventsByPubKeys(context.Context, []string, []int, uint64) (map[string][]chstore.EventView, error)
	AggregateEvents(context.Context, chstore.AggregateInput) ([]chstore.AggregateRow, error)
	AggregateEventReferencesToTargets(context.Context, chstore.AggregateInput, chstore.EventQueryInput) ([]chstore.AggregateRow, error)
	PubkeyScores(context.Context, string, []string) (map[string]chstore.PubkeyScore, error)
	DerivedMetricValues(context.Context, string, []string) (map[string]float64, error)
	AvailableTopics(context.Context, chstore.EventQueryInput) ([]chstore.TopicRow, error)
	TrendingClusters(context.Context, chstore.TrendingInput) ([]chstore.TrendingClusterRow, error)
	Notifications(context.Context, chstore.NotificationInput) ([]chstore.NotificationRow, error)
	ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error)
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
	pubkeyScoreMinFollowers uint64
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

type Option func(*resolver)

func WithUserFeedBackfill(backfiller UserFeedBackfiller) Option {
	return func(r *resolver) {
		r.userBackfiller = backfiller
	}
}

func WithPubkeyScoreMinFollowers(minFollowers int) Option {
	return func(r *resolver) {
		if minFollowers >= 0 {
			r.pubkeyScoreMinFollowers = uint64(minFollowers)
		}
	}
}

type eventNode struct {
	event          chstore.EventView
	relations      *pubkeyRelationCache
	eventRelations *eventRelationCache
}

type trendingClusterNode struct {
	row   chstore.TrendingClusterRow
	store Store
}

type notificationNode struct {
	row   chstore.NotificationRow
	event eventNode
}

type notificationConnectionSource struct {
	rows  []chstore.NotificationRow
	nodes []notificationNode
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
	aggregateByTarget   map[string]map[string][]chstore.AggregateRow
	selectedReferences  map[string]map[string][]chstore.EventView
	rankedReferencedBy  map[string]map[string][]chstore.EventView
	authoredReplyChains map[string]map[string][]chstore.EventView
	selectedConnections map[string]eventConnectionCaches
	rankedConnections   map[string]eventConnectionCaches
	authoredConnections map[string]eventConnectionCaches
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
	var selectedReferenceInputType *graphql.InputObject
	var reverseReferenceInputType *graphql.InputObject
	var aggregateReferencedByInputType *graphql.InputObject
	var authoredReplyChainInputType *graphql.InputObject
	var pubkeyScoreRankInputType *graphql.InputObject
	var shuffleInputType *graphql.InputObject
	var weightedRankTermInputType *graphql.InputObject
	var candidatePubkeyBoostInputType *graphql.InputObject
	var referenceRankInputType *graphql.InputObject
	var rankedReverseReferenceInputType *graphql.InputObject
	var rankedEventsInputType *graphql.InputObject
	var notificationInputType *graphql.InputObject

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
				"selectedReferences": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(selectedReferenceInputType)}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						node, ok := asEventNode(p.Source)
						if ok && node.eventRelations != nil {
							return node.eventRelations.loadSelectedReferences(p.Context, r, node.event, p.Args["input"])
						}
						event, hasEvent := eventFromSource(p.Source)
						if !hasEvent {
							return eventConnectionSource{}, nil
						}
						return r.eventSelectedReferences(p.Context, event, p.Args["input"])
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
				"rankedReferencedBy": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(rankedReverseReferenceInputType)}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						node, ok := asEventNode(p.Source)
						if ok && node.eventRelations != nil {
							return node.eventRelations.loadRankedReferencedBy(p.Context, r, node.event, p.Args["input"])
						}
						event, hasEvent := eventFromSource(p.Source)
						if !hasEvent {
							return eventConnectionSource{}, nil
						}
						return r.eventRankedReferencedBy(p.Context, event, p.Args["input"])
					},
				},
				"authoredReplyChain": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: authoredReplyChainInputType}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						node, ok := asEventNode(p.Source)
						if ok && node.eventRelations != nil {
							return node.eventRelations.loadAuthoredReplyChain(p.Context, r, node.event, p.Args["input"])
						}
						event, hasEvent := eventFromSource(p.Source)
						if !hasEvent {
							return eventConnectionSource{}, nil
						}
						return r.eventAuthoredReplyChain(p.Context, event, p.Args["input"])
					},
				},
				"aggregateReferencedBy": &graphql.Field{
					Type: graphql.NewNonNull(aggregationResultType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(aggregateReferencedByInputType)}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						node, ok := asEventNode(p.Source)
						if ok && node.eventRelations != nil {
							return node.eventRelations.loadAggregateReferencedBy(p.Context, r, node.event, p.Args["input"])
						}
						event, ok := eventFromSource(p.Source)
						if !ok {
							return map[string]any{"rows": []chstore.AggregateRow{}}, nil
						}
						return r.eventAggregateReferencedBy(p.Context, event, p.Args["input"])
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

	eventContextType := graphql.NewObject(graphql.ObjectConfig{
		Name: "EventContext",
		Fields: graphql.Fields{
			"root": &graphql.Field{Type: graphql.NewNonNull(eventType)},
			"events": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(eventType))),
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

	eventQueryInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "EventQueryInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"ids":         &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"pubkeys":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"pubkeysFrom": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(pubkeySourceInputType))},
			"kinds":       &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
			"tags":        &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(tagFilterType))},
			"since":       &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"until":       &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"limit":       &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 50},
			"offset":      &graphql.InputObjectFieldConfig{Type: graphql.Int},
		},
	})

	referenceInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "EventReferenceInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"tags":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(tagFilterType))},
			"limit": &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 20},
		},
	})
	selectedReferenceInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "SelectedEventReferenceInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"selectors":        &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(tagFilterType))},
			"fallback":         &graphql.InputObjectFieldConfig{Type: tagFilterType},
			"fallbackPosition": &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "FIRST"},
			"limit":            &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 20},
			"maxDepth":         &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 0},
			"excludeSelf":      &graphql.InputObjectFieldConfig{Type: graphql.Boolean, DefaultValue: true},
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
	dimensionInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "GenericDimensionInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"name":     &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"field":    &graphql.InputObjectFieldConfig{Type: graphql.String},
			"tagKey":   &graphql.InputObjectFieldConfig{Type: graphql.String},
			"tagIndex": &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 1},
			"derived":  &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})
	aggregateReferencedByInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ReverseEventReferenceAggregateInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"events":  &graphql.InputObjectFieldConfig{Type: eventQueryInputType},
			"via":     &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			"target":  &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "EVENT_ID"},
			"groupBy": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(dimensionInputType))},
			"metrics": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(metricInputType))},
			"limit":   &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 500},
			"first":   &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 100},
			"orderBy": &graphql.InputObjectFieldConfig{Type: graphql.String},
		},
	})
	pubkeyScoreRankInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "PubkeyScoreRankInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"source":       &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "vertex"},
			"target":       &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "AUTHOR"},
			"minFollowers": &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"fallback":     &graphql.InputObjectFieldConfig{Type: graphql.Float, DefaultValue: 0.0},
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
	referenceRankInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ReferenceRankInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"references":            &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(eventQueryInputType)},
			"via":                   &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			"metric":                &graphql.InputObjectFieldConfig{Type: metricInputType},
			"weight":                &graphql.InputObjectFieldConfig{Type: graphql.Float, DefaultValue: 1.0},
			"transform":             &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "IDENTITY"},
			"terms":                 &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(weightedRankTermInputType))},
			"candidatePubkeyBoosts": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(candidatePubkeyBoostInputType))},
			"shuffle":               &graphql.InputObjectFieldConfig{Type: shuffleInputType},
		},
	})
	rankedReverseReferenceInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "RankedReverseEventReferenceInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"events": &graphql.InputObjectFieldConfig{Type: eventQueryInputType},
			"via":    &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			"target": &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "EVENT_ID"},
			"rank":   &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(referenceRankInputType)},
			"limit":  &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 1},
			"offset": &graphql.InputObjectFieldConfig{Type: graphql.Int},
		},
	})
	authoredReplyChainInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "AuthoredReplyChainInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"events":          &graphql.InputObjectFieldConfig{Type: eventQueryInputType},
			"kinds":           &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
			"pubkeyFrom":      &graphql.InputObjectFieldConfig{Type: pubkeySourceInputType},
			"via":             &graphql.InputObjectFieldConfig{Type: tagFilterType},
			"target":          &graphql.InputObjectFieldConfig{Type: graphql.String, DefaultValue: "EVENT_ID"},
			"maxDepth":        &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 8},
			"maxBranchFanout": &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 32},
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
		},
	})
	rankedEventsInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "RankedEventsInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"references": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(eventQueryInputType)},
			"via":        &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			"target":     &graphql.InputObjectFieldConfig{Type: eventQueryInputType},
			"metric":     &graphql.InputObjectFieldConfig{Type: metricInputType},
			"terms":      &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(weightedRankTermInputType))},
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
	topicType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Topic",
		Fields: graphql.Fields{
			"value":     &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"parent":    &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"label":     &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"isDefault": &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"count":     &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
		},
	})
	trendingWindowEnumType := graphql.NewEnum(graphql.EnumConfig{
		Name: "TrendingWindow",
		Values: graphql.EnumValueConfigMap{
			"H8":  &graphql.EnumValueConfig{Value: "H8"},
			"H24": &graphql.EnumValueConfig{Value: "H24"},
			"D7":  &graphql.EnumValueConfig{Value: "D7"},
		},
	})
	trendingInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TrendingInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"window":   &graphql.InputObjectFieldConfig{Type: trendingWindowEnumType, DefaultValue: "H24"},
			"category": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"limit":    &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 20},
		},
	})
	trendingClusterType := graphql.NewObject(graphql.ObjectConfig{
		Name: "TrendingCluster",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":          &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.ID })},
				"window":      &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.Window })},
				"startedAt":   &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.StartedAt })},
				"category":    &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.Category })},
				"subcategory": &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.Subcategory })},
				"title":       &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.Title })},
				"description": &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.Description })},
				"eventCount":  &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.EventCount })},
				"score":       &graphql.Field{Type: graphql.NewNonNull(graphql.Float), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.Score })},
				"computedAt":  &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime), Resolve: trendingClusterField(func(row chstore.TrendingClusterRow) any { return row.ComputedAt })},
				"sampleEvents": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"limit": &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 3}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						cluster, ok := p.Source.(trendingClusterNode)
						if !ok {
							return eventConnectionSource{}, nil
						}
						limit := intValue(p.Args["limit"], 3)
						if limit <= 0 || limit > 20 {
							limit = 3
						}
						events, err := cluster.store.QueryEvents(p.Context, chstore.EventQueryInput{
							Kinds: []int{1, 1111},
							Tags: []chstore.TagFilter{{
								Key:     "cluster",
								Value:   cluster.row.ID,
								Dataset: "DERIVED_TAGS",
							}},
							Limit: uint64(limit),
						})
						if err != nil {
							return nil, err
						}
						relations := newPubkeyRelationCache(cluster.store, events)
						eventRelations := newEventRelationCache(cluster.store, events)
						return eventConnectionSource{raw: events, nodes: wrapEvents(events, relations, eventRelations)}, nil
					},
				},
			}
		}),
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
		},
	})
	notificationInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "NotificationInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"viewer": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"tab":    &graphql.InputObjectFieldConfig{Type: notificationTabEnumType, DefaultValue: "ALL"},
			"policy": &graphql.InputObjectFieldConfig{Type: notificationPolicyEnumType, DefaultValue: "STRICT"},
			"since":  &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"until":  &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"limit":  &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 50},
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
				Type: graphql.NewNonNull(graphql.String),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					node, _ := p.Source.(notificationNode)
					return node.row.Reason, nil
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
					if r.shouldBackfillAuthorQuery(input.PubKeys, input.IDs, input.Tags, input.Kinds, len(events), input.Limit) {
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
					if r.shouldBackfillAuthorQuery(input.PubKeys, input.IDs, input.Tags, input.Kinds, 0, 1) {
						if _, err := r.hydrateAuthors(p.Context, input.PubKeys, 100); err != nil {
							slog.Warn("graphql aggregate author backfill failed", "pubkeys", input.PubKeys, "error", err)
						}
					}
					rows, err := r.store.AggregateEvents(p.Context, input)
					return map[string]any{"rows": rows}, err
				},
			},
			"availableTopics": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(topicType))),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: eventQueryInputType}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					raw, _ := p.Args["input"].(map[string]any)
					input, err := r.parseEventQueryInput(p.Context, raw)
					if err != nil {
						return nil, err
					}
					return r.store.AvailableTopics(p.Context, input)
				},
			},
			"trending": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(trendingClusterType))),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: trendingInputType}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					raw, _ := p.Args["input"].(map[string]any)
					input := parseTrendingInput(raw)
					rows, err := r.store.TrendingClusters(p.Context, input)
					if err != nil {
						return nil, err
					}
					nodes := make([]trendingClusterNode, 0, len(rows))
					for _, row := range rows {
						nodes = append(nodes, trendingClusterNode{row: row, store: r.store})
					}
					return nodes, nil
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
					rows, err := r.store.Notifications(p.Context, input)
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
			"eventContext": &graphql.Field{
				Type: eventContextType,
				Args: graphql.FieldConfigArgument{
					"id":    &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)},
					"limit": &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 1000},
				},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					id := p.Args["id"].(string)
					if err := validateHex64(id); err != nil {
						return nil, err
					}
					limit := intValue(p.Args["limit"], 1000)
					if limit <= 0 || limit > 2000 {
						limit = 1000
					}
					return r.eventContext(p.Context, id, limit)
				},
			},
		},
	})

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

func (r *resolver) eventContext(ctx context.Context, id string, limit int) (map[string]any, error) {
	root, events, err := r.store.ThreadEvents(ctx, id, limit)
	if err != nil {
		return nil, err
	}
	all := make([]chstore.EventView, 0, len(events)+1)
	all = append(all, *root)
	all = append(all, events...)
	relations := newPubkeyRelationCache(r.store, all)
	eventRelations := newEventRelationCacheWithPubkeyScoreMinFollowers(r.store, all, r.pubkeyScoreMinFollowers)
	return map[string]any{
		"root":   wrapEvent(*root, relations, eventRelations),
		"events": wrapEvents(events, relations, eventRelations),
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
}

type selectedReferenceInput struct {
	Selectors        []graphTagPredicate
	Fallback         *graphTagPredicate
	FallbackPosition string
	Limit            int
	MaxDepth         int
	ExcludeSelf      bool
}

type genericDimension struct {
	Name     string
	Field    string
	TagKey   string
	TagIndex int
	Derived  string
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

func (r *resolver) eventSelectedReferences(ctx context.Context, event chstore.EventView, raw any) (eventConnectionSource, error) {
	cache := newEventRelationCacheWithPubkeyScoreMinFollowers(r.store, []chstore.EventView{event}, r.pubkeyScoreMinFollowers)
	return cache.loadSelectedReferences(ctx, r, event, raw)
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

func (r *resolver) eventRankedReferencedBy(ctx context.Context, event chstore.EventView, raw any) (eventConnectionSource, error) {
	input, err := r.rankedReverseReferenceQuery(ctx, event, raw)
	if err != nil {
		return eventConnectionSource{}, err
	}
	candidates, err := r.queryEvents(ctx, input.Events)
	if err != nil {
		return eventConnectionSource{}, err
	}
	if len(candidates) == 0 {
		return eventConnectionSource{}, nil
	}

	candidateIDs := make([]string, 0, len(candidates))
	for _, candidate := range candidates {
		candidateIDs = append(candidateIDs, candidate.ID)
	}
	rankAggregate := input.RankReferences
	if !rankAggregate.Empty {
		rankAggregate.Tags = append(rankAggregate.Tags, chstore.TagFilter{
			Key: input.RankVia.Key, Value: input.RankVia.Value, Values: rankTagValues(input.RankVia, candidateIDs),
		})
		if rankAggregate.Limit == 0 || rankAggregate.Limit > 1000 {
			rankAggregate.Limit = uint64(len(candidateIDs))
		}
		if rankAggregate.Limit < uint64(len(candidateIDs)) {
			rankAggregate.Limit = uint64(len(candidateIDs))
		}
	}
	var targetIDs []string
	if useWeightedRanking(input.WeightedTerms, input.CandidateBoosts, input.Shuffle) {
		targetIDs, err = weightedRankCandidateIDs(ctx, r.store, candidates, input.WeightedTerms, input.CandidateBoosts, input.Shuffle, input.Offset, input.Limit)
	} else {
		rows, err := r.store.AggregateEvents(ctx, rankAggregate)
		if err != nil {
			return eventConnectionSource{}, err
		}
		targetIDs = rankedCandidateIDs(rows, candidates, input.Offset, input.Limit)
	}
	if len(targetIDs) == 0 {
		return eventConnectionSource{}, nil
	}
	targetQuery := chstore.EventQueryInput{IDs: targetIDs, Limit: uint64(len(targetIDs))}
	events, err := r.queryEvents(ctx, targetQuery)
	if err != nil {
		return eventConnectionSource{}, err
	}
	eventsByID := make(map[string]chstore.EventView, len(events))
	for _, event := range events {
		eventsByID[event.ID] = event
	}
	ordered := make([]chstore.EventView, 0, len(events))
	for _, id := range targetIDs {
		if event, ok := eventsByID[id]; ok {
			ordered = append(ordered, event)
		}
	}
	return r.newEventConnection(ordered), nil
}

func (r *resolver) eventAuthoredReplyChain(ctx context.Context, event chstore.EventView, raw any) (eventConnectionSource, error) {
	cache := newEventRelationCacheWithPubkeyScoreMinFollowers(r.store, []chstore.EventView{event}, r.pubkeyScoreMinFollowers)
	return cache.loadAuthoredReplyChain(ctx, r, event, raw)
}

func (r *resolver) eventAggregateReferencedBy(ctx context.Context, event chstore.EventView, raw any) (map[string]any, error) {
	input, dimensions, metrics, first, orderBy, err := r.aggregateReferencedByQuery(ctx, event, raw)
	if err != nil {
		return nil, err
	}
	events, err := r.queryEvents(ctx, input)
	if err != nil {
		return nil, err
	}
	return map[string]any{"rows": aggregateReferencedRows(events, dimensions, metrics, first, orderBy)}, nil
}

func aggregateReferencedRows(events []chstore.EventView, dimensions []genericDimension, metrics []genericMetric, first int, orderBy string) []chstore.AggregateRow {
	if len(metrics) == 0 {
		metrics = []genericMetric{{Name: "count", Op: "COUNT"}}
	}

	type group struct {
		dimensions map[string]string
		events     []chstore.EventView
	}
	groups := map[string]*group{}
	for _, event := range events {
		dimValues := map[string]string{}
		keyParts := make([]string, 0, len(dimensions))
		for _, dim := range dimensions {
			value := selectorString(event, selectorParts{
				field: dim.Field, tagKey: dim.TagKey, tagIndex: dim.TagIndex, derived: dim.Derived,
			})
			dimValues[dim.Name] = value
			keyParts = append(keyParts, dim.Name+"="+value)
		}
		key := strings.Join(keyParts, "\x00")
		if key == "" {
			key = "__all__"
		}
		if groups[key] == nil {
			groups[key] = &group{dimensions: dimValues}
		}
		groups[key].events = append(groups[key].events, event)
	}
	if len(groups) == 0 {
		groups["__all__"] = &group{dimensions: map[string]string{}}
	}

	rows := make([]chstore.AggregateRow, 0, len(groups))
	for _, group := range groups {
		row := chstore.AggregateRow{Dimensions: group.dimensions, Metrics: map[string]uint64{}}
		for _, metric := range metrics {
			row.Metrics[metric.Name] = computeMetric(group.events, metric)
		}
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		key := orderBy
		if key == "" && len(metrics) > 0 {
			key = metrics[0].Name
		}
		if key == "" {
			return false
		}
		return rows[i].Metrics[key] > rows[j].Metrics[key]
	})
	if first <= 0 || first > 100 {
		first = 100
	}
	if len(rows) > first {
		rows = rows[:first]
	}
	return rows
}

func (r *resolver) rankedEvents(ctx context.Context, raw any) (eventConnectionSource, error) {
	input, err := r.parseRankedEventsInput(ctx, raw)
	if err != nil {
		return eventConnectionSource{}, err
	}
	rows, err := r.rankedEventRows(ctx, input)
	if err != nil {
		return eventConnectionSource{}, err
	}
	targetIDs := rankedTargetIDs(rows, 0, max(input.Limit+input.Offset, input.Limit))
	if len(targetIDs) == 0 {
		return eventConnectionSource{}, nil
	}
	if len(input.Target.IDs) > 0 {
		targetIDs = intersectRankedIDs(targetIDs, input.Target.IDs)
	}
	if len(targetIDs) == 0 {
		return eventConnectionSource{}, nil
	}
	input.Target.IDs = targetIDs
	input.Target.Limit = uint64(len(targetIDs))

	events, err := r.queryEvents(ctx, input.Target)
	if err != nil {
		return eventConnectionSource{}, err
	}
	if useWeightedRanking(input.WeightedTerms, input.CandidateBoosts, input.Shuffle) {
		targetIDs, err = weightedRankCandidateIDs(ctx, r.store, events, input.WeightedTerms, input.CandidateBoosts, input.Shuffle, input.Offset, input.Limit)
		if err != nil {
			return eventConnectionSource{}, err
		}
	} else {
		targetIDs = rankedCandidateIDs(rows, events, input.Offset, input.Limit)
	}
	eventsByID := make(map[string]chstore.EventView, len(events))
	for _, event := range events {
		eventsByID[event.ID] = event
	}
	ordered := make([]chstore.EventView, 0, len(events))
	for _, id := range targetIDs {
		if event, ok := eventsByID[id]; ok {
			ordered = append(ordered, event)
		}
	}
	return r.newEventConnection(ordered), nil
}

func (r *resolver) rankedEventRows(ctx context.Context, input rankedEventsInput) ([]chstore.AggregateRow, error) {
	if rankedTargetHasFilters(input.Target) {
		return r.store.AggregateEventReferencesToTargets(ctx, input.References, input.Target)
	}
	return r.store.AggregateEvents(ctx, input.References)
}

func rankedTargetHasFilters(input chstore.EventQueryInput) bool {
	// Kind-only targets are post-filtered during event hydration so global
	// trending queries keep the cheaper aggregate path.
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
	Limit           int
	Offset          int
}

type rankedReverseReferenceInput struct {
	Events          chstore.EventQueryInput
	RankReferences  chstore.AggregateInput
	RankVia         graphTagPredicate
	WeightedTerms   []weightedRankTerm
	CandidateBoosts []candidatePubkeyBoost
	Shuffle         shuffleSpec
	Limit           int
	Offset          int
}

type rankedReverseReferenceBatchInput struct {
	Events          chstore.EventQueryInput
	Via             graphTagPredicate
	Target          string
	RankReferences  chstore.AggregateInput
	RankVia         graphTagPredicate
	WeightedTerms   []weightedRankTerm
	CandidateBoosts []candidatePubkeyBoost
	Shuffle         shuffleSpec
	Limit           int
	Offset          int
}

type authoredReplyChainInput struct {
	Events               chstore.EventQueryInput
	Via                  graphTagPredicate
	Target               string
	UseSourceEventAuthor bool
	MaxDepth             int
	MaxBranchFanout      int
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

type aggregateReferencedByBatchInput struct {
	Events     chstore.EventQueryInput
	Via        graphTagPredicate
	Target     string
	Dimensions []genericDimension
	Metrics    []genericMetric
	First      int
	OrderBy    string
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

func parseSelectedReferenceInput(raw any) selectedReferenceInput {
	input := selectedReferenceInput{
		Limit:            20,
		FallbackPosition: "FIRST",
		ExcludeSelf:      true,
	}
	if m, ok := raw.(map[string]any); ok {
		input.Selectors = graphTagPredicates(m["selectors"])
		if fallbackRaw, ok := m["fallback"].(map[string]any); ok {
			fallback := graphTagPredicateFrom(fallbackRaw)
			if fallback.Key != "" {
				input.Fallback = &fallback
			}
		}
		input.FallbackPosition = strings.ToUpper(stringValue(m["fallbackPosition"]))
		input.Limit = intValue(m["limit"], 20)
		input.MaxDepth = intValue(m["maxDepth"], 0)
		input.ExcludeSelf = boolValue(m["excludeSelf"], true)
	}
	if input.FallbackPosition != "LAST" {
		input.FallbackPosition = "FIRST"
	}
	if input.Limit <= 0 || input.Limit > 100 {
		input.Limit = 20
	}
	if input.MaxDepth < 0 {
		input.MaxDepth = 0
	}
	if input.MaxDepth > 8 {
		input.MaxDepth = 8
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
	input.Tags = append(input.Tags, chstore.TagFilter{Key: via.Key, Value: target})
	input.Limit = uint64(intValue(m["limit"], int(input.Limit)))
	if input.Limit == 0 || input.Limit > 500 {
		input.Limit = 50
	}
	input.Offset = uint64(intValue(m["offset"], int(input.Offset)))
	return input, nil
}

func (r *resolver) rankedReverseReferenceQuery(ctx context.Context, event chstore.EventView, raw any) (rankedReverseReferenceInput, error) {
	m, _ := raw.(map[string]any)
	var out rankedReverseReferenceInput
	if eventsRaw, ok := m["events"].(map[string]any); ok {
		events, err := r.parseEventQueryInputForSourceEvent(ctx, eventsRaw, event)
		if err != nil {
			return out, err
		}
		out.Events = events
	}
	via := graphTagPredicateFrom(m["via"])
	target := targetValue(event, stringValue(m["target"]))
	if via.Value != "" {
		target = via.Value
	}
	if target == "" {
		out.Events.Empty = true
		return out, nil
	}
	if via.Key == "" {
		via.Key = "e"
	}
	out.Events.Tags = append(out.Events.Tags, chstore.TagFilter{Key: via.Key, Value: target})
	if out.Events.Limit == 0 || out.Events.Limit > 500 {
		out.Events.Limit = 50
	}

	rankRaw, ok := m["rank"].(map[string]any)
	if !ok {
		return out, fmt.Errorf("rank input is required")
	}
	referencesRaw, ok := rankRaw["references"].(map[string]any)
	if !ok {
		return out, fmt.Errorf("rank.references input is required")
	}
	references, err := r.parseEventQueryInput(ctx, referencesRaw)
	if err != nil {
		return out, err
	}
	rankVia := graphTagPredicateFrom(rankRaw["via"])
	if rankVia.Key == "" {
		return out, fmt.Errorf("rank.via.key is required")
	}
	out.RankVia = rankVia
	out.RankReferences = chstore.AggregateInput{
		Dataset: "TAGS",
		GroupBy: []string{"TAG_VALUE"},
		Metrics: []string{rankMetricName(rankRaw["metric"])},
		IDs:     references.IDs,
		PubKeys: references.PubKeys,
		Kinds:   references.Kinds,
		Tags:    references.Tags,
		Since:   references.Since,
		Until:   references.Until,
		Limit:   references.Limit,
		Empty:   references.Empty,
	}
	out.WeightedTerms, err = weightedRankTerms(ctx, rankRaw, r.parseEventQueryInput, r.pubkeyScoreMinFollowers)
	if err != nil {
		return out, err
	}
	out.CandidateBoosts, err = r.candidatePubkeyBoosts(ctx, rankRaw["candidatePubkeyBoosts"])
	if err != nil {
		return out, err
	}
	out.Shuffle = shuffleInput(rankRaw["shuffle"])
	out.Limit = intValue(m["limit"], 1)
	if out.Limit <= 0 {
		out.Limit = 1
	} else if out.Limit > 50 {
		out.Limit = 50
	}
	out.Offset = intValue(m["offset"], 0)
	if out.Offset < 0 {
		out.Offset = 0
	}
	return out, nil
}

func (c *eventRelationCache) rankedReverseReferenceBatchQuery(ctx context.Context, raw any) (rankedReverseReferenceBatchInput, error) {
	m, _ := raw.(map[string]any)
	var out rankedReverseReferenceBatchInput
	if eventsRaw, ok := m["events"].(map[string]any); ok {
		events, err := c.parseEventQueryInput(ctx, eventsRaw)
		if err != nil {
			return out, err
		}
		out.Events = events
	}
	via := graphTagPredicateFrom(m["via"])
	if via.Key == "" {
		via.Key = "e"
	}
	out.Via = via
	out.Target = stringValue(m["target"])
	if out.Events.Limit == 0 || out.Events.Limit > 500 {
		out.Events.Limit = 50
	}

	rankRaw, ok := m["rank"].(map[string]any)
	if !ok {
		return out, fmt.Errorf("rank input is required")
	}
	referencesRaw, ok := rankRaw["references"].(map[string]any)
	if !ok {
		return out, fmt.Errorf("rank.references input is required")
	}
	references, err := c.parseEventQueryInput(ctx, referencesRaw)
	if err != nil {
		return out, err
	}
	rankVia := graphTagPredicateFrom(rankRaw["via"])
	if rankVia.Key == "" {
		return out, fmt.Errorf("rank.via.key is required")
	}
	out.RankVia = rankVia
	out.RankReferences = chstore.AggregateInput{
		Dataset: "TAGS",
		GroupBy: []string{"TAG_VALUE"},
		Metrics: []string{rankMetricName(rankRaw["metric"])},
		IDs:     references.IDs,
		PubKeys: references.PubKeys,
		Kinds:   references.Kinds,
		Tags:    references.Tags,
		Since:   references.Since,
		Until:   references.Until,
		Limit:   references.Limit,
		Empty:   references.Empty,
	}
	out.WeightedTerms, err = weightedRankTerms(ctx, rankRaw, c.parseEventQueryInput, c.pubkeyScoreMinFollowers)
	if err != nil {
		return out, err
	}
	out.CandidateBoosts, err = c.candidatePubkeyBoosts(ctx, rankRaw["candidatePubkeyBoosts"])
	if err != nil {
		return out, err
	}
	out.Shuffle = shuffleInput(rankRaw["shuffle"])
	out.Limit = intValue(m["limit"], 1)
	if out.Limit <= 0 {
		out.Limit = 1
	} else if out.Limit > 50 {
		out.Limit = 50
	}
	out.Offset = intValue(m["offset"], 0)
	if out.Offset < 0 {
		out.Offset = 0
	}
	return out, nil
}

func (r *resolver) authoredReplyChainQuery(ctx context.Context, raw any) (authoredReplyChainInput, error) {
	m, _ := raw.(map[string]any)
	eventsRaw := map[string]any{}
	if rawEvents, ok := m["events"].(map[string]any); ok {
		for key, value := range rawEvents {
			eventsRaw[key] = value
		}
	}
	if kinds := intList(m["kinds"]); len(kinds) > 0 {
		eventsRaw["kinds"] = anyIntList(kinds)
	}
	if _, ok := eventsRaw["kinds"]; !ok {
		eventsRaw["kinds"] = []any{1, 1111}
	}

	sources := pubkeySources(eventsRaw["pubkeysFrom"])
	if pubkeyFromRaw, ok := m["pubkeyFrom"].(map[string]any); ok {
		sources = append(sources, pubkeySources([]any{pubkeyFromRaw})...)
	}
	useSourceAuthor := len(sources) == 0
	for _, source := range sources {
		if source.sourceEventAuthor {
			useSourceAuthor = true
		}
	}

	events, err := parseEventQueryInput(eventsRaw)
	if err != nil {
		return authoredReplyChainInput{}, err
	}
	var derivedPubkeys []string
	for _, source := range sources {
		if source.latestEventTags == nil {
			continue
		}
		values, err := r.resolveLatestEventTagPubkeys(ctx, *source.latestEventTags)
		if err != nil {
			return authoredReplyChainInput{}, err
		}
		derivedPubkeys = append(derivedPubkeys, values...)
	}
	events.PubKeys = uniqueStrings(append(events.PubKeys, derivedPubkeys...))

	via := graphTagPredicateFrom(m["via"])
	if via.Key == "" {
		via.Key = "e"
	}
	maxDepth := intValue(m["maxDepth"], 8)
	if maxDepth <= 0 {
		maxDepth = 8
	} else if maxDepth > 8 {
		maxDepth = 8
	}
	maxBranchFanout := intValue(m["maxBranchFanout"], 32)
	if maxBranchFanout <= 0 {
		maxBranchFanout = 32
	} else if maxBranchFanout > 100 {
		maxBranchFanout = 100
	}
	events.Limit = uint64(maxBranchFanout)

	target := strings.ToUpper(stringValue(m["target"]))
	if target == "" {
		target = "EVENT_ID"
	}

	return authoredReplyChainInput{
		Events:               events,
		Via:                  via,
		Target:               target,
		UseSourceEventAuthor: useSourceAuthor,
		MaxDepth:             maxDepth,
		MaxBranchFanout:      maxBranchFanout,
	}, nil
}

func (c *eventRelationCache) aggregateReferencedByBatchQuery(ctx context.Context, raw any) (aggregateReferencedByBatchInput, error) {
	m, _ := raw.(map[string]any)
	var out aggregateReferencedByBatchInput
	if eventsRaw, ok := m["events"].(map[string]any); ok {
		events, err := c.parseEventQueryInput(ctx, eventsRaw)
		if err != nil {
			return out, err
		}
		out.Events = events
	}
	via := graphTagPredicateFrom(m["via"])
	if via.Key == "" {
		via.Key = "e"
	}
	out.Via = via
	out.Target = stringValue(m["target"])
	out.Events.Limit = uint64(intValue(m["limit"], int(out.Events.Limit)))
	if out.Events.Limit == 0 || out.Events.Limit > 500 {
		out.Events.Limit = 50
	}
	out.Dimensions = genericDimensions(m["groupBy"])
	out.Metrics = genericMetrics(m["metrics"])
	out.First = intValue(m["first"], 100)
	out.OrderBy = stringValue(m["orderBy"])
	return out, nil
}

func (r *resolver) aggregateReferencedByQuery(ctx context.Context, event chstore.EventView, raw any) (chstore.EventQueryInput, []genericDimension, []genericMetric, int, string, error) {
	m, _ := raw.(map[string]any)
	input, err := r.reverseReferenceQuery(ctx, event, map[string]any{
		"events": m["events"],
		"via":    m["via"],
		"target": m["target"],
		"limit":  m["limit"],
	})
	if err != nil {
		return input, nil, nil, 0, "", err
	}
	if input.Limit == 0 || input.Limit > 1000 {
		input.Limit = 500
	}
	return input, genericDimensions(m["groupBy"]), genericMetrics(m["metrics"]), intValue(m["first"], 100), stringValue(m["orderBy"]), nil
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
			Since: references.Since,
			Until: references.Until,
			Limit: uint64(aggregateLimit),
			Empty: references.Empty,
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
		source = "vertex"
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
	scores := make(map[string]float64, len(candidates))
	candidateIDs := make([]string, 0, len(candidates))
	seen := map[string]struct{}{}
	for _, candidate := range candidates {
		if candidate.ID == "" {
			continue
		}
		if _, ok := seen[candidate.ID]; ok {
			continue
		}
		seen[candidate.ID] = struct{}{}
		candidateIDs = append(candidateIDs, candidate.ID)
		scores[candidate.ID] = 0
		for _, boost := range boosts {
			if _, ok := boost.PubKeys[candidate.PubKey]; ok {
				scores[candidate.ID] += boost.Weight
			}
		}
	}
	if len(candidateIDs) == 0 {
		return nil, nil
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

	ranked := make([]candidateRank, 0, len(candidates))
	added := map[string]struct{}{}
	for _, candidate := range candidates {
		if _, ok := scores[candidate.ID]; !ok {
			continue
		}
		if _, ok := added[candidate.ID]; ok {
			continue
		}
		added[candidate.ID] = struct{}{}
		ranked = append(ranked, candidateRank{event: candidate, score: scores[candidate.ID]})
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
		return nil, nil
	}
	ranked = ranked[offset:]
	if len(ranked) > limit {
		ranked = ranked[:limit]
	}
	out := make([]string, 0, len(ranked))
	for _, entry := range ranked {
		out = append(out, entry.event.ID)
	}
	return out, nil
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

func rankTagValues(predicate graphTagPredicate, candidateIDs []string) []string {
	if predicate.Value != "" || len(predicate.Values) > 0 {
		return predicate.Values
	}
	return candidateIDs
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
	}
}

func genericDimensions(v any) []genericDimension {
	values := anyList(v)
	out := make([]genericDimension, 0, len(values))
	for _, value := range values {
		raw, ok := value.(map[string]any)
		if !ok {
			continue
		}
		name := stringValue(raw["name"])
		if name == "" {
			continue
		}
		out = append(out, genericDimension{
			Name:     name,
			Field:    stringValue(raw["field"]),
			TagKey:   stringValue(raw["tagKey"]),
			TagIndex: intValue(raw["tagIndex"], 1),
			Derived:  stringValue(raw["derived"]),
		})
	}
	return out
}

func genericMetrics(v any) []genericMetric {
	values := anyList(v)
	out := make([]genericMetric, 0, len(values))
	for _, value := range values {
		raw, ok := value.(map[string]any)
		if !ok {
			continue
		}
		name := stringValue(raw["name"])
		if name == "" {
			continue
		}
		out = append(out, genericMetric{
			Name:          name,
			Op:            strings.ToUpper(stringValue(raw["op"])),
			Field:         stringValue(raw["field"]),
			TagKey:        stringValue(raw["tagKey"]),
			TagIndex:      intValue(raw["tagIndex"], 1),
			Derived:       stringValue(raw["derived"]),
			DistinctField: stringValue(raw["distinctField"]),
		})
	}
	return out
}

func referenceAggregateDimensions(values []genericDimension) []chstore.ReferenceAggregateDimension {
	out := make([]chstore.ReferenceAggregateDimension, 0, len(values))
	for _, value := range values {
		out = append(out, chstore.ReferenceAggregateDimension{
			Name:     value.Name,
			Field:    value.Field,
			TagKey:   value.TagKey,
			TagIndex: value.TagIndex,
			Derived:  value.Derived,
		})
	}
	return out
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
	return r.store.QueryEvents(ctx, input)
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

func Handler(schema graphql.Schema) http.HandlerFunc {
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
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
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
		IDs:     stringList(raw["ids"]),
		PubKeys: stringList(raw["pubkeys"]),
		Kinds:   intList(raw["kinds"]),
		Tags:    tagFilters(raw["tags"]),
		Since:   int64(intValue(raw["since"], 0)),
		Until:   int64(intValue(raw["until"], 0)),
		Limit:   uint64(intValue(raw["limit"], 50)),
		Offset:  uint64(intValue(raw["offset"], 0)),
	}
	return input, validateHexFilters(input.IDs, input.PubKeys)
}

func parseTrendingInput(raw map[string]any) chstore.TrendingInput {
	input := chstore.TrendingInput{
		Window: "H24",
		Limit:  20,
	}
	if raw == nil {
		return input
	}
	if window := strings.ToUpper(strings.TrimSpace(stringValue(raw["window"]))); window == "H8" || window == "H24" || window == "D7" {
		input.Window = window
	}
	input.Category = strings.TrimSpace(stringValue(raw["category"]))
	if limit := intValue(raw["limit"], 20); limit > 0 {
		input.Limit = uint64(limit)
	}
	return input
}

func parseNotificationInput(raw map[string]any) (chstore.NotificationInput, error) {
	input := chstore.NotificationInput{
		Tab:    "ALL",
		Policy: "STRICT",
		Limit:  50,
	}
	if raw == nil {
		return input, fmt.Errorf("notification viewer is required")
	}
	input.Viewer = strings.ToLower(strings.TrimSpace(stringValue(raw["viewer"])))
	if err := validateHex64(input.Viewer); err != nil {
		return input, fmt.Errorf("notification viewer: %w", err)
	}
	if tab := strings.ToUpper(strings.TrimSpace(stringValue(raw["tab"]))); tab == "ALL" || tab == "MENTIONS" {
		input.Tab = tab
	}
	if policy := strings.ToUpper(strings.TrimSpace(stringValue(raw["policy"]))); policy == "RELAXED" || policy == "MODERATE" || policy == "STRICT" {
		input.Policy = policy
	}
	input.Since = int64(intValue(raw["since"], 0))
	input.Until = int64(intValue(raw["until"], 0))
	if limit := intValue(raw["limit"], 50); limit > 0 {
		input.Limit = uint64(limit)
	}
	return input, nil
}

func parseAggregateInput(raw map[string]any) (chstore.AggregateInput, error) {
	input := chstore.AggregateInput{
		Dataset: fmt.Sprint(raw["dataset"]),
		GroupBy: stringList(raw["groupBy"]),
		Metrics: stringList(raw["metrics"]),
		IDs:     stringList(raw["ids"]),
		PubKeys: stringList(raw["pubkeys"]),
		Kinds:   intList(raw["kinds"]),
		Tags:    tagFilters(raw["tags"]),
		Since:   int64(intValue(raw["since"], 0)),
		Until:   int64(intValue(raw["until"], 0)),
		Limit:   uint64(intValue(raw["limit"], 100)),
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

func anyIntList(values []int) []any {
	out := make([]any, 0, len(values))
	for _, value := range values {
		out = append(out, value)
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

func trendingClusterField(fn func(chstore.TrendingClusterRow) any) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		switch cluster := p.Source.(type) {
		case trendingClusterNode:
			return fn(cluster.row), nil
		case *trendingClusterNode:
			if cluster != nil {
				return fn(cluster.row), nil
			}
		case chstore.TrendingClusterRow:
			return fn(cluster), nil
		case *chstore.TrendingClusterRow:
			if cluster != nil {
				return fn(*cluster), nil
			}
		}
		return nil, nil
	}
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

func newNotificationConnection(store Store, rows []chstore.NotificationRow, pubkeyScoreMinFollowers uint64) notificationConnectionSource {
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

func newEventConnectionCaches(store Store, grouped map[string][]chstore.EventView) eventConnectionCaches {
	return newEventConnectionCachesWithPubkeyScoreMinFollowers(store, grouped, defaultPubkeyScoreMinFollowers)
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

func newEventRelationCache(store Store, events []chstore.EventView) *eventRelationCache {
	return newEventRelationCacheWithPubkeyScoreMinFollowers(store, events, defaultPubkeyScoreMinFollowers)
}

func newEventRelationCacheWithPubkeyScoreMinFollowers(store Store, events []chstore.EventView, pubkeyScoreMinFollowers uint64) *eventRelationCache {
	return &eventRelationCache{
		store:                   store,
		events:                  append([]chstore.EventView(nil), events...),
		pubkeyScoreMinFollowers: pubkeyScoreMinFollowers,
		latestEventTags:         map[string][]string{},
		aggregateByTarget:       map[string]map[string][]chstore.AggregateRow{},
		selectedReferences:      map[string]map[string][]chstore.EventView{},
		rankedReferencedBy:      map[string]map[string][]chstore.EventView{},
		authoredReplyChains:     map[string]map[string][]chstore.EventView{},
		selectedConnections:     map[string]eventConnectionCaches{},
		rankedConnections:       map[string]eventConnectionCaches{},
		authoredConnections:     map[string]eventConnectionCaches{},
	}
}

func (c *eventRelationCache) loadAggregateReferencedBy(ctx context.Context, r *resolver, event chstore.EventView, raw any) (map[string]any, error) {
	if c == nil {
		return r.eventAggregateReferencedBy(ctx, event, raw)
	}
	key := "aggregateReferencedBy:" + graphInputSignature(raw)

	c.mu.Lock()
	cached, ok := c.aggregateByTarget[key]
	c.mu.Unlock()
	if !ok {
		value, err, _ := c.group.Do(key, func() (any, error) {
			c.mu.Lock()
			if existing, exists := c.aggregateByTarget[key]; exists {
				c.mu.Unlock()
				return existing, nil
			}
			c.mu.Unlock()

			started := time.Now()
			loaded, err := c.loadAggregateReferencedByBatch(ctx, raw)
			if err != nil {
				return nil, err
			}

			c.mu.Lock()
			c.aggregateByTarget[key] = loaded
			c.mu.Unlock()

			slog.Debug(
				"graphql batched aggregate referenced-by loaded",
				"parents", len(c.events),
				"results", aggregateRowMapLen(loaded),
				"duration_ms", time.Since(started).Milliseconds(),
			)
			return loaded, nil
		})
		if err != nil {
			return nil, err
		}
		cached, _ = value.(map[string][]chstore.AggregateRow)
	}
	return map[string]any{"rows": cached[event.ID]}, nil
}

func (c *eventRelationCache) loadAggregateReferencedByBatch(ctx context.Context, raw any) (map[string][]chstore.AggregateRow, error) {
	out := make(map[string][]chstore.AggregateRow, len(c.events))
	input, err := c.aggregateReferencedByBatchQuery(ctx, raw)
	if err != nil {
		return nil, err
	}

	targetToParentIDs := make(map[string][]string, len(c.events))
	targets := make([]string, 0, len(c.events))
	seenTargets := map[string]struct{}{}
	for _, event := range c.events {
		target := targetValue(event, input.Target)
		if input.Via.Value != "" {
			target = input.Via.Value
		}
		if target == "" {
			out[event.ID] = aggregateReferencedRows(nil, input.Dimensions, input.Metrics, input.First, input.OrderBy)
			continue
		}
		targetToParentIDs[target] = append(targetToParentIDs[target], event.ID)
		if _, ok := seenTargets[target]; ok {
			continue
		}
		seenTargets[target] = struct{}{}
		targets = append(targets, target)
	}
	if len(targets) > 0 {
		sort.Strings(targets)
		if aggregateStore, ok := c.store.(referenceAggregateStore); ok {
			first := uint64(0)
			if input.First > 0 {
				first = uint64(input.First)
			}
			rowsByTarget, supported, err := aggregateStore.AggregateEventsByTagTargets(ctx, chstore.ReferenceAggregateInput{
				Events:         input.Events,
				Tag:            chstore.TagFilter{Key: input.Via.Key},
				Targets:        targets,
				LimitPerTarget: input.Events.Limit,
				GroupBy:        referenceAggregateDimensions(input.Dimensions),
				Metrics:        referenceAggregateMetrics(input.Metrics),
				First:          first,
				OrderBy:        input.OrderBy,
			})
			if err != nil {
				return nil, err
			}
			if supported {
				for _, target := range targets {
					rows := rowsByTarget[target]
					if len(rows) == 0 {
						rows = aggregateReferencedRows(nil, input.Dimensions, input.Metrics, input.First, input.OrderBy)
					}
					for _, parentID := range targetToParentIDs[target] {
						out[parentID] = rows
					}
				}
				return out, nil
			}
		}

		eventsByTarget, err := c.store.QueryEventsByTagTargets(ctx, input.Events, chstore.TagFilter{Key: input.Via.Key}, targets, input.Events.Limit)
		if err != nil {
			return nil, err
		}
		for _, target := range targets {
			rows := aggregateReferencedRows(eventsByTarget[target], input.Dimensions, input.Metrics, input.First, input.OrderBy)
			for _, parentID := range targetToParentIDs[target] {
				out[parentID] = rows
			}
		}
	}
	for _, event := range c.events {
		if _, ok := out[event.ID]; ok {
			continue
		}
		out[event.ID] = aggregateReferencedRows(nil, input.Dimensions, input.Metrics, input.First, input.OrderBy)
	}
	return out, nil
}

func (c *eventRelationCache) loadSelectedReferences(ctx context.Context, _ *resolver, event chstore.EventView, raw any) (eventConnectionSource, error) {
	if c == nil {
		return eventConnectionSource{}, nil
	}
	key := "selectedReferences:" + graphInputSignature(raw)

	c.mu.Lock()
	cached, ok := c.selectedReferences[key]
	connectionCaches := c.selectedConnections[key]
	c.mu.Unlock()
	if !ok {
		value, err, _ := c.group.Do(key, func() (any, error) {
			c.mu.Lock()
			if existing, exists := c.selectedReferences[key]; exists {
				connectionCaches = c.selectedConnections[key]
				c.mu.Unlock()
				return existing, nil
			}
			c.mu.Unlock()

			started := time.Now()
			loaded, err := c.loadSelectedReferencesBatch(ctx, raw)
			if err != nil {
				return nil, err
			}

			c.mu.Lock()
			c.selectedReferences[key] = loaded
			c.selectedConnections[key] = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, loaded, c.pubkeyScoreMinFollowers)
			c.mu.Unlock()

			slog.Debug(
				"graphql batched selected references loaded",
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
		connectionCaches = c.selectedConnections[key]
		c.mu.Unlock()
	}
	if connectionCaches.relations == nil || connectionCaches.eventRelations == nil {
		connectionCaches = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, cached, c.pubkeyScoreMinFollowers)
		c.mu.Lock()
		c.selectedConnections[key] = connectionCaches
		c.mu.Unlock()
	}
	return newEventConnectionWithCaches(cached[event.ID], connectionCaches.relations, connectionCaches.eventRelations), nil
}

func (c *eventRelationCache) loadSelectedReferencesBatch(ctx context.Context, raw any) (map[string][]chstore.EventView, error) {
	out := make(map[string][]chstore.EventView, len(c.events))
	for _, event := range c.events {
		out[event.ID] = nil
	}

	input := parseSelectedReferenceInput(raw)
	selectedBySource := make(map[string][]string, len(c.events))
	visitedBySource := make(map[string]map[string]struct{}, len(c.events))
	for _, event := range c.events {
		ids := selectReferenceIDs(event, input)
		selectedBySource[event.ID] = ids
		visited := map[string]struct{}{event.ID: {}}
		for _, id := range ids {
			visited[id] = struct{}{}
		}
		visitedBySource[event.ID] = visited
	}

	fetched := map[string]chstore.EventView{}
	for depth := 0; ; depth++ {
		fetchIDs := selectedReferenceFetchIDs(selectedBySource, fetched)
		if len(fetchIDs) > 0 {
			eventsByID, err := c.queryEventsByIDs(ctx, fetchIDs)
			if err != nil {
				return nil, err
			}
			for id, event := range eventsByID {
				fetched[id] = event
			}
		}

		if depth >= input.MaxDepth {
			break
		}

		changed := false
		for sourceID, ids := range selectedBySource {
			visited := visitedBySource[sourceID]
			nextIDs := make([]string, 0, len(ids))
			seenNext := map[string]struct{}{}
			for _, id := range ids {
				event, ok := fetched[id]
				if !ok {
					nextIDs = appendUniqueReferenceID(nextIDs, seenNext, id, sourceID, false)
					continue
				}

				candidates := selectReferenceIDs(event, input)
				advanced := false
				for _, candidateID := range candidates {
					if _, seen := visited[candidateID]; seen {
						continue
					}
					visited[candidateID] = struct{}{}
					nextIDs = appendUniqueReferenceID(nextIDs, seenNext, candidateID, sourceID, false)
					advanced = true
				}
				if !advanced {
					nextIDs = appendUniqueReferenceID(nextIDs, seenNext, id, sourceID, false)
				}
				if len(nextIDs) >= input.Limit {
					break
				}
			}
			if !sameStrings(ids, nextIDs) {
				changed = true
				selectedBySource[sourceID] = nextIDs
			}
		}
		if !changed {
			break
		}
	}

	for sourceID, ids := range selectedBySource {
		ordered := make([]chstore.EventView, 0, len(ids))
		seen := map[string]struct{}{}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			event, ok := fetched[id]
			if !ok {
				continue
			}
			seen[id] = struct{}{}
			ordered = append(ordered, event)
		}
		out[sourceID] = ordered
	}
	return out, nil
}

func (c *eventRelationCache) queryEventsByIDs(ctx context.Context, ids []string) (map[string]chstore.EventView, error) {
	ids = uniqueStrings(ids)
	out := make(map[string]chstore.EventView, len(ids))
	for start := 0; start < len(ids); start += 500 {
		end := start + 500
		if end > len(ids) {
			end = len(ids)
		}
		batch := ids[start:end]
		events, err := c.store.QueryEvents(ctx, chstore.EventQueryInput{IDs: batch, Limit: uint64(len(batch))})
		if err != nil {
			return nil, err
		}
		for _, event := range events {
			out[event.ID] = event
		}
	}
	return out, nil
}

func selectedReferenceFetchIDs(selectedBySource map[string][]string, fetched map[string]chstore.EventView) []string {
	ids := make([]string, 0)
	seen := map[string]struct{}{}
	for _, selected := range selectedBySource {
		for _, id := range selected {
			if _, ok := fetched[id]; ok {
				continue
			}
			ids = appendUniqueReferenceID(ids, seen, id, "", false)
		}
	}
	return ids
}

func selectReferenceIDs(event chstore.EventView, input selectedReferenceInput) []string {
	for _, selector := range input.Selectors {
		ids := matchingReferenceIDs(event, selector, input.Limit, input.ExcludeSelf, "FIRST")
		if len(ids) > 0 {
			return ids
		}
	}
	if input.Fallback == nil {
		return nil
	}
	return matchingReferenceIDs(event, *input.Fallback, input.Limit, input.ExcludeSelf, input.FallbackPosition)
}

func matchingReferenceIDs(event chstore.EventView, predicate graphTagPredicate, limit int, excludeSelf bool, position string) []string {
	if limit <= 0 {
		return nil
	}
	out := make([]string, 0, limit)
	seen := map[string]struct{}{}
	if strings.ToUpper(position) == "LAST" {
		for i := len(event.Tags) - 1; i >= 0; i-- {
			out = appendReferenceTagID(out, seen, event, event.Tags[i], predicate, limit, excludeSelf)
			if len(out) >= limit {
				break
			}
		}
		return out
	}
	for _, tag := range event.Tags {
		out = appendReferenceTagID(out, seen, event, tag, predicate, limit, excludeSelf)
		if len(out) >= limit {
			break
		}
	}
	return out
}

func appendReferenceTagID(out []string, seen map[string]struct{}, event chstore.EventView, tag []string, predicate graphTagPredicate, limit int, excludeSelf bool) []string {
	if len(out) >= limit || !sourceTagMatches(tag, predicate) || len(tag) < 2 || !hex64Pattern.MatchString(tag[1]) {
		return out
	}
	return appendUniqueReferenceID(out, seen, tag[1], event.ID, excludeSelf)
}

func appendUniqueReferenceID(out []string, seen map[string]struct{}, id string, sourceID string, excludeSelf bool) []string {
	if id == "" || (excludeSelf && id == sourceID) {
		return out
	}
	if _, ok := seen[id]; ok {
		return out
	}
	seen[id] = struct{}{}
	return append(out, id)
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (c *eventRelationCache) loadRankedReferencedBy(ctx context.Context, r *resolver, event chstore.EventView, raw any) (eventConnectionSource, error) {
	if c == nil {
		return r.eventRankedReferencedBy(ctx, event, raw)
	}
	key := "rankedReferencedBy:" + graphInputSignature(raw)

	c.mu.Lock()
	cached, ok := c.rankedReferencedBy[key]
	connectionCaches := c.rankedConnections[key]
	c.mu.Unlock()
	if !ok {
		value, err, _ := c.group.Do(key, func() (any, error) {
			c.mu.Lock()
			if existing, exists := c.rankedReferencedBy[key]; exists {
				connectionCaches = c.rankedConnections[key]
				c.mu.Unlock()
				return existing, nil
			}
			c.mu.Unlock()

			started := time.Now()
			loaded, err := c.loadRankedReferencedByBatch(ctx, raw)
			if err != nil {
				return nil, err
			}

			c.mu.Lock()
			c.rankedReferencedBy[key] = loaded
			c.rankedConnections[key] = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, loaded, c.pubkeyScoreMinFollowers)
			c.mu.Unlock()

			slog.Debug(
				"graphql batched ranked referenced-by loaded",
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
		connectionCaches = c.rankedConnections[key]
		c.mu.Unlock()
	}
	if connectionCaches.relations == nil || connectionCaches.eventRelations == nil {
		connectionCaches = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, cached, c.pubkeyScoreMinFollowers)
		c.mu.Lock()
		c.rankedConnections[key] = connectionCaches
		c.mu.Unlock()
	}
	return newEventConnectionWithCaches(cached[event.ID], connectionCaches.relations, connectionCaches.eventRelations), nil
}

func (c *eventRelationCache) loadRankedReferencedByBatch(ctx context.Context, raw any) (map[string][]chstore.EventView, error) {
	out := make(map[string][]chstore.EventView, len(c.events))
	for _, event := range c.events {
		out[event.ID] = nil
	}

	input, err := c.rankedReverseReferenceBatchQuery(ctx, raw)
	if err != nil {
		return nil, err
	}
	if input.Events.Empty {
		return out, nil
	}

	targetToParentIDs := make(map[string][]string, len(c.events))
	targets := make([]string, 0, len(c.events))
	seenTargets := map[string]struct{}{}
	for _, event := range c.events {
		target := targetValue(event, input.Target)
		if input.Via.Value != "" {
			target = input.Via.Value
		}
		if target == "" {
			continue
		}
		targetToParentIDs[target] = append(targetToParentIDs[target], event.ID)
		if _, ok := seenTargets[target]; ok {
			continue
		}
		seenTargets[target] = struct{}{}
		targets = append(targets, target)
	}
	if len(targets) == 0 {
		return out, nil
	}
	sort.Strings(targets)

	candidatesByTarget, err := c.store.QueryEventsByTagTargets(ctx, input.Events, chstore.TagFilter{Key: input.Via.Key}, targets, input.Events.Limit)
	if err != nil {
		return nil, err
	}

	candidateIDs := make([]string, 0)
	seenCandidateIDs := map[string]struct{}{}
	for _, target := range targets {
		for _, candidate := range candidatesByTarget[target] {
			if candidate.ID == "" {
				continue
			}
			if _, ok := seenCandidateIDs[candidate.ID]; ok {
				continue
			}
			seenCandidateIDs[candidate.ID] = struct{}{}
			candidateIDs = append(candidateIDs, candidate.ID)
		}
	}
	if len(candidateIDs) == 0 {
		return out, nil
	}
	sort.Strings(candidateIDs)

	var rows []chstore.AggregateRow
	if !useWeightedRanking(input.WeightedTerms, input.CandidateBoosts, input.Shuffle) && !input.RankReferences.Empty {
		rankAggregate := input.RankReferences
		rankAggregate.Tags = append(rankAggregate.Tags, chstore.TagFilter{
			Key: input.RankVia.Key, Value: input.RankVia.Value, Values: rankTagValues(input.RankVia, candidateIDs),
		})
		if rankAggregate.Limit == 0 || rankAggregate.Limit < uint64(len(candidateIDs)) || rankAggregate.Limit > 1000 {
			rankAggregate.Limit = uint64(len(candidateIDs))
		}
		rows, err = c.store.AggregateEvents(ctx, rankAggregate)
		if err != nil {
			return nil, err
		}
	}

	selectedByParentID := make(map[string][]string, len(c.events))
	selectedIDs := make([]string, 0)
	seenSelectedIDs := map[string]struct{}{}
	for _, target := range targets {
		var targetIDs []string
		if useWeightedRanking(input.WeightedTerms, input.CandidateBoosts, input.Shuffle) {
			targetIDs, err = weightedRankCandidateIDs(ctx, c.store, candidatesByTarget[target], input.WeightedTerms, input.CandidateBoosts, input.Shuffle, input.Offset, input.Limit)
			if err != nil {
				return nil, err
			}
		} else {
			targetIDs = rankedCandidateIDs(rows, candidatesByTarget[target], input.Offset, input.Limit)
		}
		if len(targetIDs) == 0 {
			continue
		}
		for _, parentID := range targetToParentIDs[target] {
			selectedByParentID[parentID] = append(selectedByParentID[parentID], targetIDs...)
		}
		for _, id := range targetIDs {
			if _, ok := seenSelectedIDs[id]; ok {
				continue
			}
			seenSelectedIDs[id] = struct{}{}
			selectedIDs = append(selectedIDs, id)
		}
	}
	if len(selectedIDs) == 0 {
		return out, nil
	}

	targetEvents, err := c.store.QueryEvents(ctx, chstore.EventQueryInput{IDs: selectedIDs, Limit: uint64(len(selectedIDs))})
	if err != nil {
		return nil, err
	}
	eventsByID := make(map[string]chstore.EventView, len(targetEvents))
	for _, event := range targetEvents {
		eventsByID[event.ID] = event
	}
	for parentID, ids := range selectedByParentID {
		ordered := make([]chstore.EventView, 0, len(ids))
		seen := map[string]struct{}{}
		for _, id := range ids {
			if _, ok := seen[id]; ok {
				continue
			}
			event, ok := eventsByID[id]
			if !ok {
				continue
			}
			seen[id] = struct{}{}
			ordered = append(ordered, event)
		}
		out[parentID] = ordered
	}
	return out, nil
}

func (c *eventRelationCache) loadAuthoredReplyChain(ctx context.Context, r *resolver, event chstore.EventView, raw any) (eventConnectionSource, error) {
	if c == nil {
		return r.eventAuthoredReplyChain(ctx, event, raw)
	}
	key := "authoredReplyChain:" + graphInputSignature(raw)

	c.mu.Lock()
	cached, ok := c.authoredReplyChains[key]
	connectionCaches := c.authoredConnections[key]
	c.mu.Unlock()
	if !ok {
		value, err, _ := c.group.Do(key, func() (any, error) {
			c.mu.Lock()
			if existing, exists := c.authoredReplyChains[key]; exists {
				connectionCaches = c.authoredConnections[key]
				c.mu.Unlock()
				return existing, nil
			}
			c.mu.Unlock()

			started := time.Now()
			loaded, err := c.loadAuthoredReplyChainsBatch(ctx, r, raw)
			if err != nil {
				return nil, err
			}

			c.mu.Lock()
			c.authoredReplyChains[key] = loaded
			c.authoredConnections[key] = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, loaded, c.pubkeyScoreMinFollowers)
			c.mu.Unlock()

			slog.Debug(
				"graphql batched authored reply chains loaded",
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
		connectionCaches = c.authoredConnections[key]
		c.mu.Unlock()
	}
	if connectionCaches.relations == nil || connectionCaches.eventRelations == nil {
		connectionCaches = newEventConnectionCachesWithPubkeyScoreMinFollowers(c.store, cached, c.pubkeyScoreMinFollowers)
		c.mu.Lock()
		c.authoredConnections[key] = connectionCaches
		c.mu.Unlock()
	}
	return newEventConnectionWithCaches(cached[event.ID], connectionCaches.relations, connectionCaches.eventRelations), nil
}

type authoredReplyChainState struct {
	source         chstore.EventView
	current        chstore.EventView
	allowedPubKeys map[string]struct{}
	visited        map[string]struct{}
	done           bool
}

func (c *eventRelationCache) loadAuthoredReplyChainsBatch(ctx context.Context, r *resolver, raw any) (map[string][]chstore.EventView, error) {
	out := make(map[string][]chstore.EventView, len(c.events))
	for _, event := range c.events {
		out[event.ID] = nil
	}

	input, err := r.authoredReplyChainQuery(ctx, raw)
	if err != nil {
		return nil, err
	}
	if input.Events.Empty {
		return out, nil
	}

	states := make(map[string]*authoredReplyChainState, len(c.events))
	for _, event := range c.events {
		allowed := make(map[string]struct{}, len(input.Events.PubKeys)+1)
		for _, pubkey := range input.Events.PubKeys {
			allowed[pubkey] = struct{}{}
		}
		if input.UseSourceEventAuthor {
			allowed[event.PubKey] = struct{}{}
		}
		if len(allowed) == 0 {
			continue
		}
		states[event.ID] = &authoredReplyChainState{
			source:         event,
			current:        event,
			allowedPubKeys: allowed,
			visited:        map[string]struct{}{event.ID: {}},
		}
	}
	if len(states) == 0 {
		return out, nil
	}

	for depth := 0; depth < input.MaxDepth; depth++ {
		targets := make([]string, 0, len(states))
		targetToSourceID := make(map[string][]string, len(states))
		seenTargets := map[string]struct{}{}
		queryPubKeys := map[string]struct{}{}
		for sourceID, state := range states {
			if state.done {
				continue
			}
			target := targetValue(state.current, input.Target)
			if input.Via.Value != "" {
				target = input.Via.Value
			}
			if target == "" {
				state.done = true
				continue
			}
			if _, ok := seenTargets[target]; !ok {
				seenTargets[target] = struct{}{}
				targets = append(targets, target)
			}
			targetToSourceID[target] = append(targetToSourceID[target], sourceID)
			for pubkey := range state.allowedPubKeys {
				queryPubKeys[pubkey] = struct{}{}
			}
		}
		if len(targets) == 0 {
			break
		}
		sort.Strings(targets)

		query := input.Events
		query.PubKeys = mapKeys(queryPubKeys)
		query.Limit = uint64(input.MaxBranchFanout)
		candidatesByTarget, err := c.store.QueryEventsByTagTargets(ctx, query, chstore.TagFilter{Key: input.Via.Key}, targets, query.Limit)
		if err != nil {
			return nil, err
		}

		advanced := false
		for _, target := range targets {
			for _, sourceID := range targetToSourceID[target] {
				state := states[sourceID]
				if state == nil || state.done {
					continue
				}
				child, ok := bestAuthoredDirectChild(candidatesByTarget[target], target, state.allowedPubKeys, input.Via, state.visited)
				if !ok {
					state.done = true
					continue
				}
				state.visited[child.ID] = struct{}{}
				out[sourceID] = append(out[sourceID], child)
				state.current = child
				advanced = true
			}
		}
		if !advanced {
			break
		}
	}
	return out, nil
}

func bestAuthoredDirectChild(
	candidates []chstore.EventView,
	parentTarget string,
	allowedPubKeys map[string]struct{},
	via graphTagPredicate,
	visited map[string]struct{},
) (chstore.EventView, bool) {
	var best chstore.EventView
	found := false
	for _, candidate := range candidates {
		if candidate.ID == "" {
			continue
		}
		if _, seen := visited[candidate.ID]; seen {
			continue
		}
		if _, allowed := allowedPubKeys[candidate.PubKey]; !allowed {
			continue
		}
		if !directChildMatches(candidate, parentTarget, via) {
			continue
		}
		if !found || candidate.CreatedAt.Before(best.CreatedAt) || (candidate.CreatedAt.Equal(best.CreatedAt) && candidate.ID < best.ID) {
			best = candidate
			found = true
		}
	}
	return best, found
}

func directChildMatches(candidate chstore.EventView, parentTarget string, via graphTagPredicate) bool {
	if via.Key == "e" {
		if parentID := directReplyParentID(candidate); parentID != "" {
			return parentID == parentTarget
		}
	}
	for _, tag := range candidate.Tags {
		predicate := via
		predicate.Value = parentTarget
		predicate.Values = nil
		if sourceTagMatches(tag, predicate) {
			return true
		}
	}
	return false
}

func directReplyParentID(event chstore.EventView) string {
	var firstE string
	var lastUnmarkedE string
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "e" || !hex64Pattern.MatchString(tag[1]) {
			continue
		}
		if firstE == "" {
			firstE = tag[1]
		}
		marker := ""
		if len(tag) >= 4 {
			marker = strings.ToLower(tag[3])
		}
		switch marker {
		case "reply":
			return tag[1]
		case "":
			lastUnmarkedE = tag[1]
		}
	}
	if lastUnmarkedE != "" {
		return lastUnmarkedE
	}
	return firstE
}

func (c *eventRelationCache) parseEventQueryInput(ctx context.Context, raw map[string]any) (chstore.EventQueryInput, error) {
	input, err := parseEventQueryInput(raw)
	if err != nil {
		return input, err
	}
	sources := pubkeySources(raw["pubkeysFrom"])
	if len(sources) == 0 {
		return input, nil
	}
	derived, err := c.resolvePubkeySources(ctx, sources)
	if err != nil {
		return input, err
	}
	input.PubKeys = uniqueStrings(append(input.PubKeys, derived...))
	if len(input.PubKeys) == 0 {
		input.Empty = true
	}
	return input, nil
}

func (c *eventRelationCache) resolvePubkeySources(ctx context.Context, sources []pubkeySource) ([]string, error) {
	var out []string
	for _, source := range sources {
		if source.latestEventTags == nil {
			continue
		}
		values, err := c.resolveLatestEventTagPubkeys(ctx, *source.latestEventTags)
		if err != nil {
			return nil, err
		}
		out = append(out, values...)
	}
	return uniqueStrings(out), nil
}

func (c *eventRelationCache) candidatePubkeyBoosts(ctx context.Context, raw any) ([]candidatePubkeyBoost, error) {
	return candidatePubkeyBoosts(ctx, raw, c.resolvePubkeySources)
}

func (c *eventRelationCache) resolveLatestEventTagPubkeys(ctx context.Context, source latestEventTagPubkeySource) ([]string, error) {
	normalized, err := normalizeLatestEventTagPubkeySource(source)
	if err != nil {
		return nil, err
	}
	key := "latestEventTags:" + latestEventTagPubkeySourceSignature(normalized)

	c.mu.Lock()
	cached, ok := c.latestEventTags[key]
	c.mu.Unlock()
	if !ok {
		value, err, _ := c.group.Do(key, func() (any, error) {
			c.mu.Lock()
			if existing, exists := c.latestEventTags[key]; exists {
				c.mu.Unlock()
				return existing, nil
			}
			c.mu.Unlock()

			events, err := c.store.QueryLatestEventsByPubKeys(ctx, []string{normalized.PubKey}, normalized.Kinds, uint64(normalized.Limit))
			if err != nil {
				return nil, err
			}
			values := latestEventTagPubkeyValues(events[normalized.PubKey], normalized)

			c.mu.Lock()
			c.latestEventTags[key] = values
			c.mu.Unlock()
			return values, nil
		})
		if err != nil {
			return nil, err
		}
		cached, _ = value.([]string)
	}
	return append([]string(nil), cached...), nil
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

func mapKeys(values map[string]struct{}) []string {
	out := make([]string, 0, len(values))
	for value := range values {
		if value != "" {
			out = append(out, value)
		}
	}
	sort.Strings(out)
	return out
}

func graphInputSignature(value any) string {
	encoded, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprintf("%#v", value)
	}
	return string(encoded)
}

func latestEventTagPubkeySourceSignature(source latestEventTagPubkeySource) string {
	return strings.Join([]string{
		source.PubKey,
		kindSignature(source.Kinds),
		graphTagPredicateSignature(source.Tag),
		strconv.Itoa(source.Limit),
		strconv.Itoa(source.MaxValues),
	}, "|")
}

func graphTagPredicateSignature(predicate graphTagPredicate) string {
	return strings.Join([]string{
		predicate.Key,
		predicate.Value,
		strings.Join(uniqueStrings(predicate.Values), ","),
		predicate.Marker,
		strings.Join(uniqueStrings(predicate.Markers), ","),
		strings.Join(uniqueStrings(predicate.ExcludeMarkers), ","),
		strconv.Itoa(predicate.Index),
	}, "\x00")
}

func eventViewMapLen(values map[string][]chstore.EventView) int {
	var total int
	for _, events := range values {
		total += len(events)
	}
	return total
}

func aggregateRowMapLen(values map[string][]chstore.AggregateRow) int {
	var total int
	for _, rows := range values {
		total += len(rows)
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

func takeUnvisited(visited map[string]struct{}, ids []string, max int) []string {
	out := make([]string, 0, min(len(ids), max))
	for _, id := range ids {
		if _, ok := visited[id]; ok {
			continue
		}
		visited[id] = struct{}{}
		out = append(out, id)
		if len(out) >= max {
			break
		}
	}
	return out
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		if key != "" {
			out = append(out, key)
		}
	}
	sort.Strings(out)
	return out
}

func chunks(values []string, size int) [][]string {
	var out [][]string
	for start := 0; start < len(values); start += size {
		end := min(start+size, len(values))
		out = append(out, values[start:end])
	}
	return out
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
