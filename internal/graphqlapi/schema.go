package graphqlapi

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

type Store interface {
	EventByID(context.Context, string) (*chstore.EventView, error)
	QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error)
	QueryLatestEventsByPubKeys(context.Context, []string, []int, uint64) (map[string][]chstore.EventView, error)
	AggregateEvents(context.Context, chstore.AggregateInput) ([]chstore.AggregateRow, error)
	ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error)
}

var hex64Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type resolver struct {
	store          Store
	userBackfiller UserFeedBackfiller
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

type eventNode struct {
	event     chstore.EventView
	relations *pubkeyRelationCache
}

type eventConnectionSource struct {
	raw   []chstore.EventView
	nodes []eventNode
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
	r := &resolver{store: store}
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
	var aggregateReferencedByInputType *graphql.InputObject
	var referenceRankInputType *graphql.InputObject
	var rankedReverseReferenceInputType *graphql.InputObject
	var rankedEventsInputType *graphql.InputObject

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
				"rankedReferencedBy": &graphql.Field{
					Type: graphql.NewNonNull(eventConnectionType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(rankedReverseReferenceInputType)}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
						event, ok := eventFromSource(p.Source)
						if !ok {
							return eventConnectionSource{}, nil
						}
						return r.eventRankedReferencedBy(p.Context, event, p.Args["input"])
					},
				},
				"aggregateReferencedBy": &graphql.Field{
					Type: graphql.NewNonNull(aggregationResultType),
					Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(aggregateReferencedByInputType)}},
					Resolve: func(p graphql.ResolveParams) (any, error) {
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
			"marker": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"index":  &graphql.InputObjectFieldConfig{Type: graphql.Int},
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
			"latestEventTags": &graphql.InputObjectFieldConfig{Type: latestEventTagPubkeySourceInputType},
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
	referenceRankInputType = graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "ReferenceRankInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"references": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(eventQueryInputType)},
			"via":        &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(tagFilterType)},
			"metric":     &graphql.InputObjectFieldConfig{Type: metricInputType},
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
			"limit":      &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 30},
			"offset":     &graphql.InputObjectFieldConfig{Type: graphql.Int},
		},
	})

	queryType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Query",
		Fields: graphql.Fields{
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
					return wrapEvent(*event, newPubkeyRelationCache(r.store, []chstore.EventView{*event})), nil
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
					relations := newPubkeyRelationCache(r.store, events)
					return eventConnectionSource{raw: events, nodes: wrapEvents(events, relations)}, nil
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
	return map[string]any{
		"root":   wrapEvent(*root, relations),
		"events": wrapEvents(events, relations),
	}, nil
}

type graphTagPredicate struct {
	Key    string
	Value  string
	Values []string
	Marker string
	Index  int
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
	relations := newPubkeyRelationCache(r.store, events)
	return eventConnectionSource{raw: events, nodes: wrapEvents(events, relations)}, nil
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
	relations := newPubkeyRelationCache(r.store, events)
	return eventConnectionSource{raw: events, nodes: wrapEvents(events, relations)}, nil
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
	rows, err := r.store.AggregateEvents(ctx, rankAggregate)
	if err != nil {
		return eventConnectionSource{}, err
	}
	targetIDs := rankedCandidateIDs(rows, candidates, input.Offset, input.Limit)
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
	relations := newPubkeyRelationCache(r.store, ordered)
	return eventConnectionSource{raw: ordered, nodes: wrapEvents(ordered, relations)}, nil
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
	return map[string]any{"rows": rows}, nil
}

func (r *resolver) rankedEvents(ctx context.Context, raw any) (eventConnectionSource, error) {
	input, err := r.parseRankedEventsInput(ctx, raw)
	if err != nil {
		return eventConnectionSource{}, err
	}
	rows, err := r.store.AggregateEvents(ctx, input.References)
	if err != nil {
		return eventConnectionSource{}, err
	}
	targetIDs := rankedTargetIDs(rows, input.Offset, input.Limit)
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
	relations := newPubkeyRelationCache(r.store, ordered)
	return eventConnectionSource{raw: ordered, nodes: wrapEvents(ordered, relations)}, nil
}

type referenceInput struct {
	Tags  []graphTagPredicate
	Limit int
}

type rankedEventsInput struct {
	References chstore.AggregateInput
	Target     chstore.EventQueryInput
	Limit      int
	Offset     int
}

type rankedReverseReferenceInput struct {
	Events         chstore.EventQueryInput
	RankReferences chstore.AggregateInput
	RankVia        graphTagPredicate
	Limit          int
	Offset         int
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
		parsed, err := r.parseEventQueryInput(ctx, eventsRaw)
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
		events, err := r.parseEventQueryInput(ctx, eventsRaw)
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
	out.Limit = intValue(m["limit"], 1)
	if out.Limit <= 0 || out.Limit > 50 {
		out.Limit = 1
	}
	out.Offset = intValue(m["offset"], 0)
	if out.Offset < 0 {
		out.Offset = 0
	}
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
		Target: chstore.EventQueryInput{Limit: uint64(limit)},
		Limit:  limit,
		Offset: offset,
	}
	if targetRaw, ok := m["target"].(map[string]any); ok {
		target, err := r.parseEventQueryInput(ctx, targetRaw)
		if err != nil {
			return out, err
		}
		out.Target = target
	}
	out.Target.Limit = uint64(limit)
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
		Key:    stringValue(raw["key"]),
		Value:  stringValue(raw["value"]),
		Values: stringList(raw["values"]),
		Marker: stringValue(raw["marker"]),
		Index:  intValue(raw["index"], -1),
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
	latestEventTags *latestEventTagPubkeySource
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

func (r *resolver) resolveLatestEventTagPubkeys(ctx context.Context, source latestEventTagPubkeySource) ([]string, error) {
	if err := validateHex64(source.PubKey); err != nil {
		return nil, fmt.Errorf("pubkeysFrom.latestEventTags.pubkey: %w", err)
	}
	if len(source.Kinds) == 0 {
		return nil, fmt.Errorf("pubkeysFrom.latestEventTags.kinds is required")
	}
	if source.Tag.Key == "" {
		return nil, fmt.Errorf("pubkeysFrom.latestEventTags.tag.key is required")
	}
	if source.Limit <= 0 || source.Limit > 20 {
		source.Limit = 1
	}
	if source.MaxValues <= 0 || source.MaxValues > 5000 {
		source.MaxValues = 2000
	}
	events, err := r.store.QueryLatestEventsByPubKeys(ctx, []string{source.PubKey}, uniqueInts(source.Kinds), uint64(source.Limit))
	if err != nil {
		return nil, err
	}
	latest := events[source.PubKey]
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
				return out, nil
			}
		}
	}
	return out, nil
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
		if source.latestEventTags != nil {
			out = append(out, source)
		}
	}
	return out
}

func Handler(schema graphql.Schema) http.HandlerFunc {
	type request struct {
		Query         string         `json:"query"`
		OperationName string         `json:"operationName"`
		Variables     map[string]any `json:"variables"`
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST /graphql only", http.StatusMethodNotAllowed)
			return
		}
		var req request
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		result := graphql.Do(graphql.Params{
			Schema:         schema,
			RequestString:  req.Query,
			OperationName:  req.OperationName,
			VariableValues: req.Variables,
			Context:        ctx,
		})
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	}
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

func tagFilters(v any) []chstore.TagFilter {
	values := anyList(v)
	out := make([]chstore.TagFilter, 0, len(values))
	for _, value := range values {
		raw, ok := value.(map[string]any)
		if !ok {
			continue
		}
		out = append(out, chstore.TagFilter{
			Key:    fmt.Sprint(raw["key"]),
			Value:  stringValue(raw["value"]),
			Values: stringList(raw["values"]),
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

func wrapEvent(event chstore.EventView, relations *pubkeyRelationCache) eventNode {
	return eventNode{event: event, relations: relations}
}

func wrapEvents(events []chstore.EventView, relations *pubkeyRelationCache) []eventNode {
	out := make([]eventNode, 0, len(events))
	for _, event := range events {
		out = append(out, wrapEvent(event, relations))
	}
	return out
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
	return wrapEvents(related, newPubkeyRelationCache(c.store, related)), nil
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
