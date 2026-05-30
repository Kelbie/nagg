package graphqlapi

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

type Store interface {
	EventByID(context.Context, string) (*chstore.EventView, error)
	QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error)
	AggregateEvents(context.Context, chstore.AggregateInput) ([]chstore.AggregateRow, error)
}

var hex64Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type resolver struct {
	store Store
}

func NewSchema(store Store) (graphql.Schema, error) {
	r := &resolver{store: store}
	jsonType := jsonScalar("JSON")

	eventType := graphql.NewObject(graphql.ObjectConfig{
		Name: "NostrEvent",
		Fields: graphql.Fields{
			"id":        &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"pubkey":    &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"kind":      &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"createdAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime)},
			"content":   &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"tags":      &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))))},
			"sig":       &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"updatedAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime)},
		},
	})

	pageInfoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "PageInfo",
		Fields: graphql.Fields{
			"hasNextPage": &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"endCursor":   &graphql.Field{Type: graphql.String},
		},
	})

	eventConnectionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "EventConnection",
		Fields: graphql.Fields{
			"nodes": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(eventType))),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					return p.Source, nil
				},
			},
			"pageInfo": &graphql.Field{
				Type: graphql.NewNonNull(pageInfoType),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					nodes, _ := p.Source.([]chstore.EventView)
					return map[string]any{"hasNextPage": false, "endCursor": eventEndCursor(nodes)}, nil
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
			"profiles": &graphql.Field{
				Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(eventType))),
			},
		},
	})

	tagFilterType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "TagFilterInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"key":    &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"value":  &graphql.InputObjectFieldConfig{Type: graphql.String},
			"values": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
		},
	})

	eventQueryInputType := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "EventQueryInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"ids":     &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"pubkeys": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"kinds":   &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
			"tags":    &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(tagFilterType))},
			"limit":   &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 50},
		},
	})

	aggregateRowType := graphql.NewObject(graphql.ObjectConfig{
		Name: "AggregationRow",
		Fields: graphql.Fields{
			"dimensions": &graphql.Field{Type: graphql.NewNonNull(jsonType)},
			"metrics":    &graphql.Field{Type: graphql.NewNonNull(jsonType)},
		},
	})
	aggregationResultType := graphql.NewObject(graphql.ObjectConfig{
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
			"limit":   &graphql.InputObjectFieldConfig{Type: graphql.Int, DefaultValue: 100},
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
					return r.store.EventByID(p.Context, id)
				},
			},
			"events": &graphql.Field{
				Type: graphql.NewNonNull(eventConnectionType),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(eventQueryInputType)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					input, err := parseEventQueryInput(p.Args["input"].(map[string]any))
					if err != nil {
						return nil, err
					}
					return r.store.QueryEvents(p.Context, input)
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
					rows, err := r.store.AggregateEvents(p.Context, input)
					return map[string]any{"rows": rows}, err
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

func (r *resolver) eventContext(ctx context.Context, id string, limit int) (map[string]any, error) {
	root, err := r.store.EventByID(ctx, id)
	if err != nil {
		return nil, err
	}

	eventsByID := map[string]chstore.EventView{root.ID: *root}
	visited := map[string]struct{}{}
	frontier := []string{root.ID}

	for depth := 0; depth < 8 && len(frontier) > 0 && len(eventsByID) < limit; depth++ {
		batch := takeUnvisited(visited, frontier, 100)
		if len(batch) == 0 {
			break
		}
		remaining := limit - len(eventsByID)
		if remaining <= 0 {
			break
		}
		events, err := r.store.QueryEvents(ctx, chstore.EventQueryInput{
			Tags:  []chstore.TagFilter{{Key: "e", Values: batch}},
			Limit: uint64(min(remaining, 500)),
		})
		if err != nil {
			return nil, err
		}
		frontier = frontier[:0]
		for _, event := range events {
			if _, ok := eventsByID[event.ID]; ok {
				continue
			}
			eventsByID[event.ID] = event
			frontier = append(frontier, event.ID)
		}
	}

	pubkeys := map[string]struct{}{}
	for _, event := range eventsByID {
		pubkeys[event.PubKey] = struct{}{}
		for _, tag := range event.Tags {
			if len(tag) >= 2 && tag[0] == "p" && hex64Pattern.MatchString(tag[1]) {
				pubkeys[tag[1]] = struct{}{}
			}
		}
	}

	profilesByPubkey := map[string]chstore.EventView{}
	for _, batch := range chunks(keys(pubkeys), 200) {
		profiles, err := r.store.QueryEvents(ctx, chstore.EventQueryInput{
			PubKeys: batch,
			Kinds:   []int{0},
			Limit:   uint64(min(len(batch)*2, 500)),
		})
		if err != nil {
			return nil, err
		}
		for _, profile := range profiles {
			if _, ok := profilesByPubkey[profile.PubKey]; ok {
				continue
			}
			profilesByPubkey[profile.PubKey] = profile
		}
	}

	events := make([]chstore.EventView, 0, len(eventsByID)-1)
	for _, event := range eventsByID {
		if event.ID != root.ID {
			events = append(events, event)
		}
	}
	profiles := make([]chstore.EventView, 0, len(profilesByPubkey))
	for _, profile := range profilesByPubkey {
		profiles = append(profiles, profile)
	}
	return map[string]any{"root": root, "events": events, "profiles": profiles}, nil
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
		Limit:   uint64(intValue(raw["limit"], 50)),
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
