package graphqlapi

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/graphql-go/graphql"
	"github.com/vertex-lab/nagg/internal/mintinfo"
)

// This file quarantines the ONE cashu-specific GraphQL surface. The generic
// event/aggregate/ranking schema stays protocol-neutral; mint-info history is a
// product concept (like the /nostr/mint/* REST routes), so it lives here and is
// merged into the Query type in NewSchema, exactly like socialQueryFields.

// MintHistoryProvider serves a mint's NUT-06 info history. Satisfied by
// *mintinfo.Reader.
type MintHistoryProvider interface {
	History(ctx context.Context, mintURL string, includeObservations bool) (*mintinfo.History, bool, error)
}

// mintQueryFields builds the mintInfoHistory root query field.
func mintQueryFields(r *resolver, jsonType *graphql.Scalar) graphql.Fields {
	snapshotType := graphql.NewObject(graphql.ObjectConfig{
		Name: "MintInfoSnapshot",
		Fields: graphql.Fields{
			"at":       &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"hash":     &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"document": &graphql.Field{Type: jsonType},
		},
	})
	revisionType := graphql.NewObject(graphql.ObjectConfig{
		Name: "MintInfoRevision",
		Fields: graphql.Fields{
			"at":                 &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"previousLastSeenAt": &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"hash":               &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"summary":            &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))},
			"patch":              &graphql.Field{Type: jsonType},
		},
	})
	observationType := graphql.NewObject(graphql.ObjectConfig{
		Name: "MintInfoObservation",
		Fields: graphql.Fields{
			"at":        &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"hash":      &graphql.Field{Type: graphql.String},
			"changed":   &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"reachable": &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
		},
	})
	historyType := graphql.NewObject(graphql.ObjectConfig{
		Name: "MintInfoHistory",
		Fields: graphql.Fields{
			"mintUrl":        &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"normalizedUrl":  &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"currentHash":    &graphql.Field{Type: graphql.String},
			"firstSeenAt":    &graphql.Field{Type: graphql.Int},
			"lastCheckedAt":  &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"checkCount":     &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"unchangedSince": &graphql.Field{Type: graphql.Int},
			"initial":        &graphql.Field{Type: snapshotType},
			"revisions":      &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(revisionType)))},
			"observations":   &graphql.Field{Type: graphql.NewList(graphql.NewNonNull(observationType))},
		},
	})
	input := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "MintInfoHistoryInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"mintUrl":             &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"includeObservations": &graphql.InputObjectFieldConfig{Type: graphql.Boolean},
		},
	})
	return graphql.Fields{
		"mintInfoHistory": &graphql.Field{
			Type: historyType,
			Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(input)}},
			Resolve: func(p graphql.ResolveParams) (any, error) {
				return r.mintInfoHistory(p.Context, p.Args["input"].(map[string]any))
			},
		},
	}
}

func (r *resolver) mintInfoHistory(ctx context.Context, raw map[string]any) (any, error) {
	if r.mintInfo == nil {
		return nil, nil
	}
	mintURL, _ := raw["mintUrl"].(string)
	if strings.TrimSpace(mintURL) == "" {
		return nil, fmt.Errorf("mintInfoHistory: mintUrl is required")
	}
	includeObservations, _ := raw["includeObservations"].(bool)
	history, found, err := r.mintInfo.History(ctx, mintURL, includeObservations)
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, nil
	}
	return mintHistoryToMap(history), nil
}

// mintHistoryToMap shapes the history for graphql-go's map resolver, unmarshaling
// the RawMessage document/patch so the JSON scalar serializes real objects rather
// than a byte string.
func mintHistoryToMap(h *mintinfo.History) map[string]any {
	out := map[string]any{
		"mintUrl":        h.MintURL,
		"normalizedUrl":  h.NormalizedURL,
		"currentHash":    h.CurrentHash,
		"firstSeenAt":    h.FirstSeenAt,
		"lastCheckedAt":  h.LastCheckedAt,
		"checkCount":     h.CheckCount,
		"unchangedSince": h.UnchangedSince,
	}
	if h.Initial != nil {
		out["initial"] = map[string]any{
			"at": h.Initial.At, "hash": h.Initial.Hash, "document": rawToAny(h.Initial.Document),
		}
	}
	revisions := make([]map[string]any, 0, len(h.Revisions))
	for _, rev := range h.Revisions {
		revisions = append(revisions, map[string]any{
			"at": rev.At, "previousLastSeenAt": rev.PreviousLastSeenAt, "hash": rev.Hash,
			"summary": rev.Summary, "patch": rawToAny(rev.Patch),
		})
	}
	out["revisions"] = revisions
	if h.Observations != nil {
		observations := make([]map[string]any, 0, len(h.Observations))
		for _, o := range h.Observations {
			observations = append(observations, map[string]any{
				"at": o.At, "hash": o.Hash, "changed": o.Changed, "reachable": o.Reachable,
			})
		}
		out["observations"] = observations
	}
	return out
}

func rawToAny(raw json.RawMessage) any {
	if len(raw) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return nil
	}
	return v
}
