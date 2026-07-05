package graphqlapi

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/graphql-go/graphql"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

// dmEnvelopes returns the DM envelopes involving the viewer, across the
// requested kinds (NIP-04 kind 4 and NIP-17 gift wraps kind 1059 by default).
// nagg never decrypts: it returns the raw events and the client decrypts and
// buckets them by counterparty. The query unions "author = viewer" with
// "p-tag = viewer" — the one shape the generic events query cannot OR in a
// single call — then dedupes and orders by createdAt DESC.
func (r *resolver) dmEnvelopes(ctx context.Context, raw map[string]any) (eventConnectionSource, error) {
	viewer := normalizeViewer(raw["viewer"])
	if err := validateHex64(viewer); err != nil {
		return eventConnectionSource{}, fmt.Errorf("dmEnvelopes viewer: %w", err)
	}
	kinds := dmKinds(raw["kinds"])
	limit := clampLimit(intValue(raw["limit"], 50), 200)
	until := int64(intValue(raw["until"], 0))

	r.tryBackfillDMEnvelopes(ctx, viewer, kinds, until, uint64(limit))
	authored, err := r.queryEvents(ctx, chstore.EventQueryInput{
		PubKeys: []string{viewer}, Kinds: kinds, Until: until, Limit: uint64(limit),
	})
	if err != nil {
		return eventConnectionSource{}, err
	}
	received, err := r.queryEvents(ctx, chstore.EventQueryInput{
		Tags:  []chstore.TagFilter{{Key: "p", Value: viewer}},
		Kinds: kinds, Until: until, Limit: uint64(limit),
	})
	if err != nil {
		return eventConnectionSource{}, err
	}
	return r.newEventConnection(mergeEventsDescDedup(limit, authored, received)), nil
}

// dmConversation returns the message history for one conversation. For NIP-04
// (kind 4) the counterparty is visible, so the result is scoped to the pair.
// For gift wraps (kind 1059) the counterparty is opaque, so all wraps addressed
// to the viewer are returned and the client buckets them after decryption.
func (r *resolver) dmConversation(ctx context.Context, raw map[string]any) (eventConnectionSource, error) {
	viewer := normalizeViewer(raw["viewer"])
	if err := validateHex64(viewer); err != nil {
		return eventConnectionSource{}, fmt.Errorf("dmConversation viewer: %w", err)
	}
	counterparty := normalizeViewer(raw["counterparty"])
	hasCounterparty := counterparty != ""
	if hasCounterparty {
		if err := validateHex64(counterparty); err != nil {
			return eventConnectionSource{}, fmt.Errorf("dmConversation counterparty: %w", err)
		}
	}
	kinds := dmKinds(raw["kinds"])
	limit := clampLimit(intValue(raw["limit"], 50), 200)
	until := int64(intValue(raw["until"], 0))

	r.tryBackfillDMEnvelopes(ctx, viewer, kinds, until, uint64(limit))
	// Split kinds: kind 4 can be scoped to the pair; everything else (gift
	// wraps) is opaque and is returned as the full viewer inbox.
	var directKinds, opaqueKinds []int
	for _, k := range kinds {
		if k == 4 {
			directKinds = append(directKinds, k)
		} else {
			opaqueKinds = append(opaqueKinds, k)
		}
	}

	var collected []chstore.EventView
	if len(directKinds) > 0 {
		if hasCounterparty {
			sent, err := r.queryEvents(ctx, chstore.EventQueryInput{
				PubKeys: []string{viewer}, Kinds: directKinds,
				Tags: []chstore.TagFilter{{Key: "p", Value: counterparty}}, Until: until, Limit: uint64(limit),
			})
			if err != nil {
				return eventConnectionSource{}, err
			}
			got, err := r.queryEvents(ctx, chstore.EventQueryInput{
				PubKeys: []string{counterparty}, Kinds: directKinds,
				Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Until: until, Limit: uint64(limit),
			})
			if err != nil {
				return eventConnectionSource{}, err
			}
			collected = append(collected, sent...)
			collected = append(collected, got...)
		} else {
			sent, err := r.queryEvents(ctx, chstore.EventQueryInput{
				PubKeys: []string{viewer}, Kinds: directKinds, Until: until, Limit: uint64(limit),
			})
			if err != nil {
				return eventConnectionSource{}, err
			}
			got, err := r.queryEvents(ctx, chstore.EventQueryInput{
				Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Kinds: directKinds, Until: until, Limit: uint64(limit),
			})
			if err != nil {
				return eventConnectionSource{}, err
			}
			collected = append(collected, sent...)
			collected = append(collected, got...)
		}
	}
	if len(opaqueKinds) > 0 {
		wraps, err := r.queryEvents(ctx, chstore.EventQueryInput{
			Tags: []chstore.TagFilter{{Key: "p", Value: viewer}}, Kinds: opaqueKinds, Until: until, Limit: uint64(limit),
		})
		if err != nil {
			return eventConnectionSource{}, err
		}
		collected = append(collected, wraps...)
	}
	return r.newEventConnection(mergeEventsDescDedup(limit, collected)), nil
}

// followStatus returns the follow relationship between the viewer and each
// candidate in a single round-trip.
func (r *resolver) followStatus(ctx context.Context, raw map[string]any) ([]map[string]any, error) {
	viewer := normalizeViewer(raw["viewer"])
	if err := validateHex64(viewer); err != nil {
		return nil, fmt.Errorf("followStatus viewer: %w", err)
	}
	candidates := validHex64List(raw["candidates"])
	if len(candidates) > 500 {
		candidates = candidates[:500]
	}
	edges, err := r.store.FollowEdges(ctx, viewer, candidates)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(candidates))
	for _, candidate := range candidates {
		edge := edges[candidate]
		mutual := edge.Following && edge.FollowsYou
		relationship := "none"
		switch {
		case mutual:
			relationship = "mutual"
		case edge.Following:
			relationship = "following"
		case edge.FollowsYou:
			relationship = "follows_you"
		}
		out = append(out, map[string]any{
			"pubkey":       candidate,
			"following":    edge.Following,
			"followsYou":   edge.FollowsYou,
			"mutual":       mutual,
			"relationship": relationship,
		})
	}
	return out, nil
}

// ownProfiles returns metadata plus follower/following counts for a small set
// of the viewer's own accounts (capped at 10).
func (r *resolver) ownProfiles(ctx context.Context, rawPubkeys any) ([]map[string]any, error) {
	pubkeys := validHex64List(rawPubkeys)
	if len(pubkeys) > 10 {
		pubkeys = pubkeys[:10]
	}
	if len(pubkeys) == 0 {
		return []map[string]any{}, nil
	}
	profiles, err := r.store.LatestK0(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	counts, err := r.store.BatchPubkeyStats(ctx, pubkeys)
	if err != nil {
		return nil, err
	}
	out := make([]map[string]any, 0, len(pubkeys))
	for _, pubkey := range pubkeys {
		profile := profiles[pubkey]
		count := counts[pubkey]
		var createdAt any
		if !profile.CreatedAt.IsZero() {
			createdAt = profile.CreatedAt
		}
		out = append(out, map[string]any{
			"pubkey":      pubkey,
			"name":        profile.Name,
			"displayName": profile.DisplayName,
			"picture":     profile.Picture,
			"about":       profile.About,
			"nip05":       profile.NIP05,
			"lud16":       profile.LUD16,
			"banner":      profile.Banner,
			"website":     profile.Website,
			"followers":   int(count.Followers),
			"follows":     int(count.Follows),
			"createdAt":   createdAt,
		})
	}
	return out, nil
}

// socialQueryFields builds the DM/social root query fields. They are added to
// the Query type after it is constructed so the large schema literal stays
// untouched.
func socialQueryFields(r *resolver, eventConnectionType *graphql.Object) graphql.Fields {
	dmEnvelopesInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "DmEnvelopesInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"viewer": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"kinds":  &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
			"until":  &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"limit":  &graphql.InputObjectFieldConfig{Type: graphql.Int},
		},
	})
	dmConversationInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "DmConversationInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"viewer":       &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"counterparty": &graphql.InputObjectFieldConfig{Type: graphql.String},
			"kinds":        &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
			"until":        &graphql.InputObjectFieldConfig{Type: graphql.Int},
			"limit":        &graphql.InputObjectFieldConfig{Type: graphql.Int},
		},
	})
	followStatusInput := graphql.NewInputObject(graphql.InputObjectConfig{
		Name: "FollowStatusInput",
		Fields: graphql.InputObjectConfigFieldMap{
			"viewer":     &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"candidates": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))},
		},
	})
	followStatusRowType := graphql.NewObject(graphql.ObjectConfig{
		Name: "FollowStatusRow",
		Fields: graphql.Fields{
			"pubkey":       &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"following":    &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"followsYou":   &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"mutual":       &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"relationship": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
	})
	ownProfileType := graphql.NewObject(graphql.ObjectConfig{
		Name: "OwnProfile",
		Fields: graphql.Fields{
			"pubkey":      &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"name":        &graphql.Field{Type: graphql.String},
			"displayName": &graphql.Field{Type: graphql.String},
			"picture":     &graphql.Field{Type: graphql.String},
			"about":       &graphql.Field{Type: graphql.String},
			"nip05":       &graphql.Field{Type: graphql.String},
			"lud16":       &graphql.Field{Type: graphql.String},
			"banner":      &graphql.Field{Type: graphql.String},
			"website":     &graphql.Field{Type: graphql.String},
			"followers":   &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"follows":     &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
			"createdAt":   &graphql.Field{Type: graphql.DateTime},
		},
	})

	return graphql.Fields{
		"dmEnvelopes": &graphql.Field{
			Type: graphql.NewNonNull(eventConnectionType),
			Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(dmEnvelopesInput)}},
			Resolve: func(p graphql.ResolveParams) (any, error) {
				return r.dmEnvelopes(p.Context, p.Args["input"].(map[string]any))
			},
		},
		"dmConversation": &graphql.Field{
			Type: graphql.NewNonNull(eventConnectionType),
			Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(dmConversationInput)}},
			Resolve: func(p graphql.ResolveParams) (any, error) {
				return r.dmConversation(p.Context, p.Args["input"].(map[string]any))
			},
		},
		"followStatus": &graphql.Field{
			Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(followStatusRowType))),
			Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(followStatusInput)}},
			Resolve: func(p graphql.ResolveParams) (any, error) {
				return r.followStatus(p.Context, p.Args["input"].(map[string]any))
			},
		},
		"ownProfiles": &graphql.Field{
			Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(ownProfileType))),
			Args: graphql.FieldConfigArgument{"pubkeys": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))}},
			Resolve: func(p graphql.ResolveParams) (any, error) {
				return r.ownProfiles(p.Context, p.Args["pubkeys"])
			},
		},
	}
}

// dmKinds returns the requested DM kinds, defaulting to NIP-04 + NIP-17 wraps.
func dmKinds(raw any) []int {
	kinds := intList(raw)
	if len(kinds) == 0 {
		return []int{4, 1059}
	}
	return kinds
}

func (r *resolver) tryBackfillDMEnvelopes(ctx context.Context, pubkey string, kinds []int, until int64, limit uint64) bool {
	if r.dmEnvelopeBackfiller == nil || pubkey == "" {
		return false
	}
	if hydrator, ok := r.dmEnvelopeBackfiller.(DMEnvelopeHydrator); ok {
		completed, err := hydrator.HydrateDMEnvelopes(ctx, pubkey, kinds, until, limit)
		if err != nil {
			slog.Warn("graphql dm envelope hydration failed", "pubkey", pubkey, "error", err)
			return false
		}
		return completed
	}
	if err := r.dmEnvelopeBackfiller.BackfillDMEnvelopes(ctx, pubkey, kinds, until, limit); err != nil {
		slog.Warn("graphql dm envelope backfill failed", "pubkey", pubkey, "error", err)
		return false
	}
	return true
}

func clampLimit(limit, max int) int {
	if limit <= 0 {
		return 50
	}
	if limit > max {
		return max
	}
	return limit
}

func normalizeViewer(raw any) string {
	s, _ := raw.(string)
	return strings.ToLower(strings.TrimSpace(s))
}

func validHex64List(raw any) []string {
	values := stringList(raw)
	out := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		normalized := strings.ToLower(strings.TrimSpace(value))
		if validateHex64(normalized) != nil {
			continue
		}
		if _, ok := seen[normalized]; ok {
			continue
		}
		seen[normalized] = struct{}{}
		out = append(out, normalized)
	}
	return out
}

// mergeEventsDescDedup merges event slices, dedupes by id, orders by createdAt
// DESC (id DESC tiebreak), and truncates to limit.
func mergeEventsDescDedup(limit int, lists ...[]chstore.EventView) []chstore.EventView {
	seen := make(map[string]struct{})
	merged := make([]chstore.EventView, 0)
	for _, list := range lists {
		for _, event := range list {
			if _, ok := seen[event.ID]; ok {
				continue
			}
			seen[event.ID] = struct{}{}
			merged = append(merged, event)
		}
	}
	sort.SliceStable(merged, func(i, j int) bool {
		if merged[i].CreatedAt.Equal(merged[j].CreatedAt) {
			return merged[i].ID > merged[j].ID
		}
		return merged[i].CreatedAt.After(merged[j].CreatedAt)
	})
	if limit > 0 && len(merged) > limit {
		merged = merged[:limit]
	}
	return merged
}
