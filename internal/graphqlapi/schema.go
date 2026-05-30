package graphqlapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"
	"time"

	"github.com/graphql-go/graphql"
	"github.com/graphql-go/graphql/language/ast"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

type Store interface {
	EventByID(context.Context, string) (*chstore.EventView, error)
	ProfileByPubKey(context.Context, string) (*chstore.ProfileView, error)
	LikeCount(context.Context, string) (uint64, error)
	RepostCount(context.Context, string) (uint64, error)
	CommentCount(context.Context, string) (uint64, error)
	DirectReplyCount(context.Context, string) (uint64, error)
	ThreadParticipants(context.Context, string) (uint64, error)
	ReactionTallies(context.Context, string, uint64) ([]chstore.ActorEdge, error)
	Likers(context.Context, string, uint64) ([]chstore.ActorEdge, error)
	Reposters(context.Context, string, uint64) ([]chstore.ActorEdge, error)
	Comments(context.Context, string, uint64, bool) ([]chstore.CommentView, error)
	Followers(context.Context, string) (uint64, error)
	Following(context.Context, string) (uint64, error)
	FollowerList(context.Context, string, uint64) ([]chstore.ActorEdge, error)
	FollowingList(context.Context, string, uint64) ([]chstore.ActorEdge, error)
	FollowedBy(context.Context, string, string) (bool, error)
	AggregateEvents(context.Context, chstore.AggregateInput) ([]chstore.AggregateRow, error)
}

var hex64Pattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

type resolver struct {
	store Store
}

func NewSchema(store Store) (graphql.Schema, error) {
	r := &resolver{store: store}

	pageInfoType := graphql.NewObject(graphql.ObjectConfig{
		Name: "PageInfo",
		Fields: graphql.Fields{
			"hasNextPage": &graphql.Field{Type: graphql.NewNonNull(graphql.Boolean)},
			"endCursor":   &graphql.Field{Type: graphql.String},
		},
	})

	profileMetadataType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ProfileMetadata",
		Fields: graphql.Fields{
			"name":        &graphql.Field{Type: graphql.String},
			"displayName": &graphql.Field{Type: graphql.String},
			"picture":     &graphql.Field{Type: graphql.String},
			"about":       &graphql.Field{Type: graphql.String},
			"nip05":       &graphql.Field{Type: graphql.String},
			"lud16":       &graphql.Field{Type: graphql.String},
		},
	})

	var profileType *graphql.Object
	var eventType *graphql.Object
	var threadType *graphql.Object
	var commentType *graphql.Object

	reactionTallyType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ReactionTally",
		Fields: graphql.Fields{
			"content": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			"count":   &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
		},
	})

	reactionEdgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ReactionEdge",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"node":      &graphql.Field{Type: graphql.NewNonNull(profileType)},
				"content":   &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
				"reactedAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime)},
				"cursor":    &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			}
		}),
	})
	repostEdgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: "RepostEdge",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"node":       &graphql.Field{Type: graphql.NewNonNull(profileType)},
				"repostedAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime)},
				"cursor":     &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			}
		}),
	})
	followEdgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: "FollowEdge",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"node":       &graphql.Field{Type: graphql.NewNonNull(profileType)},
				"followedAt": &graphql.Field{Type: graphql.DateTime},
				"cursor":     &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			}
		}),
	})

	reactionConnectionType := connectionType("ReactionConnection", reactionEdgeType, pageInfoType)
	repostConnectionType := connectionType("RepostConnection", repostEdgeType, pageInfoType)
	followConnectionType := connectionType("FollowConnection", followEdgeType, pageInfoType)

	commentEdgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: "CommentEdge",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"node":   &graphql.Field{Type: graphql.NewNonNull(commentType)},
				"cursor": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
			}
		}),
	})
	commentConnectionType := connectionType("CommentConnection", commentEdgeType, pageInfoType)

	zapType := graphql.NewObject(graphql.ObjectConfig{
		Name: "Zap",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id":          &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
				"zapper":      &graphql.Field{Type: profileType},
				"amountSats":  &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
				"amountMsats": &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
				"comment":     &graphql.Field{Type: graphql.String},
				"zappedAt":    &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime)},
			}
		}),
	})
	zapEdgeType := graphql.NewObject(graphql.ObjectConfig{
		Name: "ZapEdge",
		Fields: graphql.Fields{
			"node":   &graphql.Field{Type: graphql.NewNonNull(zapType)},
			"cursor": &graphql.Field{Type: graphql.NewNonNull(graphql.String)},
		},
	})
	zapConnectionType := connectionType("ZapConnection", zapEdgeType, pageInfoType)

	profileType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Profile",
		Fields: graphql.Fields{
			"pubkey": &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: func(p graphql.ResolveParams) (any, error) {
				return p.Source.(*chstore.ProfileView).PubKey, nil
			}},
			"metadata": &graphql.Field{Type: profileMetadataType, Resolve: func(p graphql.ResolveParams) (any, error) {
				profile := p.Source.(*chstore.ProfileView)
				if profile.Name == "" && profile.DisplayName == "" && profile.Picture == "" && profile.About == "" && profile.NIP05 == "" && profile.LUD16 == "" {
					return nil, nil
				}
				return map[string]any{
					"name": profile.Name, "displayName": profile.DisplayName, "picture": profile.Picture,
					"about": profile.About, "nip05": profile.NIP05, "lud16": profile.LUD16,
				}, nil
			}},
			"followers": &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: r.profileCount(func(ctx context.Context, pubkey string) (uint64, error) {
				return r.store.Followers(ctx, pubkey)
			})},
			"following": &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: r.profileCount(func(ctx context.Context, pubkey string) (uint64, error) {
				return r.store.Following(ctx, pubkey)
			})},
			"followerList": &graphql.Field{Type: graphql.NewNonNull(followConnectionType), Args: firstArg(), Resolve: func(p graphql.ResolveParams) (any, error) {
				profile := p.Source.(*chstore.ProfileView)
				edges, err := r.store.FollowerList(p.Context, profile.PubKey, first(p))
				return profileConnection(p.Context, r.store, edges), err
			}},
			"followingList": &graphql.Field{Type: graphql.NewNonNull(followConnectionType), Args: firstArg(), Resolve: func(p graphql.ResolveParams) (any, error) {
				profile := p.Source.(*chstore.ProfileView)
				edges, err := r.store.FollowingList(p.Context, profile.PubKey, first(p))
				return profileConnection(p.Context, r.store, edges), err
			}},
			"followedBy": &graphql.Field{
				Type: graphql.NewNonNull(graphql.Boolean),
				Args: graphql.FieldConfigArgument{"viewerPubkey": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					profile := p.Source.(*chstore.ProfileView)
					return r.store.FollowedBy(p.Context, profile.PubKey, p.Args["viewerPubkey"].(string))
				},
			},
			"updatedAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime), Resolve: func(p graphql.ResolveParams) (any, error) {
				return p.Source.(*chstore.ProfileView).UpdatedAt, nil
			}},
		},
	})

	eventType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Event",
		Fields: graphql.FieldsThunk(func() graphql.Fields {
			return graphql.Fields{
				"id": &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: func(p graphql.ResolveParams) (any, error) {
					return p.Source.(*chstore.EventView).ID, nil
				}},
				"pubkey": &graphql.Field{Type: graphql.String, Resolve: func(p graphql.ResolveParams) (any, error) {
					return nullableString(p.Source.(*chstore.EventView).PubKey), nil
				}},
				"kind": &graphql.Field{Type: graphql.Int, Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					if ev.PubKey == "" {
						return nil, nil
					}
					return ev.Kind, nil
				}},
				"createdAt": &graphql.Field{Type: graphql.DateTime, Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					if ev.PubKey == "" {
						return nil, nil
					}
					return ev.CreatedAt, nil
				}},
				"content": &graphql.Field{Type: graphql.String, Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					if ev.PubKey == "" {
						return nil, nil
					}
					return ev.Content, nil
				}},
				"tags": &graphql.Field{Type: graphql.NewList(graphql.NewList(graphql.String)), Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					if ev.PubKey == "" {
						return nil, nil
					}
					return ev.Tags, nil
				}},
				"sig": &graphql.Field{Type: graphql.String, Resolve: func(p graphql.ResolveParams) (any, error) {
					return nullableString(p.Source.(*chstore.EventView).Sig), nil
				}},
				"author": &graphql.Field{Type: profileType, Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					if ev.PubKey == "" {
						return nil, nil
					}
					return r.store.ProfileByPubKey(p.Context, ev.PubKey)
				}},
				"likes":         &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: r.eventCount(r.store.LikeCount)},
				"reposts":       &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: r.eventCount(r.store.RepostCount)},
				"commentCount":  &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: r.eventCount(r.store.CommentCount)},
				"zaps":          &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: zeroInt},
				"zapSats":       &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: zeroInt},
				"uniqueZappers": &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: zeroInt},
				"reactionsByContent": &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(reactionTallyType))), Args: firstArg(), Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					rows, err := r.store.ReactionTallies(p.Context, ev.ID, first(p))
					out := make([]map[string]any, 0, len(rows))
					for _, row := range rows {
						out = append(out, map[string]any{"content": row.Content, "count": int(row.Count)})
					}
					return out, err
				}},
				"likers": &graphql.Field{Type: graphql.NewNonNull(reactionConnectionType), Args: firstArg(), Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					edges, err := r.store.Likers(p.Context, ev.ID, first(p))
					return reactionConnection(p.Context, r.store, edges), err
				}},
				"reposters": &graphql.Field{Type: graphql.NewNonNull(repostConnectionType), Args: firstArg(), Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					edges, err := r.store.Reposters(p.Context, ev.ID, first(p))
					return repostConnection(p.Context, r.store, edges), err
				}},
				"zappers": &graphql.Field{Type: graphql.NewNonNull(zapConnectionType), Args: firstArg(), Resolve: emptyConnection},
				"thread": &graphql.Field{Type: graphql.NewNonNull(threadType), Resolve: func(p graphql.ResolveParams) (any, error) {
					ev := p.Source.(*chstore.EventView)
					return map[string]any{"root": ev}, nil
				}},
				"updatedAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime), Resolve: func(p graphql.ResolveParams) (any, error) {
					return p.Source.(*chstore.EventView).UpdatedAt, nil
				}},
			}
		}),
	})

	commentType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Comment",
		Fields: graphql.Fields{
			"id": &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: func(p graphql.ResolveParams) (any, error) {
				return p.Source.(chstore.CommentView).ID, nil
			}},
			"author": &graphql.Field{Type: graphql.NewNonNull(profileType), Resolve: func(p graphql.ResolveParams) (any, error) {
				return r.store.ProfileByPubKey(p.Context, p.Source.(chstore.CommentView).PubKey)
			}},
			"content": &graphql.Field{Type: graphql.NewNonNull(graphql.String), Resolve: func(p graphql.ResolveParams) (any, error) {
				return p.Source.(chstore.CommentView).Content, nil
			}},
			"createdAt": &graphql.Field{Type: graphql.NewNonNull(graphql.DateTime), Resolve: func(p graphql.ResolveParams) (any, error) {
				return p.Source.(chstore.CommentView).CreatedAt, nil
			}},
			"replyCount": &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: func(p graphql.ResolveParams) (any, error) {
				c := p.Source.(chstore.CommentView)
				n, err := r.store.CommentCount(p.Context, c.ID)
				return int(n), err
			}},
			"likes":         &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: r.commentCount(r.store.LikeCount)},
			"reposts":       &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: r.commentCount(r.store.RepostCount)},
			"zaps":          &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: zeroInt},
			"zapSats":       &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: zeroInt},
			"uniqueZappers": &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: zeroInt},
		},
	})

	threadType = graphql.NewObject(graphql.ObjectConfig{
		Name: "Thread",
		Fields: graphql.Fields{
			"root": &graphql.Field{Type: graphql.NewNonNull(eventType), Resolve: func(p graphql.ResolveParams) (any, error) {
				return p.Source.(map[string]any)["root"], nil
			}},
			"directReplies": &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: func(p graphql.ResolveParams) (any, error) {
				root := p.Source.(map[string]any)["root"].(*chstore.EventView)
				n, err := r.store.DirectReplyCount(p.Context, root.ID)
				return int(n), err
			}},
			"participants": &graphql.Field{Type: graphql.NewNonNull(graphql.Int), Resolve: func(p graphql.ResolveParams) (any, error) {
				root := p.Source.(map[string]any)["root"].(*chstore.EventView)
				n, err := r.store.ThreadParticipants(p.Context, root.ID)
				return int(n), err
			}},
			"comments": &graphql.Field{
				Type: graphql.NewNonNull(commentConnectionType),
				Args: mergeArgs(firstArg(), graphql.FieldConfigArgument{
					"sort": &graphql.ArgumentConfig{Type: graphql.String, DefaultValue: "NEWEST"},
				}),
				Resolve: func(p graphql.ResolveParams) (any, error) {
					root := p.Source.(map[string]any)["root"].(*chstore.EventView)
					rows, err := r.store.Comments(p.Context, root.ID, first(p), strings.ToUpper(fmt.Sprint(p.Args["sort"])) != "OLDEST")
					edges := make([]map[string]any, 0, len(rows))
					for _, row := range rows {
						edges = append(edges, map[string]any{"node": row, "cursor": cursor(row.CreatedAt, row.ID)})
					}
					return map[string]any{"edges": edges, "pageInfo": pageInfo(edges), "totalCount": len(edges)}, err
				},
			},
		},
	})

	jsonType := jsonScalar("JSON")
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
			"dataset": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.String)},
			"groupBy": &graphql.InputObjectFieldConfig{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(graphql.String)))},
			"metrics": &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.String))},
			"kinds":   &graphql.InputObjectFieldConfig{Type: graphql.NewList(graphql.NewNonNull(graphql.Int))},
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
			"profile": &graphql.Field{
				Type: profileType,
				Args: graphql.FieldConfigArgument{"pubkey": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					pubkey := p.Args["pubkey"].(string)
					if err := validateHex64(pubkey); err != nil {
						return nil, err
					}
					return r.store.ProfileByPubKey(p.Context, pubkey)
				},
			},
			"thread": &graphql.Field{
				Type: threadType,
				Args: graphql.FieldConfigArgument{"rootEventId": &graphql.ArgumentConfig{Type: graphql.NewNonNull(graphql.String)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					id := p.Args["rootEventId"].(string)
					if err := validateHex64(id); err != nil {
						return nil, err
					}
					ev, err := r.store.EventByID(p.Context, id)
					if err != nil {
						return nil, err
					}
					return map[string]any{"root": ev}, nil
				},
			},
			"aggregateEvents": &graphql.Field{
				Type: graphql.NewNonNull(aggregationResultType),
				Args: graphql.FieldConfigArgument{"input": &graphql.ArgumentConfig{Type: graphql.NewNonNull(aggregationInputType)}},
				Resolve: func(p graphql.ResolveParams) (any, error) {
					input := parseAggregateInput(p.Args["input"].(map[string]any))
					rows, err := r.store.AggregateEvents(p.Context, input)
					return map[string]any{"rows": rows}, err
				},
			},
		},
	})

	return graphql.NewSchema(graphql.SchemaConfig{Query: queryType})
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

func (r *resolver) eventCount(fn func(context.Context, string) (uint64, error)) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		ev := p.Source.(*chstore.EventView)
		n, err := fn(p.Context, ev.ID)
		return int(n), err
	}
}

func (r *resolver) commentCount(fn func(context.Context, string) (uint64, error)) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		comment := p.Source.(chstore.CommentView)
		n, err := fn(p.Context, comment.ID)
		return int(n), err
	}
}

func (r *resolver) profileCount(fn func(context.Context, string) (uint64, error)) graphql.FieldResolveFn {
	return func(p graphql.ResolveParams) (any, error) {
		profile := p.Source.(*chstore.ProfileView)
		n, err := fn(p.Context, profile.PubKey)
		return int(n), err
	}
}

func firstArg() graphql.FieldConfigArgument {
	return graphql.FieldConfigArgument{"first": &graphql.ArgumentConfig{Type: graphql.Int, DefaultValue: 50}}
}

func first(p graphql.ResolveParams) uint64 {
	n, _ := p.Args["first"].(int)
	if n <= 0 {
		return 50
	}
	if n > 100 {
		return 100
	}
	return uint64(n)
}

func reactionConnection(ctx context.Context, store Store, rows []chstore.ActorEdge) map[string]any {
	edges := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		edges = append(edges, map[string]any{
			"node":      mustProfile(ctx, store, row.PubKey),
			"content":   row.Content,
			"reactedAt": row.CreatedAt,
			"cursor":    cursor(row.CreatedAt, row.PubKey),
		})
	}
	return map[string]any{"edges": edges, "pageInfo": pageInfo(edges), "totalCount": len(edges)}
}

func repostConnection(ctx context.Context, store Store, rows []chstore.ActorEdge) map[string]any {
	edges := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		edges = append(edges, map[string]any{
			"node":       mustProfile(ctx, store, row.PubKey),
			"repostedAt": row.CreatedAt,
			"cursor":     cursor(row.CreatedAt, row.PubKey),
		})
	}
	return map[string]any{"edges": edges, "pageInfo": pageInfo(edges), "totalCount": len(edges)}
}

func profileConnection(ctx context.Context, store Store, rows []chstore.ActorEdge) map[string]any {
	edges := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		edges = append(edges, map[string]any{
			"node":       mustProfile(ctx, store, row.PubKey),
			"followedAt": row.CreatedAt,
			"cursor":     cursor(row.CreatedAt, row.PubKey),
		})
	}
	return map[string]any{"edges": edges, "pageInfo": pageInfo(edges), "totalCount": len(edges)}
}

func mustProfile(ctx context.Context, store Store, pubkey string) *chstore.ProfileView {
	profile, err := store.ProfileByPubKey(ctx, pubkey)
	if err != nil {
		return &chstore.ProfileView{PubKey: pubkey, UpdatedAt: time.Now().UTC()}
	}
	return profile
}

func emptyConnection(graphql.ResolveParams) (any, error) {
	return map[string]any{"edges": []map[string]any{}, "pageInfo": pageInfo(nil), "totalCount": 0}, nil
}

func zeroInt(graphql.ResolveParams) (any, error) {
	return 0, nil
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func pageInfo(edges []map[string]any) map[string]any {
	var end any
	if len(edges) > 0 {
		end = edges[len(edges)-1]["cursor"]
	}
	return map[string]any{"hasNextPage": false, "endCursor": end}
}

func cursor(t time.Time, id string) string {
	return base64.StdEncoding.EncodeToString([]byte(t.UTC().Format(time.RFC3339Nano) + "|" + id))
}

func validateHex64(value string) error {
	if !hex64Pattern.MatchString(value) {
		return fmt.Errorf("expected lowercase 64-char hex")
	}
	return nil
}

func parseAggregateInput(raw map[string]any) chstore.AggregateInput {
	input := chstore.AggregateInput{Dataset: fmt.Sprint(raw["dataset"]), Limit: 100}
	if limit, ok := raw["limit"].(int); ok && limit > 0 {
		input.Limit = uint64(limit)
	}
	input.GroupBy = stringList(raw["groupBy"])
	input.Metrics = stringList(raw["metrics"])
	for _, v := range anyList(raw["kinds"]) {
		if n, ok := v.(int); ok {
			input.Kinds = append(input.Kinds, n)
		}
	}
	return input
}

func stringList(v any) []string {
	values := anyList(v)
	out := make([]string, 0, len(values))
	for _, value := range values {
		out = append(out, fmt.Sprint(value))
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

func connectionType(name string, edgeType *graphql.Object, pageInfoType *graphql.Object) *graphql.Object {
	return graphql.NewObject(graphql.ObjectConfig{
		Name: name,
		Fields: graphql.Fields{
			"edges":      &graphql.Field{Type: graphql.NewNonNull(graphql.NewList(graphql.NewNonNull(edgeType)))},
			"pageInfo":   &graphql.Field{Type: graphql.NewNonNull(pageInfoType)},
			"totalCount": &graphql.Field{Type: graphql.NewNonNull(graphql.Int)},
		},
	})
}

func mergeArgs(a, b graphql.FieldConfigArgument) graphql.FieldConfigArgument {
	out := graphql.FieldConfigArgument{}
	for k, v := range a {
		out[k] = v
	}
	for k, v := range b {
		out[k] = v
	}
	return out
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
