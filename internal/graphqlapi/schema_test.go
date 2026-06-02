package graphqlapi

import (
	"context"
	"testing"
	"time"

	"github.com/graphql-go/graphql"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

const testPubkey = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"

type fakeStore struct {
	events [][]chstore.EventView
	calls  int
}

func (s *fakeStore) EventByID(context.Context, string) (*chstore.EventView, error) {
	return nil, nil
}

func (s *fakeStore) QueryEvents(context.Context, chstore.EventQueryInput) ([]chstore.EventView, error) {
	if len(s.events) == 0 {
		s.calls++
		return nil, nil
	}
	idx := s.calls
	if idx >= len(s.events) {
		idx = len(s.events) - 1
	}
	s.calls++
	return s.events[idx], nil
}

func (s *fakeStore) QueryLatestEventsByPubKeys(context.Context, []string, []int, uint64) (map[string][]chstore.EventView, error) {
	return map[string][]chstore.EventView{}, nil
}

func (s *fakeStore) AggregateEvents(context.Context, chstore.AggregateInput) ([]chstore.AggregateRow, error) {
	return nil, nil
}

func (s *fakeStore) ThreadEvents(context.Context, string, int) (*chstore.EventView, []chstore.EventView, error) {
	return nil, nil, nil
}

type fakeUserBackfiller struct {
	calls  int
	pubkey string
	limit  uint64
}

func (f *fakeUserBackfiller) BackfillUserFeed(_ context.Context, pubkey string, limit uint64) error {
	f.calls++
	f.pubkey = pubkey
	f.limit = limit
	return nil
}

type fakeHydratingUserBackfiller struct {
	fakeUserBackfiller
	completed bool
	hydrated  int
}

func (f *fakeHydratingUserBackfiller) HydrateUserFeed(ctx context.Context, pubkey string, limit uint64) (bool, error) {
	f.hydrated++
	return f.completed, f.BackfillUserFeed(ctx, pubkey, limit)
}

func TestEventsQueryBackfillsAuthorWhenFirstPageShort(t *testing.T) {
	store := &fakeStore{
		events: [][]chstore.EventView{
			nil,
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "hello",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeUserBackfiller{}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeys:["` + testPubkey + `"],
				kinds:[1,6,16],
				limit:20
			}) { nodes { id kind pubkey content } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.limit != 20 {
		t.Fatalf("backfill call = %+v", backfiller)
	}
	if store.calls != 2 {
		t.Fatalf("store calls = %d, want 2", store.calls)
	}
	data := result.Data.(map[string]any)
	events := data["events"].(map[string]any)
	nodes := events["nodes"].([]any)
	if len(nodes) != 1 {
		t.Fatalf("nodes len = %d, want 1", len(nodes))
	}
	node := nodes[0].(map[string]any)
	if node["content"] != "hello" {
		t.Fatalf("node = %+v", node)
	}
}

func TestEventsQueryReturnsIndexedDataWhenHydrationIsSlow(t *testing.T) {
	store := &fakeStore{
		events: [][]chstore.EventView{
			nil,
			{{
				ID:        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "eventually available",
				Tags:      [][]string{},
				Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
			}},
		},
	}
	backfiller := &fakeHydratingUserBackfiller{completed: false}
	schema, err := NewSchema(store, WithUserFeedBackfill(backfiller))
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeys:["` + testPubkey + `"],
				kinds:[1,6,16],
				limit:20
			}) { nodes { id kind pubkey content } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if backfiller.hydrated != 1 || backfiller.calls != 1 || backfiller.pubkey != testPubkey || backfiller.limit != 20 {
		t.Fatalf("hydration call = %+v", backfiller)
	}
	if store.calls != 1 {
		t.Fatalf("store calls = %d, want 1", store.calls)
	}
	data := result.Data.(map[string]any)
	events := data["events"].(map[string]any)
	nodes := events["nodes"].([]any)
	if len(nodes) != 0 {
		t.Fatalf("nodes len = %d, want 0", len(nodes))
	}
}

func TestEventReferencesResolveByGenericTagPredicate(t *testing.T) {
	sourceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	quoteID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        sourceID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "quoting",
				Tags:      [][]string{{"q", quoteID}},
				Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			}},
			{{
				ID:        quoteID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "quoted",
				Tags:      [][]string{},
				Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			}},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + sourceID + `"], limit:1}) {
				nodes {
					id
					references(input:{tags:[{key:"q"}], limit:1}) {
						nodes { id content }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	refs := nodes[0].(map[string]any)["references"].(map[string]any)["nodes"].([]any)
	if len(refs) != 1 || refs[0].(map[string]any)["id"] != quoteID {
		t.Fatalf("refs = %+v", refs)
	}
}

func TestEventAggregateReferencedByCountsDistinctGenericSources(t *testing.T) {
	sourceID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	reactionID1 := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	reactionID2 := "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	reactionPubkey := "1111111111111111111111111111111111111111111111111111111111111111"
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        sourceID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "target",
				Tags:      [][]string{},
				Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			}},
			{
				{
					ID:        reactionID1,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID}},
					Sig:       "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
				},
				{
					ID:        reactionID2,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID}},
					Sig:       "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
				},
			},
		},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + sourceID + `"], limit:1}) {
				nodes {
					aggregateReferencedBy(input:{
						via:{key:"e"}
						events:{kinds:[7], limit:20}
						metrics:[{name:"pubkeys", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]
					}) {
						rows { metrics }
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	rows := nodes[0].(map[string]any)["aggregateReferencedBy"].(map[string]any)["rows"].([]any)
	metrics := rows[0].(map[string]any)["metrics"].(map[string]uint64)
	if metrics["pubkeys"] != 1 {
		t.Fatalf("metrics = %+v", metrics)
	}
}
