package graphqlapi

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/graphql-go/graphql"
	chstore "github.com/vertex-lab/nagg/internal/clickhouse"
)

const testPubkey = "50d94fc2d8580c682b071a542f8b1e31a200b0508bab95a33bef0855df281d63"

func testHex(ch string) string {
	return strings.Repeat(ch, 64)
}

type fakeStore struct {
	events          [][]chstore.EventView
	eventByID       map[string]chstore.EventView
	latestEvents    map[string][]chstore.EventView
	aggregateRows   [][]chstore.AggregateRow
	calls           int
	aggregateCalls  int
	eventInputs     []chstore.EventQueryInput
	referenceInputs []referencingEventsInput
	latestInputs    []latestEventsInput
	aggregateInputs []chstore.AggregateInput
}

type latestEventsInput struct {
	pubkeys []string
	kinds   []int
	limit   uint64
}

type referencingEventsInput struct {
	input          chstore.EventQueryInput
	tag            chstore.TagFilter
	targets        []string
	limitPerTarget uint64
}

func (s *fakeStore) EventByID(_ context.Context, id string) (*chstore.EventView, error) {
	if s.eventByID != nil {
		if event, ok := s.eventByID[id]; ok {
			return &event, nil
		}
	}
	return nil, nil
}

func (s *fakeStore) QueryEvents(_ context.Context, input chstore.EventQueryInput) ([]chstore.EventView, error) {
	s.eventInputs = append(s.eventInputs, input)
	if input.Empty {
		s.calls++
		return []chstore.EventView{}, nil
	}
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

func (s *fakeStore) QueryEventsByTagTargets(_ context.Context, input chstore.EventQueryInput, tag chstore.TagFilter, targets []string, limitPerTarget uint64) (map[string][]chstore.EventView, error) {
	s.referenceInputs = append(s.referenceInputs, referencingEventsInput{
		input:          input,
		tag:            tag,
		targets:        append([]string(nil), targets...),
		limitPerTarget: limitPerTarget,
	})
	out := make(map[string][]chstore.EventView, len(targets))
	targetSet := make(map[string]struct{}, len(targets))
	for _, target := range targets {
		out[target] = nil
		targetSet[target] = struct{}{}
	}
	if input.Empty {
		s.calls++
		return out, nil
	}
	if len(s.events) == 0 {
		s.calls++
		return out, nil
	}
	idx := s.calls
	if idx >= len(s.events) {
		idx = len(s.events) - 1
	}
	s.calls++
	for _, event := range s.events[idx] {
		if len(input.PubKeys) > 0 && !containsString(input.PubKeys, event.PubKey) {
			continue
		}
		for _, eventTag := range event.Tags {
			if len(eventTag) < 2 || eventTag[0] != tag.Key {
				continue
			}
			if _, ok := targetSet[eventTag[1]]; !ok {
				continue
			}
			out[eventTag[1]] = append(out[eventTag[1]], event)
		}
	}
	return out, nil
}

func (s *fakeStore) QueryLatestEventsByPubKeys(_ context.Context, pubkeys []string, kinds []int, limit uint64) (map[string][]chstore.EventView, error) {
	s.latestInputs = append(s.latestInputs, latestEventsInput{pubkeys: pubkeys, kinds: kinds, limit: limit})
	out := make(map[string][]chstore.EventView, len(pubkeys))
	for _, pubkey := range pubkeys {
		out[pubkey] = nil
		if s.latestEvents != nil {
			out[pubkey] = s.latestEvents[pubkey]
		}
	}
	return out, nil
}

func (s *fakeStore) AggregateEvents(_ context.Context, input chstore.AggregateInput) ([]chstore.AggregateRow, error) {
	s.aggregateInputs = append(s.aggregateInputs, input)
	if input.Empty {
		s.aggregateCalls++
		return []chstore.AggregateRow{}, nil
	}
	if len(s.aggregateRows) == 0 {
		s.aggregateCalls++
		return nil, nil
	}
	idx := s.aggregateCalls
	if idx >= len(s.aggregateRows) {
		idx = len(s.aggregateRows) - 1
	}
	s.aggregateCalls++
	return s.aggregateRows[idx], nil
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

func containsString(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
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

func TestEventSelectedReferencesPrefersMarkedRootAndBatches(t *testing.T) {
	rootID := testHex("a")
	parentID := testHex("b")
	replyID := testHex("c")
	secondReplyID := testHex("d")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{
				{
					ID:        replyID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "reply",
					Tags:      [][]string{{"e", parentID, "", "reply"}, {"e", rootID, "", "root"}},
					Sig:       strings.Repeat("e", 128),
				},
				{
					ID:        secondReplyID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "reply two",
					Tags:      [][]string{{"e", rootID, "", "root"}},
					Sig:       strings.Repeat("f", 128),
				},
			},
			{{
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("a", 128),
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
			events(input:{kinds:[1], limit:2}) {
				nodes {
					id
					selectedReferences(input:{
						selectors:[{key:"e", marker:"root"}]
						fallback:{key:"e", excludeMarkers:["mention"]}
						maxDepth:8
						limit:1
					}) {
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
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v", nodes)
	}
	for _, node := range nodes {
		refs := node.(map[string]any)["selectedReferences"].(map[string]any)["nodes"].([]any)
		if len(refs) != 1 || refs[0].(map[string]any)["id"] != rootID {
			t.Fatalf("refs = %+v", refs)
		}
	}
	if len(store.eventInputs) != 2 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
	if got := store.eventInputs[1].IDs; len(got) != 1 || got[0] != rootID {
		t.Fatalf("selected reference fetch ids = %+v", got)
	}
}

func TestEventSelectedReferencesWalksFallbackChain(t *testing.T) {
	rootID := testHex("a")
	parentID := testHex("b")
	replyID := testHex("c")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        replyID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "reply",
				Tags:      [][]string{{"e", parentID, "", "reply"}},
				Sig:       strings.Repeat("e", 128),
			}},
			{{
				ID:        parentID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "parent",
				Tags:      [][]string{{"e", rootID, "", "root"}},
				Sig:       strings.Repeat("f", 128),
			}},
			{{
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("a", 128),
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
			events(input:{ids:["` + replyID + `"], limit:1}) {
				nodes {
					id
					selectedReferences(input:{
						selectors:[{key:"e", marker:"root"}]
						fallback:{key:"e", excludeMarkers:["mention"]}
						maxDepth:8
						limit:1
					}) {
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
	refs := nodes[0].(map[string]any)["selectedReferences"].(map[string]any)["nodes"].([]any)
	if len(refs) != 1 || refs[0].(map[string]any)["id"] != rootID {
		t.Fatalf("refs = %+v", refs)
	}
	if len(store.eventInputs) != 3 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
	}
}

func TestEventSelectedReferencesIgnoresMentionFallback(t *testing.T) {
	mentionID := testHex("a")
	eventID := testHex("b")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{{
				ID:        eventID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "root with mention",
				Tags:      [][]string{{"e", mentionID, "", "mention"}},
				Sig:       strings.Repeat("e", 128),
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
			events(input:{ids:["` + eventID + `"], limit:1}) {
				nodes {
					id
					selectedReferences(input:{
						selectors:[{key:"e", marker:"root"}]
						fallback:{key:"e", excludeMarkers:["mention"]}
						maxDepth:8
						limit:1
					}) {
						nodes { id }
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
	refs := nodes[0].(map[string]any)["selectedReferences"].(map[string]any)["nodes"].([]any)
	if len(refs) != 0 {
		t.Fatalf("refs = %+v", refs)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs = %+v", store.eventInputs)
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

func TestAggregateEventsAcceptsTimeBounds(t *testing.T) {
	store := &fakeStore{
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"},
				Metrics:    map[string]uint64{"unique_pubkeys": 2},
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			aggregateEvents(input:{
				dataset:"TAGS"
				kinds:[7]
				tags:[{key:"e"}]
				since:1710000000
				until:1710086400
				groupBy:["TAG_VALUE"]
				metrics:["UNIQUE_PUBKEYS"]
				limit:5
			}) {
				rows { dimensions metrics }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	input := store.aggregateInputs[0]
	if input.Since != 1_710_000_000 || input.Until != 1_710_086_400 {
		t.Fatalf("time bounds = %d/%d", input.Since, input.Until)
	}
}

func TestRankedEventsUsesRecentReferencesAndPreservesRank(t *testing.T) {
	topID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	secondID := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": topID},
				Metrics:    map[string]uint64{"unique_pubkeys": 3},
			},
			{
				Dimensions: map[string]string{"tag_value": secondID},
				Metrics:    map[string]uint64{"unique_pubkeys": 2},
			},
		}},
		events: [][]chstore.EventView{{
			{
				ID:        secondID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "second",
				Tags:      [][]string{},
				Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
			},
			{
				ID:        topID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "top",
				Tags:      [][]string{},
				Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			rankedEvents(input:{
				references:{kinds:[7], since:1710000000}
				via:{key:"e"}
				target:{kinds:[1]}
				metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
				limit:2
			}) {
				nodes { id content }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	aggregateInput := store.aggregateInputs[0]
	if aggregateInput.Dataset != "TAGS" || aggregateInput.Since != 1_710_000_000 {
		t.Fatalf("aggregate input = %+v", aggregateInput)
	}
	if len(aggregateInput.Tags) != 1 || aggregateInput.Tags[0].Key != "e" {
		t.Fatalf("aggregate tags = %+v", aggregateInput.Tags)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	if got := store.eventInputs[0].IDs; len(got) != 2 || got[0] != topID || got[1] != secondID {
		t.Fatalf("target ids = %+v", got)
	}
	data := result.Data.(map[string]any)
	nodes := data["rankedEvents"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != topID || nodes[1].(map[string]any)["id"] != secondID {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestEventsQueryResolvesPubkeysFromLatestEventTags(t *testing.T) {
	authorA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	authorB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", authorB}, {"p", authorA}, {"t", "not-a-pubkey"}},
			}},
		},
		events: [][]chstore.EventView{{
			{
				ID:        "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				PubKey:    authorA,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "from derived pubkey",
				Tags:      [][]string{},
				Sig:       "eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee",
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeysFrom:[{
					latestEventTags:{
						pubkey:"` + testPubkey + `"
						kinds:[3]
						tag:{key:"p"}
						limit:1
						maxValues:10
					}
				}]
				kinds:[1]
				limit:20
			}) { nodes { id pubkey content } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.latestInputs) != 1 {
		t.Fatalf("latest inputs len = %d", len(store.latestInputs))
	}
	if got := store.latestInputs[0]; len(got.pubkeys) != 1 || got.pubkeys[0] != testPubkey || got.kinds[0] != 3 || got.limit != 1 {
		t.Fatalf("latest input = %+v", got)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	if got := store.eventInputs[0].PubKeys; len(got) != 2 || got[0] != authorA || got[1] != authorB {
		t.Fatalf("derived pubkeys = %+v", got)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["pubkey"] != authorA {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestEventsQueryWithEmptyPubkeySourceDoesNotBroaden(t *testing.T) {
	store := &fakeStore{}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{
				pubkeysFrom:[{
					latestEventTags:{
						pubkey:"` + testPubkey + `"
						kinds:[3]
						tag:{key:"p"}
					}
				}]
				kinds:[1]
				limit:20
			}) { nodes { id } }
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 0 {
		t.Fatalf("empty pubkey source should not query events, got %+v", store.eventInputs)
	}
	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 0 {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestRankedReferencedByRanksCandidateEventsByGenericReferences(t *testing.T) {
	rootID := "1111111111111111111111111111111111111111111111111111111111111111"
	replyAID := "2222222222222222222222222222222222222222222222222222222222222222"
	replyBID := "3333333333333333333333333333333333333333333333333333333333333333"
	authorA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	authorB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       "44444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444444",
			},
		},
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "5555555555555555555555555555555555555555555555555555555555555555",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", authorA}, {"p", authorB}},
			}},
		},
		events: [][]chstore.EventView{
			{
				{
					ID:        replyBID,
					PubKey:    authorB,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_003, 0),
					Content:   "less ranked candidate",
					Tags:      [][]string{{"e", rootID}},
					Sig:       "66666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666666",
				},
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "top ranked candidate",
					Tags:      [][]string{{"e", rootID}},
					Sig:       "77777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777",
				},
			},
			{
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "top ranked candidate",
					Tags:      [][]string{{"e", rootID}},
					Sig:       "77777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777777",
				},
			},
		},
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": replyAID},
				Metrics:    map[string]uint64{"unique_pubkeys": 7},
			},
			{
				Dimensions: map[string]string{"tag_value": replyBID},
				Metrics:    map[string]uint64{"unique_pubkeys": 2},
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{
						kinds:[1]
						pubkeysFrom:[{
							latestEventTags:{
								pubkey:"` + testPubkey + `"
								kinds:[3]
								tag:{key:"p"}
							}
						}]
						limit:10
					}
					rank:{
						references:{kinds:[7]}
						via:{key:"e"}
						metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
					}
					limit:1
				}) { nodes { id content } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs len = %d", len(store.referenceInputs))
	}
	candidateInput := store.referenceInputs[0].input
	if got := candidateInput.PubKeys; len(got) != 2 || got[0] != authorA || got[1] != authorB {
		t.Fatalf("candidate pubkeys = %+v", got)
	}
	if got := store.referenceInputs[0].tag.Key; got != "e" {
		t.Fatalf("candidate tag key = %q", got)
	}
	if got := store.referenceInputs[0].targets; len(got) != 1 || got[0] != rootID {
		t.Fatalf("candidate targets = %+v", got)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	aggregateInput := store.aggregateInputs[0]
	if aggregateInput.Dataset != "TAGS" || len(aggregateInput.Tags) != 1 || aggregateInput.Tags[0].Key != "e" {
		t.Fatalf("aggregate input = %+v", aggregateInput)
	}
	if got := aggregateInput.Tags[0].Values; len(got) != 2 || !containsString(got, replyAID) || !containsString(got, replyBID) {
		t.Fatalf("aggregate tag values = %+v", got)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestRankedReferencedByBatchesAcrossSiblingEvents(t *testing.T) {
	rootAID := "1111111111111111111111111111111111111111111111111111111111111111"
	rootBID := "2222222222222222222222222222222222222222222222222222222222222222"
	replyAID := "3333333333333333333333333333333333333333333333333333333333333333"
	replyBID := "4444444444444444444444444444444444444444444444444444444444444444"
	authorA := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	authorB := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "5555555555555555555555555555555555555555555555555555555555555555",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", authorA}, {"p", authorB}},
			}},
		},
		events: [][]chstore.EventView{
			{
				{
					ID:        rootAID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_010, 0),
					Content:   "root a",
					Tags:      [][]string{},
					Sig:       "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
				},
				{
					ID:        rootBID,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_009, 0),
					Content:   "root b",
					Tags:      [][]string{},
					Sig:       "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
				},
			},
			{
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_008, 0),
					Content:   "reply a",
					Tags:      [][]string{{"e", rootAID}},
					Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
				{
					ID:        replyBID,
					PubKey:    authorB,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_007, 0),
					Content:   "reply b",
					Tags:      [][]string{{"e", rootBID}},
					Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				},
			},
			{
				{
					ID:        replyAID,
					PubKey:    authorA,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_008, 0),
					Content:   "reply a",
					Tags:      [][]string{{"e", rootAID}},
					Sig:       "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				},
				{
					ID:        replyBID,
					PubKey:    authorB,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_007, 0),
					Content:   "reply b",
					Tags:      [][]string{{"e", rootBID}},
					Sig:       "dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd",
				},
			},
		},
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": replyAID},
				Metrics:    map[string]uint64{"unique_pubkeys": 10},
			},
			{
				Dimensions: map[string]string{"tag_value": replyBID},
				Metrics:    map[string]uint64{"unique_pubkeys": 9},
			},
		}},
	}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			events(input:{ids:["` + rootAID + `","` + rootBID + `"], limit:2}) {
				nodes {
					id
					rankedReferencedBy(input:{
						via:{key:"e"}
						events:{
							kinds:[1]
							pubkeysFrom:[{
								latestEventTags:{
									pubkey:"` + testPubkey + `"
									kinds:[3]
									tag:{key:"p"}
								}
							}]
							limit:10
						}
						rank:{
							references:{kinds:[7]}
							via:{key:"e"}
							metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
						}
						limit:1
					}) { nodes { id } }
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.latestInputs) != 1 {
		t.Fatalf("latest inputs len = %d", len(store.latestInputs))
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs len = %d", len(store.referenceInputs))
	}
	if got := store.referenceInputs[0].targets; len(got) != 2 || got[0] != rootAID || got[1] != rootBID {
		t.Fatalf("reference targets = %+v", got)
	}
	if store.referenceInputs[0].limitPerTarget != 10 {
		t.Fatalf("reference limit per target = %d", store.referenceInputs[0].limitPerTarget)
	}
	if len(store.aggregateInputs) != 1 {
		t.Fatalf("aggregate inputs len = %d", len(store.aggregateInputs))
	}
	if len(store.eventInputs) != 2 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}

	data := result.Data.(map[string]any)
	nodes := data["events"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes len = %d", len(nodes))
	}
	replyA := nodes[0].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	replyB := nodes[1].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(replyA) != 1 || replyA[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("replyA = %+v", replyA)
	}
	if len(replyB) != 1 || replyB[0].(map[string]any)["id"] != replyBID {
		t.Fatalf("replyB = %+v", replyB)
	}
}
