package graphqlapi

import (
	"context"
	"net/http"
	"net/http/httptest"
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
	events                      [][]chstore.EventView
	eventByID                   map[string]chstore.EventView
	latestEvents                map[string][]chstore.EventView
	aggregateRows               [][]chstore.AggregateRow
	calls                       int
	aggregateCalls              int
	referenceAggregateSupported bool
	referenceAggregateRows      map[string][]chstore.AggregateRow
	eventInputs                 []chstore.EventQueryInput
	referenceInputs             []referencingEventsInput
	referenceAggregateInputs    []chstore.ReferenceAggregateInput
	latestInputs                []latestEventsInput
	rankedTargetAggregateInputs []rankedTargetAggregateInput
	aggregateInputs             []chstore.AggregateInput
	pubkeyScoreRows             map[string]chstore.PubkeyScore
	pubkeyScoreInputs           []pubkeyScoreInput
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

type rankedTargetAggregateInput struct {
	references chstore.AggregateInput
	target     chstore.EventQueryInput
}

type pubkeyScoreInput struct {
	source  string
	pubkeys []string
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

func (s *fakeStore) AggregateEventReferencesToTargets(_ context.Context, references chstore.AggregateInput, target chstore.EventQueryInput) ([]chstore.AggregateRow, error) {
	s.rankedTargetAggregateInputs = append(s.rankedTargetAggregateInputs, rankedTargetAggregateInput{
		references: references,
		target:     target,
	})
	if references.Empty || target.Empty {
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

func (s *fakeStore) AggregateEventsByTagTargets(_ context.Context, input chstore.ReferenceAggregateInput) (map[string][]chstore.AggregateRow, bool, error) {
	s.referenceAggregateInputs = append(s.referenceAggregateInputs, input)
	if !s.referenceAggregateSupported {
		return nil, false, nil
	}
	out := make(map[string][]chstore.AggregateRow, len(input.Targets))
	for _, target := range input.Targets {
		out[target] = nil
		if s.referenceAggregateRows != nil {
			out[target] = s.referenceAggregateRows[target]
		}
	}
	return out, true, nil
}

func (s *fakeStore) PubkeyScores(_ context.Context, source string, pubkeys []string) (map[string]chstore.PubkeyScore, error) {
	s.pubkeyScoreInputs = append(s.pubkeyScoreInputs, pubkeyScoreInput{
		source:  source,
		pubkeys: append([]string(nil), pubkeys...),
	})
	out := make(map[string]chstore.PubkeyScore, len(pubkeys))
	for _, pubkey := range pubkeys {
		if s.pubkeyScoreRows == nil {
			continue
		}
		if row, ok := s.pubkeyScoreRows[pubkey]; ok {
			out[pubkey] = row
		}
	}
	return out, nil
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

func TestServiceInfoExposesCapabilities(t *testing.T) {
	store := &fakeStore{}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}

	result := graphql.Do(graphql.Params{
		Schema: schema,
		RequestString: `query {
			serviceInfo {
				graphqlSchemaVersion
				appViewVersion
				capabilities
				appViews { version routes }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	info := data["serviceInfo"].(map[string]any)
	if info["graphqlSchemaVersion"] == "" || info["appViewVersion"] != "v1" {
		t.Fatalf("serviceInfo = %+v", info)
	}
	caps := info["capabilities"].([]any)
	if len(caps) == 0 {
		t.Fatalf("capabilities empty: %+v", info)
	}
}

func TestHandlerWritesCapabilityHeaders(t *testing.T) {
	store := &fakeStore{}
	schema, err := NewSchema(store)
	if err != nil {
		t.Fatal(err)
	}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/graphql", strings.NewReader(`{"query":"{ serviceInfo { appViewVersion } }"}`))

	Handler(schema)(rec, req)

	if got := rec.Header().Get("X-Nagg-Capabilities"); !strings.Contains(got, "graphql.rank.pubkeyScoreTerms") {
		t.Fatalf("capability header = %q", got)
	}
	if got := rec.Header().Get("X-Nagg-App-View-Version"); got != "v1" {
		t.Fatalf("app view version header = %q", got)
	}
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

func TestEventAggregateReferencedByBatchesAcrossSiblingEvents(t *testing.T) {
	sourceID1 := testHex("a")
	sourceID2 := testHex("b")
	reactionID1 := testHex("c")
	reactionID2 := testHex("d")
	reactionID3 := testHex("e")
	reactionPubkey1 := testHex("1")
	reactionPubkey2 := testHex("2")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{
				{
					ID:        sourceID1,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_000, 0),
					Content:   "source one",
					Tags:      [][]string{},
					Sig:       strings.Repeat("a", 128),
				},
				{
					ID:        sourceID2,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "source two",
					Tags:      [][]string{},
					Sig:       strings.Repeat("b", 128),
				},
			},
			{
				{
					ID:        reactionID1,
					PubKey:    reactionPubkey1,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID1}},
					Sig:       strings.Repeat("c", 128),
				},
				{
					ID:        reactionID2,
					PubKey:    reactionPubkey1,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_003, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID1}},
					Sig:       strings.Repeat("d", 128),
				},
				{
					ID:        reactionID3,
					PubKey:    reactionPubkey2,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_004, 0),
					Content:   "+",
					Tags:      [][]string{{"e", sourceID2}},
					Sig:       strings.Repeat("e", 128),
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
			events(input:{kinds:[1], limit:2}) {
				nodes {
					id
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
	if len(nodes) != 2 {
		t.Fatalf("nodes = %+v", nodes)
	}
	gotByID := map[string]uint64{}
	for _, rawNode := range nodes {
		node := rawNode.(map[string]any)
		rows := node["aggregateReferencedBy"].(map[string]any)["rows"].([]any)
		if len(rows) != 1 {
			t.Fatalf("rows for %s = %+v", node["id"], rows)
		}
		metrics := rows[0].(map[string]any)["metrics"].(map[string]uint64)
		gotByID[node["id"].(string)] = metrics["pubkeys"]
	}
	if gotByID[sourceID1] != 1 || gotByID[sourceID2] != 1 {
		t.Fatalf("metrics = %+v", gotByID)
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs = %+v", store.referenceInputs)
	}
	if got := store.referenceInputs[0].targets; len(got) != 2 || got[0] != sourceID1 || got[1] != sourceID2 {
		t.Fatalf("targets = %+v", got)
	}
}

func TestSelectedReferencesShareNestedRelationBatches(t *testing.T) {
	replyID1 := testHex("a")
	replyID2 := testHex("b")
	rootID1 := testHex("c")
	rootID2 := testHex("d")
	reactionID1 := testHex("e")
	reactionID2 := testHex("f")
	reactionPubkey := testHex("1")
	store := &fakeStore{
		events: [][]chstore.EventView{
			{
				{
					ID:        replyID1,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_002, 0),
					Content:   "reply one",
					Tags:      [][]string{{"e", rootID1}},
					Sig:       strings.Repeat("a", 128),
				},
				{
					ID:        replyID2,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_001, 0),
					Content:   "reply two",
					Tags:      [][]string{{"e", rootID2}},
					Sig:       strings.Repeat("b", 128),
				},
			},
			{
				{
					ID:        rootID1,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_000, 0),
					Content:   "root one",
					Tags:      [][]string{},
					Sig:       strings.Repeat("c", 128),
				},
				{
					ID:        rootID2,
					PubKey:    testPubkey,
					Kind:      1,
					CreatedAt: time.Unix(1_710_000_000, 0),
					Content:   "root two",
					Tags:      [][]string{},
					Sig:       strings.Repeat("d", 128),
				},
			},
			{
				{
					ID:        reactionID1,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_003, 0),
					Content:   "+",
					Tags:      [][]string{{"e", rootID1}},
					Sig:       strings.Repeat("e", 128),
				},
				{
					ID:        reactionID2,
					PubKey:    reactionPubkey,
					Kind:      7,
					CreatedAt: time.Unix(1_710_000_004, 0),
					Content:   "+",
					Tags:      [][]string{{"e", rootID2}},
					Sig:       strings.Repeat("f", 128),
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
			events(input:{ids:["` + replyID1 + `","` + replyID2 + `"], limit:2}) {
				nodes {
					id
					selectedReferences(input:{fallback:{key:"e"}, limit:1}) {
						nodes {
							id
							aggregateReferencedBy(input:{
								via:{key:"e"}
								events:{kinds:[7], limit:20}
								metrics:[{name:"pubkeys", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}]
							}) {
								rows { metrics }
							}
						}
					}
				}
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.referenceInputs) != 1 {
		t.Fatalf("reference inputs = %+v", store.referenceInputs)
	}
	if got := store.referenceInputs[0].targets; len(got) != 2 || got[0] != rootID1 || got[1] != rootID2 {
		t.Fatalf("targets = %+v", got)
	}
}

func TestEventAggregateReferencedByUsesAggregateStoreFastPath(t *testing.T) {
	sourceID1 := testHex("a")
	sourceID2 := testHex("b")
	store := &fakeStore{
		referenceAggregateSupported: true,
		referenceAggregateRows: map[string][]chstore.AggregateRow{
			sourceID1: {{
				Dimensions: map[string]string{},
				Metrics:    map[string]uint64{"pubkeys": 2},
			}},
			sourceID2: {{
				Dimensions: map[string]string{},
				Metrics:    map[string]uint64{"pubkeys": 3},
			}},
		},
		events: [][]chstore.EventView{{
			{
				ID:        sourceID1,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "source one",
				Tags:      [][]string{},
				Sig:       strings.Repeat("a", 128),
			},
			{
				ID:        sourceID2,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "source two",
				Tags:      [][]string{},
				Sig:       strings.Repeat("b", 128),
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
			events(input:{kinds:[1], limit:2}) {
				nodes {
					id
					aggregateReferencedBy(input:{
						via:{key:"e"}
						events:{kinds:[7], limit:500}
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
	gotByID := map[string]uint64{}
	for _, rawNode := range nodes {
		node := rawNode.(map[string]any)
		rows := node["aggregateReferencedBy"].(map[string]any)["rows"].([]any)
		metrics := rows[0].(map[string]any)["metrics"].(map[string]uint64)
		gotByID[node["id"].(string)] = metrics["pubkeys"]
	}
	if gotByID[sourceID1] != 2 || gotByID[sourceID2] != 3 {
		t.Fatalf("metrics = %+v", gotByID)
	}
	if len(store.referenceAggregateInputs) != 1 {
		t.Fatalf("reference aggregate inputs = %+v", store.referenceAggregateInputs)
	}
	if len(store.referenceInputs) != 0 {
		t.Fatalf("fallback reference inputs = %+v", store.referenceInputs)
	}
	input := store.referenceAggregateInputs[0]
	if input.Tag.Key != "e" || input.LimitPerTarget != 500 {
		t.Fatalf("aggregate input = %+v", input)
	}
	if got := input.Targets; len(got) != 2 || got[0] != sourceID1 || got[1] != sourceID2 {
		t.Fatalf("targets = %+v", got)
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
	if len(store.rankedTargetAggregateInputs) != 0 {
		t.Fatalf("unexpected ranked target aggregate inputs = %+v", store.rankedTargetAggregateInputs)
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
	if got := store.eventInputs[0].Kinds; len(got) != 1 || got[0] != 1 {
		t.Fatalf("target kinds = %+v", got)
	}
	data := result.Data.(map[string]any)
	nodes := data["rankedEvents"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != topID || nodes[1].(map[string]any)["id"] != secondID {
		t.Fatalf("nodes = %+v", nodes)
	}
}

func TestRankedEventsAggregatesInsideDerivedTargetFilters(t *testing.T) {
	topID := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	followedAuthor := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Tags:      [][]string{{"p", followedAuthor}},
			}},
		},
		aggregateRows: [][]chstore.AggregateRow{{
			{
				Dimensions: map[string]string{"tag_value": topID},
				Metrics:    map[string]uint64{"unique_pubkeys": 3},
			},
		}},
		events: [][]chstore.EventView{{
			{
				ID:        topID,
				PubKey:    followedAuthor,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "popular followed post",
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
				target:{
					kinds:[1]
					pubkeysFrom:[{
						latestEventTags:{
							pubkey:"` + testPubkey + `"
							kinds:[3]
							tag:{key:"p"}
							limit:1
							maxValues:10
						}
					}]
				}
				metric:{name:"likers", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
				limit:1
			}) {
				nodes { id pubkey content }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	if len(store.aggregateInputs) != 0 {
		t.Fatalf("unexpected global aggregate inputs = %+v", store.aggregateInputs)
	}
	if len(store.rankedTargetAggregateInputs) != 1 {
		t.Fatalf("ranked target aggregate inputs len = %d", len(store.rankedTargetAggregateInputs))
	}
	aggregateInput := store.rankedTargetAggregateInputs[0]
	if len(aggregateInput.references.Tags) != 1 || aggregateInput.references.Tags[0].Key != "e" {
		t.Fatalf("reference tags = %+v", aggregateInput.references.Tags)
	}
	if len(aggregateInput.target.PubKeys) != 1 || aggregateInput.target.PubKeys[0] != followedAuthor {
		t.Fatalf("target pubkeys = %+v", aggregateInput.target.PubKeys)
	}
	if len(aggregateInput.target.Kinds) != 1 || aggregateInput.target.Kinds[0] != 1 {
		t.Fatalf("target kinds = %+v", aggregateInput.target.Kinds)
	}
	if len(store.eventInputs) != 1 {
		t.Fatalf("event inputs len = %d", len(store.eventInputs))
	}
	eventInput := store.eventInputs[0]
	if len(eventInput.IDs) != 1 || eventInput.IDs[0] != topID {
		t.Fatalf("event ids = %+v", eventInput.IDs)
	}
	if len(eventInput.PubKeys) != 1 || eventInput.PubKeys[0] != followedAuthor {
		t.Fatalf("event pubkeys = %+v", eventInput.PubKeys)
	}
	data := result.Data.(map[string]any)
	nodes := data["rankedEvents"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != topID {
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

func TestReferencedByResolvesPubkeysFromSourceEventAuthor(t *testing.T) {
	rootID := "1111111111111111111111111111111111111111111111111111111111111111"
	replyID := "2222222222222222222222222222222222222222222222222222222222222222"
	rootAuthor := "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	otherAuthor := "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    rootAuthor,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       testHex("f") + testHex("f"),
			},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyID,
				PubKey:    rootAuthor,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_001, 0),
				Content:   "thread continuation",
				Tags:      [][]string{{"e", rootID}},
				Sig:       testHex("e") + testHex("e"),
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
				referencedBy(input:{
					via:{key:"e"}
					events:{
						kinds:[1]
						pubkeysFrom:[{sourceEventAuthor:true}]
						limit:20
					}
					limit:20
				}) { nodes { id pubkey } }
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
	eventInput := store.eventInputs[0]
	if len(eventInput.PubKeys) != 1 || eventInput.PubKeys[0] != rootAuthor {
		t.Fatalf("source author pubkeys = %+v", eventInput.PubKeys)
	}
	if len(eventInput.Tags) != 1 || eventInput.Tags[0].Key != "e" || eventInput.Tags[0].Value != rootID {
		t.Fatalf("reply target tag = %+v", eventInput.Tags)
	}
	if eventInput.Empty {
		t.Fatal("source author query should not remain empty after adding source pubkey")
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["referencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["pubkey"] != rootAuthor {
		t.Fatalf("nodes = %+v (other author %s)", nodes, otherAuthor)
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

func TestRankedReferencedByCapsLargeLimitInsteadOfDefaultingToOne(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	replyA := chstore.EventView{
		ID:        replyAID,
		PubKey:    testHex("a"),
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_002, 0),
		Content:   "reply a",
		Tags:      [][]string{{"e", rootID}},
		Sig:       strings.Repeat("a", 128),
	}
	replyB := chstore.EventView{
		ID:        replyBID,
		PubKey:    testHex("b"),
		Kind:      1,
		CreatedAt: time.Unix(1_710_000_003, 0),
		Content:   "reply b",
		Tags:      [][]string{{"e", rootID}},
		Sig:       strings.Repeat("b", 128),
	}
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		events: [][]chstore.EventView{
			{replyA, replyB},
			{replyB, replyA},
		},
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
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[7], limit:500}
						via:{key:"e"}
						metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
					}
					limit:100
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 {
		t.Fatalf("nodes len = %d, nodes = %+v", len(nodes), nodes)
	}
}

func TestRankedReferencedBySupportsWeightedTermsAndCandidateBoosts(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	authorA := testHex("a")
	authorB := testHex("b")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		latestEvents: map[string][]chstore.EventView{
			testPubkey: {{
				ID:        testHex("5"),
				PubKey:    testPubkey,
				Kind:      3,
				CreatedAt: time.Unix(1_710_000_004, 0),
				Tags:      [][]string{{"p", authorA}},
				Sig:       strings.Repeat("5", 128),
			}},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyBID,
				PubKey:    authorB,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_003, 0),
				Content:   "more likes",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("6", 128),
			},
			{
				ID:        replyAID,
				PubKey:    authorA,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "followed author",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("7", 128),
			},
		}},
		referenceAggregateSupported: true,
		referenceAggregateRows: map[string][]chstore.AggregateRow{
			replyAID: {{
				Metrics: map[string]uint64{"likes": 1},
			}},
			replyBID: {{
				Metrics: map[string]uint64{"likes": 3},
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
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[7], limit:500}
						via:{key:"e"}
						metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
						weight:3.0
						transform:"LOG1P"
						candidatePubkeyBoosts:[{
							pubkeysFrom:[{
								latestEventTags:{
									pubkey:"` + testPubkey + `"
									kinds:[3]
									tag:{key:"p"}
								}
							}]
							weight:6.0
						}]
					}
					limit:2
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != replyAID || nodes[1].(map[string]any)["id"] != replyBID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.latestInputs) != 1 {
		t.Fatalf("latest inputs = %+v", store.latestInputs)
	}
	if len(store.referenceAggregateInputs) != 1 {
		t.Fatalf("reference aggregate inputs = %+v", store.referenceAggregateInputs)
	}
	input := store.referenceAggregateInputs[0]
	if input.Tag.Key != "e" || input.OrderBy != "likes" || input.First != 1 {
		t.Fatalf("reference aggregate input = %+v", input)
	}
	if got := input.Targets; len(got) != 2 || !containsString(got, replyAID) || !containsString(got, replyBID) {
		t.Fatalf("targets = %+v", got)
	}
	if len(input.Metrics) != 1 || input.Metrics[0].Name != "likes" || input.Metrics[0].DistinctField != "PUBKEY" {
		t.Fatalf("metrics = %+v", input.Metrics)
	}
	if len(store.aggregateInputs) != 0 {
		t.Fatalf("legacy aggregate inputs = %+v", store.aggregateInputs)
	}
}

func TestRankedReferencedBySupportsPubkeyScoreTermWithFollowerThreshold(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	authorA := testHex("a")
	authorB := testHex("b")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyBID,
				PubKey:    authorB,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_003, 0),
				Content:   "high score but below threshold",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("6", 128),
			},
			{
				ID:        replyAID,
				PubKey:    authorA,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "eligible cached score",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("7", 128),
			},
		}},
		pubkeyScoreRows: map[string]chstore.PubkeyScore{
			authorA: {PubKey: authorA, Source: "vertex", Score: 20, Followers: 500},
			authorB: {PubKey: authorB, Source: "vertex", Score: 90, Followers: 499},
		},
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
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[7], limit:500}
						via:{key:"e"}
						metric:{name:"likes", op:"COUNT_DISTINCT", distinctField:"PUBKEY"}
						terms:[{
							pubkeyScore:{source:"vertex", target:"AUTHOR", minFollowers:500}
							weight:1.0
						}]
					}
					limit:2
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 2 || nodes[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.pubkeyScoreInputs) != 1 {
		t.Fatalf("pubkey score inputs = %+v", store.pubkeyScoreInputs)
	}
	scoreInput := store.pubkeyScoreInputs[0]
	if scoreInput.source != "vertex" || len(scoreInput.pubkeys) != 2 {
		t.Fatalf("score input = %+v", scoreInput)
	}
}

func TestRankedReferencedBySupportsWeightedSumMetricAndOffset(t *testing.T) {
	rootID := testHex("1")
	replyAID := testHex("2")
	replyBID := testHex("3")
	store := &fakeStore{
		eventByID: map[string]chstore.EventView{
			rootID: {
				ID:        rootID,
				PubKey:    testPubkey,
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_000, 0),
				Content:   "root",
				Tags:      [][]string{},
				Sig:       strings.Repeat("1", 128),
			},
		},
		events: [][]chstore.EventView{{
			{
				ID:        replyAID,
				PubKey:    testHex("a"),
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_002, 0),
				Content:   "low zaps",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("a", 128),
			},
			{
				ID:        replyBID,
				PubKey:    testHex("b"),
				Kind:      1,
				CreatedAt: time.Unix(1_710_000_003, 0),
				Content:   "high zaps",
				Tags:      [][]string{{"e", rootID}},
				Sig:       strings.Repeat("b", 128),
			},
		}},
		referenceAggregateSupported: true,
		referenceAggregateRows: map[string][]chstore.AggregateRow{
			replyAID: {{
				Metrics: map[string]uint64{"zapSats": 10},
			}},
			replyBID: {{
				Metrics: map[string]uint64{"zapSats": 100},
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
			event(id:"` + rootID + `") {
				rankedReferencedBy(input:{
					via:{key:"e"}
					events:{kinds:[1], limit:10}
					rank:{
						references:{kinds:[9735], limit:500}
						via:{key:"e"}
						metric:{name:"zapSats", op:"SUM", derived:"nip57.amount_sats"}
					}
					limit:1
					offset:1
				}) { nodes { id } }
			}
		}`,
		Context: context.Background(),
	})

	if len(result.Errors) > 0 {
		t.Fatalf("graphql errors = %+v", result.Errors)
	}
	data := result.Data.(map[string]any)
	nodes := data["event"].(map[string]any)["rankedReferencedBy"].(map[string]any)["nodes"].([]any)
	if len(nodes) != 1 || nodes[0].(map[string]any)["id"] != replyAID {
		t.Fatalf("nodes = %+v", nodes)
	}
	if len(store.referenceAggregateInputs) != 1 {
		t.Fatalf("reference aggregate inputs = %+v", store.referenceAggregateInputs)
	}
	metric := store.referenceAggregateInputs[0].Metrics[0]
	if metric.Name != "zapSats" || metric.Op != "SUM" || metric.Derived != "nip57.amount_sats" {
		t.Fatalf("metric = %+v", metric)
	}
	if len(store.aggregateInputs) != 0 {
		t.Fatalf("legacy aggregate inputs = %+v", store.aggregateInputs)
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
