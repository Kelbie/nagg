package graphqlapi

import (
	"context"
	"encoding/json"
	"testing"
)

func TestRankedFeedEffectiveLimit(t *testing.T) {
	for _, tc := range []struct {
		name  string
		value string
		want  uint64
	}{
		{"omitted", "", 30},
		{"explicit", `,"limit":2`, 2},
		{"maximum", `,"limit":100`, 100},
		{"zero", `,"limit":0`, 30},
		{"negative", `,"limit":-1`, 30},
		{"oversized", `,"limit":101`, 30},
		{"fractional", `,"limit":2.9`, 2},
		{"wrong_type", `,"limit":"2"`, 30},
		{"nested_limit", `,"target":{"limit":1}`, 30},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var input map[string]any
			if err := json.Unmarshal([]byte(`{"references":{"kinds":[7]},"via":{"key":"e"}`+tc.value+`}`), &input); err != nil {
				t.Fatal(err)
			}
			store := &fakeStore{}
			_, limit, err := NewRanker(store).RankedEventViews(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			if limit != tc.want {
				t.Fatalf("effective limit = %d, want %d", limit, tc.want)
			}
			if len(store.aggregateInputs) != 1 || store.aggregateInputs[0].Limit != limit {
				t.Fatalf("ranking query limit does not match reported limit %d: %+v", limit, store.aggregateInputs)
			}
		})
	}
}
