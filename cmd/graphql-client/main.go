package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"
)

type graphQLResponse struct {
	Data   map[string]any   `json:"data"`
	Errors []map[string]any `json:"errors"`
}

func main() {
	endpoint := "http://127.0.0.1:8080/graphql"
	if v := os.Getenv("NAGG_GRAPHQL_ENDPOINT"); v != "" {
		endpoint = v
	}

	examples := []struct {
		name  string
		query string
	}{
		{
			name: "1. generic events per kind",
			query: `query {
				aggregateEvents(input: {
					dataset: "EVENTS"
					groupBy: ["KIND"]
					metrics: ["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"]
					limit: 8
				}) { rows { dimensions metrics } }
			}`,
		},
		{
			name: "2. generic tag-key distribution",
			query: `query {
				aggregateEvents(input: {
					dataset: "TAGS"
					groupBy: ["TAG_KEY"]
					metrics: ["COUNT", "UNIQUE_EVENTS"]
					limit: 8
				}) { rows { dimensions metrics } }
			}`,
		},
		{
			name: "3. old 'likes/reactions' use case as generic kind/tag/content aggregation",
			query: `query {
				aggregateEvents(input: {
					dataset: "TAGS"
					kinds: [7]
					tags: [{ key: "e" }]
					groupBy: ["TAG_VALUE", "CONTENT"]
					metrics: ["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"]
					limit: 5
				}) { rows { dimensions metrics } }
			}`,
		},
		{
			name: "4. old 'followers' use case as generic kind/tag aggregation",
			query: `query {
				aggregateEvents(input: {
					dataset: "TAGS"
					kinds: [3]
					tags: [{ key: "p" }]
					groupBy: ["TAG_VALUE"]
					metrics: ["UNIQUE_PUBKEYS", "COUNT"]
					limit: 5
				}) { rows { dimensions metrics } }
			}`,
		},
	}

	var reactionTarget string
	var replyTarget string
	for _, example := range examples {
		resp := mustPost(endpoint, example.query)
		fmt.Printf("\n# %s\n%s\n", example.name, pretty(resp))
		if example.name[0] == '3' {
			reactionTarget = firstDimension(resp, "tag_value")
		}
	}

	replyTargetResp := mustPost(endpoint, `query {
		aggregateEvents(input: {
			dataset: "TAGS"
			kinds: [1, 1111]
			tags: [{ key: "e" }]
			groupBy: ["TAG_VALUE"]
			metrics: ["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"]
			limit: 1
		}) { rows { dimensions metrics } }
	}`)
	replyTarget = firstDimension(replyTargetResp, "tag_value")
	fmt.Printf("\n# 5. old 'comments/thread' use case as generic reply-target discovery\n%s\n", pretty(replyTargetResp))

	if reactionTarget != "" {
		query := fmt.Sprintf(`query {
			events(input: {
				kinds: [7]
				tags: [{ key: "e", value: "%s" }]
				limit: 5
			}) {
				nodes { id pubkey kind createdAt content tags }
			}
		}`, reactionTarget)
		fmt.Printf("\n# 6. events matching the discovered reaction target %s\n%s\n", reactionTarget, pretty(mustPost(endpoint, query)))
	}

	if replyTarget != "" {
		query := fmt.Sprintf(`query {
			events(input: {
				kinds: [1, 1111]
				tags: [{ key: "e", value: "%s" }]
				limit: 5
			}) {
				nodes { id pubkey kind createdAt content tags }
			}
		}`, replyTarget)
		fmt.Printf("\n# 7. events matching the discovered reply/comment target %s\n%s\n", replyTarget, pretty(mustPost(endpoint, query)))
	}
}

func mustPost(endpoint, query string) graphQLResponse {
	resp, err := post(endpoint, query)
	if err != nil {
		panic(err)
	}
	if len(resp.Errors) > 0 {
		panic(pretty(resp.Errors))
	}
	return resp
}

func post(endpoint, query string) (graphQLResponse, error) {
	body, _ := json.Marshal(map[string]any{"query": query})
	client := http.Client{Timeout: 15 * time.Second}
	res, err := client.Post(endpoint, "application/json", bytes.NewReader(body))
	if err != nil {
		return graphQLResponse{}, err
	}
	defer res.Body.Close()
	raw, err := io.ReadAll(res.Body)
	if err != nil {
		return graphQLResponse{}, err
	}
	if res.StatusCode >= 300 {
		return graphQLResponse{}, fmt.Errorf("%s: %s", res.Status, string(raw))
	}
	var out graphQLResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return graphQLResponse{}, err
	}
	return out, nil
}

func pretty(v any) string {
	raw, _ := json.MarshalIndent(v, "", "  ")
	return string(raw)
}

func firstDimension(resp graphQLResponse, key string) string {
	agg, _ := resp.Data["aggregateEvents"].(map[string]any)
	rows, _ := agg["rows"].([]any)
	for _, row := range rows {
		r, _ := row.(map[string]any)
		dims, _ := r["dimensions"].(map[string]any)
		if v, _ := dims[key].(string); v != "" {
			return v
		}
	}
	return ""
}
