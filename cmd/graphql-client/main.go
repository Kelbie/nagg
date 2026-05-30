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

	queries := []struct {
		name  string
		query string
	}{
		{
			name: "events per kind",
			query: `query {
				aggregateEvents(input: { dataset: "EVENTS", groupBy: ["KIND"], metrics: ["UNIQUE_EVENTS"], limit: 8 }) {
					rows { dimensions metrics }
				}
			}`,
		},
		{
			name: "top reaction targets",
			query: `query {
				aggregateEvents(input: { dataset: "REACTIONS", groupBy: ["TARGET_EVENT", "REACTION"], metrics: ["UNIQUE_EVENTS"], limit: 5 }) {
					rows { dimensions metrics }
				}
			}`,
		},
		{
			name: "top tag keys",
			query: `query {
				aggregateEvents(input: { dataset: "TAGS", groupBy: ["TAG_KEY"], metrics: ["COUNT", "UNIQUE_EVENTS"], limit: 8 }) {
					rows { dimensions metrics }
				}
			}`,
		},
	}

	var topEvent string
	for _, q := range queries {
		resp, err := post(endpoint, q.query)
		if err != nil {
			panic(err)
		}
		fmt.Printf("\n# %s\n%s\n", q.name, pretty(resp))
		if q.name == "top reaction targets" {
			topEvent = firstDimension(resp, "target_event")
		}
	}

	if topEvent == "" {
		fmt.Println("\n# typed event example\nNo reaction target found yet; ingest more kind 7 events to exercise event engagement.")
		return
	}

	typed := fmt.Sprintf(`query {
		event(id: "%s") {
			id kind pubkey createdAt content
			likes reposts commentCount
			reactionsByContent(first: 5) { content count }
			likers(first: 5) { totalCount edges { node { pubkey followers following } content reactedAt } }
			thread { directReplies participants comments(first: 5) { totalCount edges { node { id content author { pubkey } replyCount } } } }
		}
	}`, topEvent)

	resp, err := post(endpoint, typed)
	if err != nil {
		panic(err)
	}
	fmt.Printf("\n# typed event engagement\n%s\n", pretty(resp))
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
