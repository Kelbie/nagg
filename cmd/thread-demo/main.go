package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"time"
)

type graphQLResponse struct {
	Data   map[string]any   `json:"data"`
	Errors []map[string]any `json:"errors"`
}

type profile struct {
	Name    string
	Picture string
}

func main() {
	root := env("NAGG_THREAD_ROOT", "")
	if len(os.Args) > 1 {
		root = os.Args[1]
	}
	if root == "" {
		panic("usage: thread-demo <root-event-id>")
	}
	endpoint := env("NAGG_GRAPHQL_ENDPOINT", "http://127.0.0.1:8080/graphql")

	rootResp := mustPost(endpoint, fmt.Sprintf(`query {
		event(id: "%s") { id pubkey kind createdAt content tags }
	}`, root))
	rootEvent := rootResp.Data["event"].(map[string]any)

	associatedResp := mustPost(endpoint, fmt.Sprintf(`query {
		aggregateEvents(input: {
			dataset: "TAGS"
			tags: [{ key: "e", value: "%s" }]
			groupBy: ["KIND"]
			metrics: ["UNIQUE_EVENTS", "UNIQUE_PUBKEYS"]
			limit: 20
		}) { rows { dimensions metrics } }
	}`, root))

	repliesResp := mustPost(endpoint, fmt.Sprintf(`query {
		events(input: {
			kinds: [1, 1111]
			tags: [{ key: "e", value: "%s" }]
			limit: 100
		}) {
			nodes { id pubkey kind createdAt content tags }
		}
	}`, root))
	replies := nodes(repliesResp)

	pubkeys := map[string]struct{}{rootEvent["pubkey"].(string): {}}
	for _, reply := range replies {
		pubkeys[reply["pubkey"].(string)] = struct{}{}
	}
	pubkeyList := keys(pubkeys)
	profiles := fetchProfiles(endpoint, pubkeyList)

	fmt.Printf("Thread root: %s\n", root)
	fmt.Printf("Author: %s\n", describe(profiles, rootEvent["pubkey"].(string)))
	fmt.Printf("Kind: %.0f\n", rootEvent["kind"].(float64))
	fmt.Printf("Created: %s\n", rootEvent["createdAt"])
	fmt.Printf("Content: %s\n\n", truncate(fmt.Sprint(rootEvent["content"]), 280))

	fmt.Println("Associated events by kind:")
	for _, row := range aggregateRows(associatedResp) {
		dims := row["dimensions"].(map[string]any)
		metrics := row["metrics"].(map[string]any)
		fmt.Printf("- kind %s: %v events, %v pubkeys\n", dims["kind"], metrics["unique_events"], metrics["unique_pubkeys"])
	}

	fmt.Printf("\nReplies fetched: %d\n", len(replies))
	for i, reply := range replies {
		if i >= 12 {
			fmt.Printf("- ... %d more replies omitted\n", len(replies)-i)
			break
		}
		pubkey := reply["pubkey"].(string)
		p := profiles[pubkey]
		fmt.Printf("- %s\n", describe(profiles, pubkey))
		if p.Picture != "" {
			fmt.Printf("  picture: %s\n", p.Picture)
		}
		fmt.Printf("  %s\n", truncate(fmt.Sprint(reply["content"]), 220))
	}
}

func fetchProfiles(endpoint string, pubkeys []string) map[string]profile {
	rawPubkeys, _ := json.Marshal(pubkeys)
	resp := mustPost(endpoint, fmt.Sprintf(`query {
		events(input: {
			kinds: [0]
			pubkeys: %s
			limit: %d
		}) {
			nodes { pubkey content createdAt }
		}
	}`, rawPubkeys, max(1, len(pubkeys)*2)))

	out := map[string]profile{}
	for _, node := range nodes(resp) {
		pubkey := node["pubkey"].(string)
		if _, ok := out[pubkey]; ok {
			continue
		}
		var meta map[string]any
		_ = json.Unmarshal([]byte(fmt.Sprint(node["content"])), &meta)
		out[pubkey] = profile{
			Name:    first(meta["display_name"], meta["displayName"], meta["name"], pubkey[:12]),
			Picture: first(meta["picture"]),
		}
	}
	return out
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
	client := http.Client{Timeout: 20 * time.Second}
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

func nodes(resp graphQLResponse) []map[string]any {
	events := resp.Data["events"].(map[string]any)
	rawNodes := events["nodes"].([]any)
	out := make([]map[string]any, 0, len(rawNodes))
	for _, raw := range rawNodes {
		out = append(out, raw.(map[string]any))
	}
	return out
}

func aggregateRows(resp graphQLResponse) []map[string]any {
	agg := resp.Data["aggregateEvents"].(map[string]any)
	rawRows := agg["rows"].([]any)
	out := make([]map[string]any, 0, len(rawRows))
	for _, raw := range rawRows {
		out = append(out, raw.(map[string]any))
	}
	return out
}

func keys(m map[string]struct{}) []string {
	out := make([]string, 0, len(m))
	for key := range m {
		out = append(out, key)
	}
	sort.Strings(out)
	return out
}

func describe(profiles map[string]profile, pubkey string) string {
	if p, ok := profiles[pubkey]; ok && p.Name != "" {
		return p.Name + " (" + pubkey[:12] + "...)"
	}
	return pubkey[:12] + "..."
}

func first(values ...any) string {
	for _, value := range values {
		if s := strings.TrimSpace(fmt.Sprint(value)); s != "" && s != "<nil>" {
			return s
		}
	}
	return ""
}

func truncate(value string, n int) string {
	value = strings.Join(strings.Fields(value), " ")
	if len(value) <= n {
		return value
	}
	return value[:n] + "..."
}

func pretty(v any) string {
	raw, _ := json.MarshalIndent(v, "", "  ")
	return string(raw)
}

func env(key, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return fallback
}
