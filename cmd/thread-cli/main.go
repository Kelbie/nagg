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

type event struct {
	ID        string
	PubKey    string
	Kind      int
	CreatedAt string
	Content   string
	Tags      [][]string
}

type profile struct {
	Name    string
	Picture string
}

func main() {
	rootID := ""
	if len(os.Args) > 1 {
		rootID = os.Args[1]
	}
	if rootID == "" {
		fmt.Fprintln(os.Stderr, "usage: thread-cli <event-id>")
		os.Exit(2)
	}

	endpoint := env("NAGG_GRAPHQL_ENDPOINT", "http://127.0.0.1:8080/graphql")
	resp := mustPost(endpoint, query(rootID))
	context := resp.Data["eventContext"].(map[string]any)

	root := parseEvent(context["root"].(map[string]any))
	events := parseEvents(context["events"].([]any))
	profiles := parseProfiles(context["profiles"].([]any))

	children := map[string][]event{}
	likes := map[string]int{}
	for _, ev := range events {
		if ev.Kind == 7 && isLike(ev.Content) {
			for _, target := range eTags(ev) {
				likes[target]++
			}
			continue
		}
		if isReplyLike(ev.Kind) {
			parent := parentID(ev, root.ID)
			if parent != "" {
				children[parent] = append(children[parent], ev)
			}
		}
	}
	for id := range children {
		sort.Slice(children[id], func(i, j int) bool {
			return children[id][i].CreatedAt < children[id][j].CreatedAt
		})
	}

	fmt.Printf("nagg thread %s\n", root.ID)
	fmt.Printf("comments: %d  likes: %d\n\n", subtreeCount(root.ID, children), likes[root.ID])
	printEvent(root, profiles, likes, children, 0, map[string]struct{}{})
}

func query(rootID string) string {
	return fmt.Sprintf(`query {
		eventContext(id: "%s", limit: 1500) {
			root { id pubkey kind createdAt content tags }
			events { id pubkey kind createdAt content tags }
			profiles { pubkey content createdAt }
		}
	}`, rootID)
}

func printEvent(ev event, profiles map[string]profile, likes map[string]int, children map[string][]event, depth int, seen map[string]struct{}) {
	if _, ok := seen[ev.ID]; ok {
		return
	}
	seen[ev.ID] = struct{}{}

	indent := strings.Repeat("  ", depth)
	author := displayName(profiles, ev.PubKey)
	fmt.Printf("%s%s  %s\n", indent, author, ev.CreatedAt)
	fmt.Printf("%s%s\n", indent, strings.Repeat("-", min(72, max(12, len(author)+2))))
	fmt.Printf("%s%s\n", indent, wrap(ev.Content, indent, 88))
	fmt.Printf("%s↳ comments: %d  likes: %d  id: %s\n\n", indent, len(children[ev.ID]), likes[ev.ID], ev.ID[:12])

	for _, child := range children[ev.ID] {
		printEvent(child, profiles, likes, children, depth+1, seen)
	}
}

func parseEvents(raw []any) []event {
	events := make([]event, 0, len(raw))
	for _, item := range raw {
		events = append(events, parseEvent(item.(map[string]any)))
	}
	return events
}

func parseEvent(raw map[string]any) event {
	return event{
		ID:        raw["id"].(string),
		PubKey:    raw["pubkey"].(string),
		Kind:      int(raw["kind"].(float64)),
		CreatedAt: fmt.Sprint(raw["createdAt"]),
		Content:   fmt.Sprint(raw["content"]),
		Tags:      parseTags(raw["tags"].([]any)),
	}
}

func parseTags(raw []any) [][]string {
	tags := make([][]string, 0, len(raw))
	for _, item := range raw {
		rawTag := item.([]any)
		tag := make([]string, 0, len(rawTag))
		for _, value := range rawTag {
			tag = append(tag, fmt.Sprint(value))
		}
		tags = append(tags, tag)
	}
	return tags
}

func parseProfiles(raw []any) map[string]profile {
	profiles := map[string]profile{}
	for _, item := range raw {
		rawEvent := item.(map[string]any)
		pubkey := stringField(rawEvent, "pubkey")
		if pubkey == "" {
			continue
		}
		if _, ok := profiles[pubkey]; ok {
			continue
		}
		var meta map[string]any
		_ = json.Unmarshal([]byte(stringField(rawEvent, "content")), &meta)
		profiles[pubkey] = profile{
			Name:    first(meta["display_name"], meta["displayName"], meta["name"]),
			Picture: first(meta["picture"]),
		}
	}
	return profiles
}

func stringField(raw map[string]any, key string) string {
	if value, ok := raw[key].(string); ok {
		return value
	}
	return ""
}

func parentID(ev event, rootID string) string {
	var last string
	for _, tag := range ev.Tags {
		if len(tag) < 2 || tag[0] != "e" {
			continue
		}
		if len(tag) >= 4 && tag[3] == "reply" {
			return tag[1]
		}
		last = tag[1]
	}
	if last == "" {
		return rootID
	}
	return last
}

func eTags(ev event) []string {
	var ids []string
	for _, tag := range ev.Tags {
		if len(tag) >= 2 && tag[0] == "e" {
			ids = append(ids, tag[1])
		}
	}
	return ids
}

func isReplyLike(kind int) bool {
	return kind == 1 || kind == 1111
}

func isLike(content string) bool {
	content = strings.TrimSpace(content)
	return content == "" || content == "+"
}

func subtreeCount(id string, children map[string][]event) int {
	count := len(children[id])
	for _, child := range children[id] {
		count += subtreeCount(child.ID, children)
	}
	return count
}

func displayName(profiles map[string]profile, pubkey string) string {
	if p, ok := profiles[pubkey]; ok && p.Name != "" {
		if p.Picture != "" {
			return fmt.Sprintf("%s (%s...) [%s]", p.Name, pubkey[:12], p.Picture)
		}
		return fmt.Sprintf("%s (%s...)", p.Name, pubkey[:12])
	}
	return pubkey[:12] + "..."
}

func wrap(text, indent string, width int) string {
	words := strings.Fields(text)
	if len(words) == 0 {
		return ""
	}
	var lines []string
	line := words[0]
	for _, word := range words[1:] {
		if len(line)+1+len(word) > width {
			lines = append(lines, line)
			line = word
			continue
		}
		line += " " + word
	}
	lines = append(lines, line)
	return strings.Join(lines, "\n"+indent)
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
	client := http.Client{Timeout: 30 * time.Second}
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

func first(values ...any) string {
	for _, value := range values {
		if s := strings.TrimSpace(fmt.Sprint(value)); s != "" && s != "<nil>" {
			return s
		}
	}
	return ""
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
