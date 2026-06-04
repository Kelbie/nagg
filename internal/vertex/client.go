package vertex

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var ErrUnavailable = errors.New("vertex dvm client unavailable")

type Config struct {
	PrivateKey string
	Relay      string
}

type SearchArgs struct {
	Query  string
	Limit  int
	Sort   string
	Source string
}

type RecommendedArgs struct {
	Source string
	Limit  int
	Sort   string
}

type SearchResult struct {
	PubKey string
	Npub   string
	Rank   *float64
	Score  *float64
	Nodes  *int
}

type ProfileResult struct {
	PubKey       string        `json:"pubkey"`
	Npub         string        `json:"npub"`
	Rank         float64       `json:"rank"`
	Score        *float64      `json:"score,omitempty"`
	Followers    *uint64       `json:"followers,omitempty"`
	Follows      *uint64       `json:"follows,omitempty"`
	CreatedAt    *int64        `json:"created_at,omitempty"`
	Nodes        *int          `json:"nodes,omitempty"`
	TopFollowers []TopFollower `json:"topFollowers,omitempty"`
}

type TopFollower struct {
	PubKey string   `json:"pubkey"`
	Npub   string   `json:"npub"`
	Rank   float64  `json:"rank"`
	Score  *float64 `json:"score,omitempty"`
}

type Client struct {
	privateKey  string
	relay       string
	profile     *cachedCall[string, ProfileResult]
	search      *cachedCall[SearchArgs, []SearchResult]
	recommended *cachedCall[RecommendedArgs, []SearchResult]
}

type dvmRecord struct {
	PubKey    string   `json:"pubkey"`
	Rank      *float64 `json:"rank"`
	Followers *uint64  `json:"followers"`
	Follows   *uint64  `json:"follows"`
	CreatedAt *int64   `json:"created_at"`
}

func New(cfg Config) (*Client, error) {
	if strings.TrimSpace(cfg.PrivateKey) == "" {
		return nil, ErrUnavailable
	}
	relay := strings.TrimSpace(cfg.Relay)
	if relay == "" {
		relay = "wss://relay.vertexlab.io"
	}
	c := &Client{
		privateKey: strings.TrimSpace(cfg.PrivateKey),
		relay:      relay,
	}
	var err error
	c.profile, err = newCachedCall(c.performProfile, func(pubkey string) string {
		return pubkey
	}, func(result ProfileResult) bool {
		return result.PubKey != ""
	}, time.Hour, 7*24*time.Hour, 10_000)
	if err != nil {
		return nil, err
	}
	c.search, err = newCachedCall(c.performSearch, func(args SearchArgs) string {
		return SearchCacheKey(args)
	}, func(results []SearchResult) bool {
		return len(results) > 0
	}, time.Hour, 48*time.Hour, 2_000)
	if err != nil {
		return nil, err
	}
	c.recommended, err = newCachedCall(c.performRecommended, func(args RecommendedArgs) string {
		source := strings.TrimSpace(args.Source)
		if source == "" {
			source = "default"
		}
		sortKey := args.Sort
		if sortKey == "" {
			sortKey = "globalPagerank"
		}
		return source + "|" + strconv.Itoa(args.Limit) + "|" + sortKey
	}, func(results []SearchResult) bool {
		return len(results) > 0
	}, time.Hour, 48*time.Hour, 100)
	if err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) Search(ctx context.Context, args SearchArgs) ([]SearchResult, bool, error) {
	args = NormalizeSearchArgs(args)
	return c.search.Get(ctx, args)
}

func (c *Client) SearchRefresh(ctx context.Context, args SearchArgs) ([]SearchResult, error) {
	return c.search.Refresh(ctx, NormalizeSearchArgs(args))
}

func (c *Client) Recommended(ctx context.Context, args RecommendedArgs) ([]SearchResult, bool, error) {
	args.Source = strings.TrimSpace(args.Source)
	args.Limit = clampLimit(args.Limit, 5)
	if args.Sort == "" {
		args.Sort = "globalPagerank"
	}
	return c.recommended.Get(ctx, args)
}

func (c *Client) Profile(ctx context.Context, pubkey string) (ProfileResult, bool, error) {
	normalized, ok := NormalizePubkey(pubkey)
	if !ok {
		return ProfileResult{}, false, fmt.Errorf("invalid pubkey")
	}
	return c.profile.Get(ctx, normalized)
}

func (c *Client) ProfileRefresh(ctx context.Context, pubkey string) (ProfileResult, error) {
	normalized, ok := NormalizePubkey(pubkey)
	if !ok {
		return ProfileResult{}, fmt.Errorf("invalid pubkey")
	}
	return c.profile.Refresh(ctx, normalized)
}

func (c *Client) SearchStats() CacheStats {
	return c.search.Stats()
}

func (c *Client) ProfileStats() CacheStats {
	return c.profile.Stats()
}

func (c *Client) RecommendedStats() CacheStats {
	return c.recommended.Stats()
}

func (c *Client) performSearch(ctx context.Context, args SearchArgs) ([]SearchResult, error) {
	args = NormalizeSearchArgs(args)
	tags := nostr.Tags{
		{"param", "search", args.Query},
		{"param", "limit", strconv.Itoa(args.Limit)},
	}
	if args.Sort != "" {
		tags = append(tags, nostr.Tag{"param", "sort", args.Sort})
	}
	if args.Source != "" {
		tags = append(tags, nostr.Tag{"param", "source", args.Source})
	}
	return runDVM(ctx, c, SearchRequestKind, SearchResponseKind, tags, func(event *nostr.Event) ([]SearchResult, error) {
		return parseSearchResults(event, args.Limit)
	})
}

func (c *Client) performRecommended(ctx context.Context, args RecommendedArgs) ([]SearchResult, error) {
	tags := nostr.Tags{
		{"param", "sort", args.Sort},
		{"param", "limit", strconv.Itoa(args.Limit)},
	}
	if args.Source != "" {
		tags = append(tags, nostr.Tag{"param", "source", args.Source})
	}
	return runDVM(ctx, c, RecommendRequestKind, RecommendResponseKind, tags, func(event *nostr.Event) ([]SearchResult, error) {
		return parseSearchResults(event, args.Limit)
	})
}

func (c *Client) performProfile(ctx context.Context, pubkey string) (ProfileResult, error) {
	tags := nostr.Tags{
		{"param", "target", pubkey},
		{"param", "limit", "7"},
	}
	return runDVM(ctx, c, ProfileRequestKind, ProfileResponseKind, tags, parseProfileResult)
}

func runDVM[T any](
	ctx context.Context,
	c *Client,
	requestKind int,
	responseKind int,
	tags nostr.Tags,
	parse func(*nostr.Event) (T, error),
) (T, error) {
	var zero T
	ctx, cancel := context.WithTimeout(ctx, time.Duration(RequestTimeout)*time.Millisecond)
	defer cancel()

	request := nostr.Event{
		CreatedAt: nostr.Now(),
		Kind:      requestKind,
		Tags:      tags,
		Content:   "",
	}
	if err := request.Sign(c.privateKey); err != nil {
		return zero, err
	}

	relay, err := nostr.RelayConnect(ctx, c.relay)
	if err != nil {
		return zero, err
	}
	defer relay.Close()

	sub, err := relay.Subscribe(ctx, nostr.Filters{{
		Kinds: []int{responseKind, NoticeKind},
		Tags:  nostr.TagMap{"e": []string{request.ID}},
	}}, nostr.WithLabel("nagg-dvm"))
	if err != nil {
		return zero, err
	}
	defer sub.Unsub()

	select {
	case <-sub.EndOfStoredEvents:
	case <-time.After(2 * time.Second):
	case <-ctx.Done():
		return zero, ctx.Err()
	}

	go func() {
		publishCtx, publishCancel := context.WithTimeout(ctx, 7*time.Second)
		defer publishCancel()
		_ = relay.Publish(publishCtx, request)
	}()

	for {
		select {
		case event, ok := <-sub.Events:
			if !ok {
				return zero, fmt.Errorf("dvm subscription closed")
			}
			if event.Kind == NoticeKind {
				return zero, fmt.Errorf("DVM %d error: %s", requestKind, dvmNoticeMessage(event))
			}
			if event.Kind != responseKind {
				continue
			}
			return parse(event)
		case reason := <-sub.ClosedReason:
			return zero, fmt.Errorf("dvm subscription closed: %s", reason)
		case <-ctx.Done():
			return zero, fmt.Errorf("DVM %d request timed out: %w", requestKind, ctx.Err())
		}
	}
}

func parseSearchResults(event *nostr.Event, limit int) ([]SearchResult, error) {
	var records []dvmRecord
	if err := json.Unmarshal([]byte(event.Content), &records); err != nil {
		return nil, err
	}
	nodes := ReadNodesTag(event)
	results := make([]SearchResult, 0, len(records))
	for _, record := range records {
		pubkey, ok := NormalizePubkey(record.PubKey)
		if !ok {
			continue
		}
		results = append(results, SearchResult{
			PubKey: pubkey,
			Npub:   Npub(pubkey),
			Rank:   record.Rank,
			Score:  PagerankToScore(record.Rank, nodes),
			Nodes:  nodes,
		})
	}
	sort.SliceStable(results, func(i, j int) bool {
		return rankValue(results[i].Rank) > rankValue(results[j].Rank)
	})
	if limit > 0 && len(results) > limit {
		return results[:limit], nil
	}
	return results, nil
}

func parseProfileResult(event *nostr.Event) (ProfileResult, error) {
	var records []dvmRecord
	if err := json.Unmarshal([]byte(event.Content), &records); err != nil {
		return ProfileResult{}, err
	}
	if len(records) == 0 {
		return ProfileResult{}, fmt.Errorf("empty profile response")
	}
	nodes := ReadNodesTag(event)
	target := records[0]
	pubkey, ok := NormalizePubkey(target.PubKey)
	if !ok {
		return ProfileResult{}, fmt.Errorf("profile response missing target pubkey")
	}
	result := ProfileResult{
		PubKey:    pubkey,
		Npub:      Npub(pubkey),
		Rank:      rankValue(target.Rank),
		Score:     PagerankToScore(target.Rank, nodes),
		Followers: target.Followers,
		Follows:   target.Follows,
		CreatedAt: target.CreatedAt,
		Nodes:     nodes,
	}
	for _, record := range records[1:] {
		followerPubkey, ok := NormalizePubkey(record.PubKey)
		if !ok {
			continue
		}
		result.TopFollowers = append(result.TopFollowers, TopFollower{
			PubKey: followerPubkey,
			Npub:   Npub(followerPubkey),
			Rank:   rankValue(record.Rank),
			Score:  PagerankToScore(record.Rank, nodes),
		})
	}
	return result, nil
}

func dvmNoticeMessage(event *nostr.Event) string {
	for _, tag := range event.Tags {
		if len(tag) >= 3 && tag[0] == "status" {
			return tag[2]
		}
	}
	if strings.TrimSpace(event.Content) != "" {
		return event.Content
	}
	return "Unknown DVM error"
}

func rankValue(rank *float64) float64 {
	if rank == nil || math.IsNaN(*rank) || math.IsInf(*rank, 0) {
		return 0
	}
	return *rank
}

func clampLimit(value int, fallback int) int {
	if value <= 0 {
		return fallback
	}
	if value > 100 {
		return 100
	}
	return value
}

const DefaultSearchSort = "globalPagerank"

func NormalizeSearchArgs(args SearchArgs) SearchArgs {
	args.Query = strings.TrimSpace(args.Query)
	args.Limit = clampLimit(args.Limit, 5)
	args.Sort = strings.TrimSpace(args.Sort)
	if args.Sort == "" {
		args.Sort = DefaultSearchSort
	}
	args.Source = strings.TrimSpace(args.Source)
	return args
}

func SearchCacheKey(args SearchArgs) string {
	args = NormalizeSearchArgs(args)
	return strings.ToLower(args.Query) + "|" + strconv.Itoa(args.Limit) + "|" + args.Sort + "|" + args.Source
}
