package vertex

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nbd-wtf/go-nostr"
)

var ErrInsufficientCredits = errors.New("insufficient Vertex credits")

type ErrDVMRejected struct{ Message string }

func (e *ErrDVMRejected) Error() string { return e.Message }

func noticeError(event *nostr.Event) error {
	for _, tag := range event.Tags {
		if len(tag) >= 2 && tag[0] == "status" {
			status := strings.ToLower(strings.Join(tag[1:], " ") + " " + dvmNoticeMessage(event))
			status = strings.NewReplacer("_", " ", "-", " ").Replace(status)
			if strings.Contains(status, "insufficient") && (strings.Contains(status, "credit") || strings.Contains(status, "balance")) {
				return ErrInsufficientCredits
			}
		}
	}
	// Do not reflect arbitrary upstream text (which can include the query or tags).
	return &ErrDVMRejected{Message: "Vertex rejected the request"}
}

type SignedRequestArgs struct {
	Target string
	Search SearchArgs
}

// ValidateSignedRequest bounds untrusted input and parses the exact cache key.
// Duplicate/unknown params are rejected to avoid ambiguous upstream semantics.
func ValidateSignedRequest(event nostr.Event, now time.Time) (SignedRequestArgs, error) {
	var out SignedRequestArgs
	// Canonical hex prevents one signing identity from splitting its rate
	// allowance across differently cased pubkey spellings.
	if !nostr.IsValid32ByteHex(event.PubKey) {
		return out, errors.New("invalid Vertex signer pubkey")
	}
	if event.Kind != ProfileRequestKind && event.Kind != SearchRequestKind && event.Kind != RecommendRequestKind {
		return out, errors.New("unsupported Vertex request kind")
	}
	if int64(event.CreatedAt) < now.Unix()-300 || int64(event.CreatedAt) > now.Unix()+300 {
		return out, errors.New("Vertex request timestamp outside 300 second window")
	}
	if len(event.Content) > 1024 || len(event.Tags) > 32 {
		return out, errors.New("Vertex request too large")
	}
	params := map[string]string{}
	for _, tag := range event.Tags {
		if len(tag) > 8 {
			return out, errors.New("Vertex tag too large")
		}
		for _, value := range tag {
			if len(value) > 1024 {
				return out, errors.New("Vertex tag too large")
			}
		}
		if len(tag) == 0 || tag[0] != "param" {
			continue
		}
		if len(tag) != 3 {
			return out, errors.New("invalid Vertex param")
		}
		if _, exists := params[tag[1]]; exists {
			return out, errors.New("duplicate Vertex param")
		}
		switch tag[1] {
		case "target", "search", "limit", "sort", "source":
		default:
			return out, errors.New("unsupported Vertex param")
		}
		params[tag[1]] = tag[2]
	}
	limit := 5
	if v, ok := params["limit"]; ok {
		n, err := strconv.Atoi(v)
		if err != nil || n < 1 || n > 100 {
			return out, errors.New("Vertex limit must be 1 to 100")
		}
		limit = n
	}
	sort := params["sort"]
	if sort != "" && sort != "globalPagerank" && sort != "followerCount" && sort != "personalizedPagerank" {
		return out, errors.New("unsupported Vertex sort")
	}
	out.Search = NormalizeSearchArgs(SearchArgs{Query: params["search"], Limit: limit, Sort: sort, Source: params["source"]})
	if v := params["source"]; v != "" {
		if pk, ok := NormalizePubkey(v); !ok || pk != v {
			return out, errors.New("invalid Vertex source")
		}
	}
	if event.Kind == ProfileRequestKind {
		pk, ok := NormalizePubkey(params["target"])
		if !ok || pk != params["target"] {
			return out, errors.New("invalid Vertex target")
		}
		out.Target = pk
	} else if params["target"] != "" {
		return out, errors.New("unexpected Vertex target")
	}
	if event.Kind == SearchRequestKind {
		if len(out.Search.Query) < 3 || out.Search.Query != params["search"] {
			return out, errors.New("invalid Vertex search query")
		}
	} else if params["search"] != "" {
		return out, errors.New("unexpected Vertex search query")
	}
	if sort == "personalizedPagerank" && out.Search.Source == "" {
		return out, errors.New("personalized Vertex requests require source")
	}
	if event.ID != event.GetID() {
		return out, errors.New("invalid Vertex event id")
	}
	valid, err := event.CheckSignature()
	if err != nil || !valid {
		return out, errors.New("invalid Vertex signature")
	}
	return out, nil
}

type SignedResult struct {
	Kind      string         `json:"kind"`
	Profile   *ProfileResult `json:"-"`
	Results   []SearchResult `json:"-"`
	FetchedAt int64          `json:"fetchedAt"`
}

func (c *Client) RelaySigned(ctx context.Context, request nostr.Event) (SignedResult, error) {
	args, err := ValidateSignedRequest(request, time.Now())
	if err != nil {
		return SignedResult{}, err
	}
	return runSignedDVM(ctx, c, request, request.Kind+1000, func(event *nostr.Event) (SignedResult, error) {
		result := SignedResult{FetchedAt: time.Now().Unix()}
		if request.Kind == ProfileRequestKind {
			profile, err := parseProfileResult(event)
			if err != nil {
				return result, err
			}
			if profile.PubKey != args.Target {
				return result, errors.New("Vertex response target mismatch")
			}
			profile.FetchedAt = &result.FetchedAt
			profile.Response = event
			if args.Search.Sort != DefaultSearchSort {
				profile.Score = nil
				for i := range profile.TopFollowers {
					profile.TopFollowers[i].Score = nil
				}
			}
			result.Kind, result.Profile = "profile", &profile
		} else {
			rows, err := parseSearchResults(event, args.Search.Limit)
			if err != nil {
				return result, err
			}
			for i := range rows {
				rows[i].FetchedAt = &result.FetchedAt
				if args.Search.Sort != DefaultSearchSort {
					rows[i].Score = nil
				}
			}
			result.Kind, result.Results = "search", rows
			if request.Kind == RecommendRequestKind {
				result.Kind = "recommend"
			}
		}
		return result, nil
	})
}

// The transport seam retains subscribe-before-publish behavior in production
// and allows tests to exercise the full signed request/response path offline.
type relayDialer func(context.Context, string) (dvmRelay, error)
type dvmRelay interface {
	Subscribe(context.Context, nostr.Filters) (dvmSubscription, error)
	Publish(context.Context, nostr.Event) error
	Close() error
}
type dvmSubscription struct {
	events <-chan *nostr.Event
	eose   <-chan struct{}
	closed <-chan string
	close  func()
}
type liveRelay struct{ *nostr.Relay }

func dialRelay(ctx context.Context, url string) (dvmRelay, error) {
	r, err := nostr.RelayConnect(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("Vertex relay connection failed: %w", err)
	}
	return liveRelay{r}, nil
}
func (r liveRelay) Subscribe(ctx context.Context, filters nostr.Filters) (dvmSubscription, error) {
	sub, err := r.Relay.Subscribe(ctx, filters, nostr.WithLabel("nagg-dvm"))
	if err != nil {
		return dvmSubscription{}, err
	}
	return dvmSubscription{events: sub.Events, eose: sub.EndOfStoredEvents, closed: sub.ClosedReason, close: sub.Unsub}, nil
}
