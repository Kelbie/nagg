package vertex

import (
	"math"
	"strconv"
	"strings"

	"github.com/nbd-wtf/go-nostr"
	"github.com/nbd-wtf/go-nostr/nip19"
)

const (
	NoticeKind            = 7000
	ProfileRequestKind    = 5312
	ProfileResponseKind   = 6312
	RecommendRequestKind  = 5313
	RecommendResponseKind = 6313
	SearchRequestKind     = 5315
	SearchResponseKind    = 6315
	RequestTimeout        = 15_000
)

func PagerankToScore(pagerank *float64, nodes *int) *float64 {
	if pagerank == nil || !isFinitePositive(*pagerank) {
		return nil
	}
	if nodes == nil || *nodes <= 0 {
		return nil
	}
	b := 0.76
	a := 0.38
	c := 1 - b
	denom := float64(*nodes)*(*pagerank) + c
	if denom <= 0 {
		return nil
	}
	value := 1 - math.Pow(c/denom, a)
	if !isFinite(value) {
		return nil
	}
	score := math.Round(value*10_000) / 100
	return &score
}

func ReadNodesTag(event *nostr.Event) *int {
	if event == nil {
		return nil
	}
	for _, tag := range event.Tags {
		if len(tag) < 2 || tag[0] != "nodes" {
			continue
		}
		n, err := strconv.Atoi(tag[1])
		if err == nil && n > 0 {
			return &n
		}
		return nil
	}
	return nil
}

func NormalizePubkey(input string) (string, bool) {
	input = strings.TrimSpace(input)
	if nostr.IsValid32ByteHex(input) {
		return strings.ToLower(input), true
	}
	prefix, value, err := nip19.Decode(input)
	if err != nil || prefix != "npub" {
		return "", false
	}
	pubkey, ok := value.(string)
	if !ok || !nostr.IsValid32ByteHex(pubkey) {
		return "", false
	}
	return strings.ToLower(pubkey), true
}

func Npub(pubkey string) string {
	npub, err := nip19.EncodePublicKey(pubkey)
	if err != nil {
		return ""
	}
	return npub
}

func isFinitePositive(value float64) bool {
	return isFinite(value) && value > 0
}

func isFinite(value float64) bool {
	return !math.IsNaN(value) && !math.IsInf(value, 0)
}
