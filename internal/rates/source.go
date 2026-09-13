// Package rates collects BTC fiat observations without persistent storage.
package rates

import (
	"encoding/hex"
	"encoding/json"
	"log/slog"
	"net/url"
	"regexp"
	"sort"
	"strings"

	"github.com/nbd-wtf/go-nostr/nip19"
)

type Kind string

const (
	NostrNote Kind = "nostr-note"
	HTTPJSON  Kind = "http-json"
)

// ID identifies a provider; (ID, Currency) identifies a source. Priority only
// orders fetching (lower first), never weights the consensus.
type Source struct {
	ID       string `json:"id"`
	Currency string `json:"currency"`
	Kind     Kind   `json:"kind"`
	Pubkey   string `json:"pubkey,omitempty"`
	URL      string `json:"url,omitempty"`
	JSONKey  string `json:"jsonKey,omitempty"`
	Priority int    `json:"priority"`
}

var Registry = []Source{
	{ID: "usdbtcbot", Currency: "USD", Kind: NostrNote, Pubkey: "npub1qmnernjqn55an90lvv9gtgtxpnnk5ukxa4u6tzgsh885mauu355sjau5g5"},
	{ID: "eurbtcbot", Currency: "EUR", Kind: NostrNote, Pubkey: "520463e322421478f272218cf36edfaa7a36232a91e1c9d151247bbddee23cf4"},
	{ID: "chfbtcbot", Currency: "CHF", Kind: NostrNote, Pubkey: "npub1kp2cz8u85up3yslrtvgl4k255zq4pkrrmdg79258kug23ezu8z2q0l5cmn"},
	{ID: "mempool", Currency: "USD", Kind: HTTPJSON, URL: "https://mempool.space/api/v1/prices", JSONKey: "USD", Priority: 10},
	{ID: "mempool", Currency: "EUR", Kind: HTTPJSON, URL: "https://mempool.space/api/v1/prices", JSONKey: "EUR", Priority: 10},
	{ID: "mempool", Currency: "GBP", Kind: HTTPJSON, URL: "https://mempool.space/api/v1/prices", JSONKey: "GBP", Priority: 10},
	{ID: "mempool", Currency: "CHF", Kind: HTTPJSON, URL: "https://mempool.space/api/v1/prices", JSONKey: "CHF", Priority: 10},
}

var currencyPattern = regexp.MustCompile(`^[A-Z]{3}$`)
var sourceIDPattern = regexp.MustCompile(`^[a-zA-Z0-9_-]{1,64}$`)

// LoadSources appends valid extras, normalizes keys, and ignores invalid entries
// without logging their contents (URLs and configuration may contain secrets).
func LoadSources(extra string, httpEnabled bool, logger *slog.Logger) []Source {
	if logger == nil {
		logger = slog.Default()
	}
	all := append([]Source(nil), Registry...)
	if strings.TrimSpace(extra) != "" {
		var extras []Source
		if err := json.Unmarshal([]byte(extra), &extras); err != nil {
			logger.Warn("rates.sources.invalid_json")
		} else {
			all = append(all, extras...)
		}
	}
	var out []Source
	seen := map[string]bool{}
	for i, src := range all {
		if !normalizeSource(&src) || seen[src.ID+":"+src.Currency] {
			logger.Warn("rates.sources.invalid", "index", i)
			continue
		}
		seen[src.ID+":"+src.Currency] = true
		if src.Kind == HTTPJSON && !httpEnabled {
			continue
		}
		out = append(out, src)
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Priority < out[j].Priority })
	return out
}

func normalizeSource(s *Source) bool {
	if !sourceIDPattern.MatchString(s.ID) || !currencyPattern.MatchString(s.Currency) {
		return false
	}
	switch s.Kind {
	case NostrNote:
		if strings.HasPrefix(s.Pubkey, "npub1") {
			prefix, value, err := nip19.Decode(s.Pubkey)
			if err != nil || prefix != "npub" {
				return false
			}
			key, ok := value.(string)
			if !ok {
				return false
			}
			s.Pubkey = key
		}
		key, err := hex.DecodeString(s.Pubkey)
		if err != nil || len(key) != 32 {
			return false
		}
		s.Pubkey = strings.ToLower(s.Pubkey)
		return true
	case HTTPJSON:
		u, err := url.Parse(s.URL)
		return err == nil && (u.Scheme == "http" || u.Scheme == "https") && u.Host != "" && u.User == nil && s.JSONKey != ""
	default:
		return false
	}
}
