package aiproviders

import (
	"encoding/json"
	"strings"

	"github.com/nbd-wtf/go-nostr"
)

// AnnouncementKind is the Routstr provider announcement: an addressable
// kind-38421 event. Every routstr client reads this kind to build its provider
// list, so it — not any one node's /v1/providers/ — is the registry. A node's
// directory returns only what THAT node has seen, which is how a picker that
// should list dozens listed one.
const AnnouncementKind = 38421

// announcementLimit bounds the relay subscription. Relays hold years of
// announcements and the kind is not exclusive to Routstr (lnproxy-v1 publishes
// on it too), so the subscription is bounded and the results are filtered.
const announcementLimit = 300

// found is one discovery hit before merging: whatever that source knew.
// Sources fill each other's gaps rather than overwrite, because none of them is
// a complete record — an announcement usually carries the name, a directory
// usually carries the accepted mints, and /v1/info carries both plus the
// operator's own npub.
type found struct {
	baseURL string
	name    string
	pubkey  string
	mints   []string
}

// directoryRow is the shape both sources speak: a node's /v1/providers/ rows
// and the JSON-content form of a kind-38421 announcement are the same object.
type directoryRow struct {
	EndpointURL  string   `json:"endpoint_url"`
	EndpointURLs []string `json:"endpoint_urls"`
	Name         string   `json:"name"`
	Pubkey       string   `json:"pubkey"`
	MintURLs     []string `json:"mint_urls"`
}

// directoryBody is a node's GET /v1/providers/ response.
type directoryBody struct {
	Providers []directoryRow `json:"providers"`
}

// nodeInfo is a node's GET /v1/info — its own description of itself. Older
// nodes do not serve it, which means "this node does not say", not "broken".
type nodeInfo struct {
	Name  string   `json:"name"`
	Npub  string   `json:"npub"`
	Mints []string `json:"mints"`
}

// endpoint picks the first usable https endpoint a row offers.
func (r directoryRow) endpoint() string {
	for _, candidate := range append([]string{r.EndpointURL}, r.EndpointURLs...) {
		if url := normalizeBaseURL(candidate); url != "" {
			return url
		}
	}
	return ""
}

func (r directoryRow) toFound(signer string) (found, bool) {
	url := r.endpoint()
	if url == "" {
		return found{}, false
	}
	// The row's own key when it carries one; otherwise the signer's, which is
	// the relationship an announcement asserts anyway.
	pubkey := normalizeHexPubkey(r.Pubkey)
	if pubkey == "" {
		pubkey = signer
	}
	return found{baseURL: url, name: strings.TrimSpace(r.Name), pubkey: pubkey, mints: cleanMints(r.MintURLs)}, true
}

// parseDirectory reads a node's /v1/providers/ body. A node that does not serve
// the route, or serves something else on it, yields nothing rather than an
// error: that is a normal node, not a failure.
func parseDirectory(body []byte) []found {
	var parsed directoryBody
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil
	}
	out := make([]found, 0, len(parsed.Providers))
	for _, row := range parsed.Providers {
		if hit, ok := row.toFound(""); ok {
			out = append(out, hit)
		}
	}
	return out
}

// parseAnnouncement reads the providers out of one kind-38421 event.
//
// Two shapes are in the wild and both are honoured, same as the Routstr SDK:
// `u` tags carrying endpoints, or JSON content holding a directory. The kind is
// not exclusive to Routstr, so an event with neither shape yields nothing
// rather than a bogus row.
func parseAnnouncement(event *nostr.Event) []found {
	if event == nil || event.Kind != AnnouncementKind {
		return nil
	}
	signer := normalizeHexPubkey(event.PubKey)

	var endpoints []string
	var name string
	for _, tag := range event.Tags {
		if len(tag) < 2 {
			continue
		}
		switch tag[0] {
		case "u":
			if url := normalizeBaseURL(tag[1]); url != "" {
				endpoints = append(endpoints, url)
			}
		case "name":
			if name == "" {
				name = strings.TrimSpace(tag[1])
			}
		}
	}
	if len(endpoints) > 0 {
		out := make([]found, 0, len(endpoints))
		for _, url := range endpoints {
			out = append(out, found{baseURL: url, name: name, pubkey: signer})
		}
		return out
	}

	content := strings.TrimSpace(event.Content)
	if content == "" {
		return nil
	}
	// The content is either a bare array of rows or the same {"providers":[…]}
	// envelope the HTTP directory uses.
	var rows []directoryRow
	if err := json.Unmarshal([]byte(content), &rows); err != nil {
		var envelope directoryBody
		if err := json.Unmarshal([]byte(content), &envelope); err != nil {
			return nil
		}
		rows = envelope.Providers
	}
	out := make([]found, 0, len(rows))
	for _, row := range rows {
		if hit, ok := row.toFound(signer); ok {
			out = append(out, hit)
		}
	}
	return out
}

// normalizeHexPubkey keeps only a well-formed 64-char hex key. Anything else
// (an npub, a truncated id, a display name) would poison the follower lookup.
func normalizeHexPubkey(raw string) string {
	key := strings.ToLower(strings.TrimSpace(raw))
	if len(key) != 64 {
		return ""
	}
	for _, r := range key {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return ""
		}
	}
	return key
}

// cleanMints trims and dedupes a mint list, preserving publication order so the
// operator's own preference survives.
func cleanMints(mints []string) []string {
	if len(mints) == 0 {
		return nil
	}
	out := make([]string, 0, len(mints))
	seen := make(map[string]struct{}, len(mints))
	for _, mint := range mints {
		mint = strings.TrimRight(strings.TrimSpace(mint), "/")
		if mint == "" {
			continue
		}
		if _, dup := seen[mint]; dup {
			continue
		}
		seen[mint] = struct{}{}
		out = append(out, mint)
	}
	return out
}
