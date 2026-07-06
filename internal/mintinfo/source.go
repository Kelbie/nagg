// Package mintinfo snapshots each known Cashu mint's NUT-06 /v1/info over time,
// storing a full canonical document only when it changes and recording every
// poll, then serving the history as an initial document plus RFC 6902 diffs.
//
// It is deliberately OUTSIDE the rules registry (internal/rules): mint info is
// polled HTTP config, not a Nostr primitive, so this sits alongside
// internal/auditor and internal/enrich — the "deliberately outside the
// registry" surfaces (docs/rules-registry.md). The engine (poll → canonicalize
// → hash → store-if-changed → diff) never mentions Cashu; everything
// cashu-specific is data in a Source.
package mintinfo

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"github.com/gowebpki/jcs"
)

// Source declares what to snapshot and how to detect change. The default is
// cashu NUT-06; a second document family (a fedimint meta endpoint, a future
// NUT) is one more Source with no engine change — this is the "declarative
// separation between snapshots", kept local like enrich's task list rather than
// as a global registry entry.
type Source struct {
	// Name identifies the source in logs/metrics ("cashu_nut06").
	Name string
	// InfoPath is appended to each mint URL to reach the document ("/v1/info").
	InfoPath string
	// VolatileKeys are top-level JSON keys stripped before hashing and diffing
	// because they change on every request. For NUT-06 this is ["time"], the
	// mint's current clock — hash the raw body and every poll is a false change.
	VolatileKeys []string
}

// CashuNUT06 is the default source: the NUT-06 mint info endpoint, excluding the
// server clock from change detection.
var CashuNUT06 = Source{
	Name:         "cashu_nut06",
	InfoPath:     "/v1/info",
	VolatileKeys: []string{"time"},
}

// Canonicalize strips the source's volatile keys, then applies RFC 8785 JCS
// (recursive key sorting + normalized number/string formatting) so the result
// is a stable byte representation of the document's meaningful content. The
// output is what we store, hash, diff, and display — one consistent object, so
// a client never sees volatile noise and the stored blob matches the diff base.
//
// JCS is used deliberately instead of a hand-rolled "sort keys + json.Marshal":
// Go's encoder HTML-escapes &, <, > (common in mint URLs/descriptions) and has
// number-format edge cases, both of which silently produce a different byte
// string for an identical document — a false "change" on every such poll.
func (s Source) Canonicalize(raw []byte) ([]byte, error) {
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("mintinfo: decode document: %w", err)
	}
	for _, key := range s.VolatileKeys {
		delete(doc, key)
	}
	stripped, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("mintinfo: re-encode document: %w", err)
	}
	canonical, err := jcs.Transform(stripped)
	if err != nil {
		return nil, fmt.Errorf("mintinfo: canonicalize: %w", err)
	}
	return canonical, nil
}

// Hash is the content identity of a canonical document: hex sha256. Equal hashes
// mean equal config, so no new snapshot is stored.
func Hash(canonical []byte) string {
	sum := sha256.Sum256(canonical)
	return hex.EncodeToString(sum[:])
}

// Observation is one poll of one mint. Hash is "" when unreachable.
type Observation struct {
	MintURL   string
	CheckedAt time.Time
	Hash      string
	Changed   bool
	Reachable bool
}

// LastObservation is the poller's per-mint state, derived from the log: the last
// hash seen on a REACHABLE poll (the change basis) and the last check time of
// any poll (the due-gate clock).
type LastObservation struct {
	Hash      string
	CheckedAt time.Time
}

// Snapshot is a stored canonical document version.
type Snapshot struct {
	Hash        string
	Document    json.RawMessage
	FirstSeenAt time.Time
}
