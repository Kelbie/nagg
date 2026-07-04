package rules

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/nbd-wtf/go-nostr"
)

// ExtractedRef is one reference derived from an event by an extractor,
// destined for the generic event_refs table. Value carries the metric input
// for AggSumValue rules (0 when the relationship only counts).
type ExtractedRef struct {
	Target string
	Value  uint64
}

// ExtractorFunc derives the references a plain tag match cannot express.
// Returning an empty slice means the event yields no references for the rule.
type ExtractorFunc func(event *nostr.Event) []ExtractedRef

// extractors is the closed registry of named extraction primitives. Rules
// reference these by name, keeping the rule set inspectable data while the
// irregular parsing lives here, once. Add an entry only when a real rule
// needs it — never speculatively.
var extractors = map[string]ExtractorFunc{
	// zap_target resolves a NIP-57 zap receipt (kind 9735) to the event it
	// zapped and the paid amount in sats. The target is the receipt's e tag,
	// falling back to the e tag of the zap request JSON nested in the
	// description tag; the amount comes from the request's amount tag
	// (millisats) or, failing that, from parsing the bolt11 invoice.
	"zap_target": extractZapTarget,
}

// Extractor returns the named extractor, or nil.
func Extractor(name string) ExtractorFunc { return extractors[name] }

func extractZapTarget(event *nostr.Event) []ExtractedRef {
	targetID := firstHexTag(event.Tags, "e")
	description := firstTag(event.Tags, "description")
	if targetID == "" && description != "" {
		targetID = zapRequestFirstHexTag(description, "e")
	}
	if targetID == "" {
		return nil
	}

	msats := zapRequestAmountMSats(description)
	var sats uint64
	if msats > 0 {
		sats = msats / 1000
	} else if bolt11 := firstTag(event.Tags, "bolt11"); bolt11 != "" {
		sats = satsFromBolt11(bolt11)
	}
	return []ExtractedRef{{Target: targetID, Value: sats}}
}

func zapRequestFirstHexTag(raw, key string) string {
	tags, ok := zapRequestTags(raw)
	if !ok {
		return ""
	}
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func zapRequestAmountMSats(raw string) uint64 {
	tags, ok := zapRequestTags(raw)
	if !ok {
		return 0
	}
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == "amount" {
			n, err := strconv.ParseUint(tag[1], 10, 64)
			if err != nil {
				return 0
			}
			return n
		}
	}
	return 0
}

func zapRequestTags(raw string) ([][]string, bool) {
	if raw == "" {
		return nil, false
	}
	var req struct {
		Tags [][]string `json:"tags"`
	}
	if err := json.Unmarshal([]byte(raw), &req); err != nil {
		return nil, false
	}
	return req.Tags, true
}

// satsFromBolt11 reads the amount encoded in a bolt11 invoice's human-
// readable part. Zero means no or unparseable amount.
func satsFromBolt11(invoice string) uint64 {
	invoice = strings.ToLower(strings.TrimSpace(invoice))
	if !strings.HasPrefix(invoice, "lnbc") {
		return 0
	}
	sep := strings.LastIndexByte(invoice, '1')
	if sep <= len("lnbc") {
		return 0
	}
	amount := invoice[len("lnbc"):sep]
	if amount == "" {
		return 0
	}

	unit := byte(0)
	last := amount[len(amount)-1]
	if last < '0' || last > '9' {
		unit = last
		amount = amount[:len(amount)-1]
	}
	n, err := strconv.ParseUint(amount, 10, 64)
	if err != nil {
		return 0
	}

	switch unit {
	case 0:
		return n * 100_000_000
	case 'm':
		return n * 100_000
	case 'u':
		return n * 100
	case 'n':
		return n / 10
	case 'p':
		return n / 10_000
	default:
		return 0
	}
}

func firstHexTag(tags nostr.Tags, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key && len(tag[1]) == 64 {
			return tag[1]
		}
	}
	return ""
}

func firstTag(tags nostr.Tags, key string) string {
	for _, tag := range tags {
		if len(tag) >= 2 && tag[0] == key {
			return tag[1]
		}
	}
	return ""
}
