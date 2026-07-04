package rules

import (
	"fmt"
	"strings"
	"time"
)

// The lifetime policies below are the kind-neutral generalization of the
// retention predicates that previously lived in internal/clickhouse: the
// same SQL shapes, parameterized by kinds and by declared relationships
// instead of hard-coded engagement tables.

// KeepLatestPerAuthor deletes every event whose (kind, author) has a newer
// version — the NIP-01 replaceable-event semantics. Ties on created_at keep
// one arbitrary winner, matching relay behavior.
type KeepLatestPerAuthor struct{}

func (KeepLatestPerAuthor) DeletePredicate(kinds []int, idColumn string) string {
	return fmt.Sprintf(`kind IN (%s) AND %s NOT IN (
		SELECT argMax(id, created_at)
		FROM nostr_events
		WHERE kind IN (%s)
		GROUP BY kind, pubkey
	)`, intList(kinds), idColumn, intList(kinds))
}

func (KeepLatestPerAuthor) Describe() string { return "keep only the latest event per author" }

// KeepLatestPerAuthorDTag is KeepLatestPerAuthor keyed by (kind, author,
// d-tag) — the NIP-01 parameterized-replaceable semantics. Events without a
// d tag group under the empty key, which is exactly NIP-01's treatment.
type KeepLatestPerAuthorDTag struct{}

func (KeepLatestPerAuthorDTag) DeletePredicate(kinds []int, idColumn string) string {
	return fmt.Sprintf(`kind IN (%s) AND %s NOT IN (
		SELECT argMax(id, created_at)
		FROM nostr_events
		WHERE kind IN (%s)
		GROUP BY kind, pubkey,
			arrayFirst(t -> length(t) >= 2 AND t[1] = 'd', JSONExtract(tags_json, 'Array(Array(String))'))
	)`, intList(kinds), idColumn, intList(kinds))
}

func (KeepLatestPerAuthorDTag) Describe() string {
	return "keep only the latest event per (author, d-tag)"
}

// MaxAgeUnlessReferenced deletes events older than Age that no event of
// another kind ever referenced, where "referenced" is defined by the named
// relationships: an event id appearing as a target in any of their aggregate
// tables is protected. Aggregate tables outlive the referencing events, so
// pruning a referencing event never un-references its target.
type MaxAgeUnlessReferenced struct {
	Age time.Duration
	// ByRules names the relationships whose targets are protected. All must
	// target events (TargetEventID); Registry validation enforces existence.
	ByRules []string
}

func (p MaxAgeUnlessReferenced) DeletePredicate(kinds []int, idColumn string) string {
	seconds := int64(p.Age / time.Second)
	var ledger strings.Builder
	for i, name := range p.ByRules {
		if i > 0 {
			ledger.WriteString("\n\t\tUNION ALL ")
		} else {
			ledger.WriteString("\t\t")
		}
		fmt.Fprintf(&ledger, "SELECT target AS ref_target FROM %s", TableName(name))
	}
	return fmt.Sprintf(`kind IN (%s)
	AND created_at < now() - INTERVAL %d SECOND
	AND %s NOT IN (
%s
	)`, intList(kinds), seconds, idColumn, ledger.String())
}

func (p MaxAgeUnlessReferenced) Describe() string {
	return fmt.Sprintf("drop after %s without any referencing event", p.Age)
}

// MaxAge unconditionally deletes events older than Age.
type MaxAge struct {
	Age time.Duration
}

func (p MaxAge) DeletePredicate(kinds []int, idColumn string) string {
	return fmt.Sprintf("kind IN (%s) AND created_at < now() - INTERVAL %d SECOND",
		intList(kinds), int64(p.Age/time.Second))
}

func (p MaxAge) Describe() string { return fmt.Sprintf("drop after %s", p.Age) }
