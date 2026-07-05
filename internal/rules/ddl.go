package rules

import (
	"fmt"
	"sort"
	"strings"
)

// Generated DDL naming: a relationship named "k7_e" owns the aggregate table
// agg_k7_e and (for the ingest tier) the materialized view mv_agg_k7_e.

// TableName returns the aggregate table owned by the named relationship.
func TableName(ruleName string) string { return "agg_" + ruleName }

// ViewName returns the materialized view feeding TableName(ruleName).
func ViewName(ruleName string) string { return "mv_" + TableName(ruleName) }

// EventRefsDDL creates the generic event_refs table: the ingest-time landing
// zone for extractor-derived references (the generalization of the retired
// note_zaps table). One row per (rule, source event, target).
const EventRefsDDL = `CREATE TABLE IF NOT EXISTS event_refs
(
    rule LowCardinality(String),
    source_event_id FixedString(64),
    pubkey FixedString(64),
    created_at DateTime,
    target String,
    value UInt64
)
ENGINE = ReplacingMergeTree
PARTITION BY toYYYYMM(created_at)
ORDER BY (rule, target, source_event_id);`

// GeneratedDDL returns the CREATE statements for every declared relationship,
// in declaration order, prefixed by event_refs when any extractor rule needs
// it. The statements use IF NOT EXISTS and are safe to (re)apply at startup;
// the schema reconciler parses these same statements as the desired schema,
// so deleting a rule retires its table and view automatically.
func (r *Registry) GeneratedDDL() []string {
	var out []string
	if len(r.IngestExtractorRules()) > 0 {
		out = append(out, EventRefsDDL)
	}
	for _, rel := range r.relationships {
		out = append(out, aggregateTableDDL(rel))
		if rel.Refresh == RefreshIngest {
			out = append(out, materializedViewDDL(rel))
		}
	}
	for _, proj := range r.projections {
		out = append(out, projectionTableDDL(proj), projectionViewDDL(proj))
	}
	return out
}

// BackfillSQL returns the INSERT statement that populates a tag-ref or
// author relationship's aggregate table from historical rows — the
// "prototype it in GraphQL, then declare it" flow applied to data that
// predates the declaration. Extractor-based rules return ok == false: their
// history must be replayed through the extractor in Go (see the store's
// generic refs backfill).
func BackfillSQL(rel Relationship) (string, bool) {
	if rel.Ref.Extractor != "" {
		return "", false
	}
	return fmt.Sprintf("INSERT INTO %s\n%s;", TableName(rel.Name), ruleSelect(rel)), true
}

// ReadSpec tells readers how to fetch one finalized metric value: SELECT
// <MergeExpr> FROM <Table> WHERE target IN (...) GROUP BY target.
type ReadSpec struct {
	Table     string
	Column    string
	MergeFunc string // aggregate-merge combinator: uniqMerge or sumMerge
}

// ReadSpec resolves a (relationship, metric) pair, or ok == false.
func (r *Registry) ReadSpec(ruleName, metricName string) (ReadSpec, bool) {
	rel := r.byName[ruleName]
	if rel == nil {
		return ReadSpec{}, false
	}
	for _, m := range rel.Metrics {
		if m.Name != metricName {
			continue
		}
		merge := "uniqMerge"
		if m.Agg == AggSumValue {
			merge = "sumMerge"
		}
		return ReadSpec{Table: TableName(rel.Name), Column: m.Name, MergeFunc: merge}, true
	}
	return ReadSpec{}, false
}

func aggregateTableDDL(rel Relationship) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s\n(\n", TableName(rel.Name))
	fmt.Fprintf(&b, "    target %s", targetColumnType(rel.Ref.Target))
	for _, m := range rel.Metrics {
		fmt.Fprintf(&b, ",\n    %s %s", m.Name, metricColumnType(m.Agg))
	}
	b.WriteString("\n)\nENGINE = AggregatingMergeTree\nORDER BY target;")
	return b.String()
}

func materializedViewDDL(rel Relationship) string {
	return fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s\nTO %s\nAS\n%s;",
		ViewName(rel.Name), TableName(rel.Name), ruleSelect(rel))
}

// ruleSelect builds the aggregating SELECT shared by a rule's materialized
// view and its historical backfill.
func ruleSelect(rel Relationship) string {
	switch {
	case rel.Ref.TagKey != "":
		return tagRefSelect(rel)
	case rel.Ref.Author:
		return authorSelect(rel)
	default:
		return extractorRefSelect(rel)
	}
}

func authorSelect(rel Relationship) string {
	var b strings.Builder
	b.WriteString("SELECT\n    pubkey AS target")
	for _, m := range rel.Metrics {
		fmt.Fprintf(&b, ",\n    uniqState(id) AS %s", m.Name)
	}
	fmt.Fprintf(&b, "\nFROM nostr_events\nWHERE kind IN (%s)\nGROUP BY target", intList(rel.Kinds))
	return b.String()
}

// tagRefSelect builds the aggregating SELECT over event_tags shared by the
// materialized view and the historical backfill.
func tagRefSelect(rel Relationship) string {
	var b strings.Builder
	b.WriteString("SELECT\n    tag_value AS target")
	for _, m := range rel.Metrics {
		fmt.Fprintf(&b, ",\n    %s AS %s", tagMetricState(m.Agg), m.Name)
	}
	fmt.Fprintf(&b, "\nFROM event_tags\nWHERE kind IN (%s) AND tag_key = '%s' AND %s",
		intList(rel.Kinds), escapeSQL(rel.Ref.TagKey), targetPredicate(rel.Ref.Target, "tag_value"))
	if rel.Ref.Marker != "" {
		fmt.Fprintf(&b, " AND arrayElement(tag_extra, 2) = '%s'", escapeSQL(rel.Ref.Marker))
	}
	b.WriteString("\nGROUP BY target")
	return b.String()
}

func extractorRefSelect(rel Relationship) string {
	var b strings.Builder
	b.WriteString("SELECT\n    target")
	for _, m := range rel.Metrics {
		fmt.Fprintf(&b, ",\n    %s AS %s", refsMetricState(m.Agg), m.Name)
	}
	fmt.Fprintf(&b, "\nFROM event_refs\nWHERE rule = '%s'\nGROUP BY target", escapeSQL(rel.Name))
	return b.String()
}

func targetColumnType(t TargetType) string {
	if t == TargetAddress {
		return "String"
	}
	return "FixedString(64)"
}

func metricColumnType(a Agg) string {
	if a == AggSumValue {
		return "AggregateFunction(sum, UInt64)"
	}
	return "AggregateFunction(uniq, FixedString(64))"
}

func tagMetricState(a Agg) string {
	switch a {
	case AggUniqActors:
		return "uniqState(pubkey)"
	case AggUniqSources:
		return "uniqState(event_id)"
	default:
		// Validation rejects sum_value on tag refs.
		panic(fmt.Sprintf("rules: no tag-ref state for agg %q", a))
	}
}

func refsMetricState(a Agg) string {
	switch a {
	case AggUniqActors:
		return "uniqState(pubkey)"
	case AggUniqSources:
		return "uniqState(source_event_id)"
	case AggSumValue:
		return "sumState(value)"
	default:
		panic(fmt.Sprintf("rules: no refs state for agg %q", a))
	}
}

func targetPredicate(t TargetType, column string) string {
	switch t {
	case TargetAddress:
		return fmt.Sprintf("match(%s, '^[0-9]+:[0-9a-f]{64}:')", column)
	default:
		return fmt.Sprintf("length(%s) = 64", column)
	}
}

func intList(kinds []int) string {
	ks := append([]int(nil), kinds...)
	sort.Ints(ks)
	parts := make([]string, len(ks))
	for i, k := range ks {
		parts[i] = fmt.Sprintf("%d", k)
	}
	return strings.Join(parts, ", ")
}

func escapeSQL(s string) string { return strings.ReplaceAll(s, "'", "\\'") }
