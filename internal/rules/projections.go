package rules

import (
	"fmt"
	"strings"
)

// Projection declares a latest-event-per-author extraction: "for each pubkey,
// keep the newest event of Kinds, with these columns pulled out of it". It is
// the declarative form of the retired hand-written projections (the kind-0
// metadata table, the kind-3 reference-list table): the table, the
// materialized view feeding it, and the historical backfill are all generated
// from the declaration.
//
// The generated table is latest_<Name> — ReplacingMergeTree(created_at)
// keyed by pubkey with the implicit columns (pubkey, event_id, created_at)
// followed by the declared fields.
type Projection struct {
	// Name is kind-derived by convention ("k0", "k3"); the table becomes
	// latest_<Name>.
	Name   string
	Kinds  []int
	Fields []ProjField
}

// ProjField is one extracted column. Exactly one source must be set — the
// sources are a closed set, like extractors: new ones are added here when a
// real projection needs them, never speculatively.
type ProjField struct {
	// Name is the column name.
	Name string
	// JSONPath extracts JSONExtractString(content, JSONPath) — String.
	JSONPath string
	// RawContent stores the whole content — String.
	RawContent bool
	// TagKey collects the event's 64-hex values of this tag key —
	// Array(FixedString(64)).
	TagKey string
}

// ProjTableName returns the table owned by the named projection.
func ProjTableName(name string) string { return "latest_" + name }

// ProjViewName returns the materialized view feeding ProjTableName(name).
func ProjViewName(name string) string { return "mv_" + ProjTableName(name) }

// Projections returns the declared projections in declaration order.
func (r *Registry) Projections() []Projection { return r.projections }

func validateProjection(p Projection) error {
	if p.Name == "" {
		return fmt.Errorf("empty name")
	}
	if !validIdent(p.Name) {
		return fmt.Errorf("name must be a lowercase identifier ([a-z0-9_])")
	}
	if len(p.Kinds) == 0 {
		return fmt.Errorf("no kinds")
	}
	if len(p.Fields) == 0 {
		return fmt.Errorf("no fields")
	}
	seen := map[string]bool{}
	for _, f := range p.Fields {
		if !validIdent(f.Name) {
			return fmt.Errorf("field %q: name must be a lowercase identifier", f.Name)
		}
		switch f.Name {
		case "pubkey", "event_id", "created_at":
			return fmt.Errorf("field %q: reserved column name", f.Name)
		}
		if seen[f.Name] {
			return fmt.Errorf("field %q: duplicate", f.Name)
		}
		seen[f.Name] = true
		sources := 0
		if f.JSONPath != "" {
			sources++
		}
		if f.RawContent {
			sources++
		}
		if f.TagKey != "" {
			sources++
		}
		if sources != 1 {
			return fmt.Errorf("field %q: exactly one of JSONPath, RawContent, or TagKey must be set", f.Name)
		}
	}
	return nil
}

func projectionTableDDL(p Projection) string {
	var b strings.Builder
	fmt.Fprintf(&b, "CREATE TABLE IF NOT EXISTS %s\n(\n", ProjTableName(p.Name))
	b.WriteString("    pubkey FixedString(64),\n    event_id FixedString(64),\n    created_at DateTime")
	for _, f := range p.Fields {
		fmt.Fprintf(&b, ",\n    %s %s", f.Name, projFieldColumnType(f))
	}
	b.WriteString("\n)\nENGINE = ReplacingMergeTree(created_at)\nORDER BY pubkey;")
	return b.String()
}

func projectionViewDDL(p Projection) string {
	return fmt.Sprintf("CREATE MATERIALIZED VIEW IF NOT EXISTS %s\nTO %s\nAS\n%s;",
		ProjViewName(p.Name), ProjTableName(p.Name), projectionSelect(p, false))
}

// ProjectionBackfillSQL populates a projection from raw history — run when
// the table is first created, or during a full rebuild. Inserting every
// historical version is correct: ReplacingMergeTree(created_at) collapses to
// the newest row per pubkey.
func ProjectionBackfillSQL(p Projection) string {
	return fmt.Sprintf("INSERT INTO %s\n%s;", ProjTableName(p.Name), projectionSelect(p, true))
}

func projectionSelect(p Projection, backfill bool) string {
	var b strings.Builder
	b.WriteString("SELECT\n    pubkey,\n    id AS event_id,\n    created_at")
	for _, f := range p.Fields {
		fmt.Fprintf(&b, ",\n    %s AS %s", projFieldExpr(f), f.Name)
	}
	source := "nostr_events"
	if backfill {
		source = "nostr_events FINAL"
	}
	fmt.Fprintf(&b, "\nFROM %s\nWHERE kind IN (%s)", source, intList(p.Kinds))
	return b.String()
}

func projFieldColumnType(f ProjField) string {
	if f.TagKey != "" {
		return "Array(FixedString(64))"
	}
	return "String"
}

func projFieldExpr(f ProjField) string {
	switch {
	case f.JSONPath != "":
		return fmt.Sprintf("JSONExtractString(content, '%s')", escapeSQL(f.JSONPath))
	case f.RawContent:
		return "content"
	default:
		return fmt.Sprintf(`arrayMap(t -> t[2],
        arrayFilter(t -> length(t) >= 2 AND t[1] = '%s' AND length(t[2]) = 64,
            JSONExtract(tags_json, 'Array(Array(String))')))`, escapeSQL(f.TagKey))
	}
}
