package clickhouse

import (
	"sort"
	"strings"
	"testing"

	"github.com/vertex-lab/nagg/internal/rules"
	"github.com/vertex-lab/nagg/internal/vertex"
)

func TestParseDesiredSchema_RealMigrations(t *testing.T) {
	// Static SQL + registry-generated DDL together form the desired schema —
	// the same composition reconcileSchema uses.
	reg, err := rules.Default(20)
	if err != nil {
		t.Fatalf("rules.Default: %v", err)
	}
	generated := append(reg.GeneratedDDL(), vertex.NewPlugin().CacheDDL()...)
	desired, err := parseDesiredSchema(append(embeddedMigrations(), generated...))
	if err != nil {
		t.Fatalf("parseDesiredSchema returned error: %v", err)
	}

	wantTables := []string{
		// static SQL
		"schema_migrations",
		"viewer_feed",
		"nostr_events",
		"event_seen_relays",
		"event_tags",
		"ref_edges",
		"gated_ref_counts",
		"rollup_state",
		"latest_k3",
		"pubkey_stats",
		"rank_features",
		"latest_k0",
		"derived_tags",
		"derived_metrics",
		"enrichment_state",
		"viewer_refs",
		"known_viewers",
		"relay_backfill_state",
		"mint_info_snapshots",
		"mint_info_observations",
		// registry-generated
		"vertex_scores",
		"vertex_profile_cache",
		"vertex_search_cache",
		"event_refs",
		"agg_k7_e",
		"agg_k6_16_e",
		"agg_k1_q",
		"agg_k9735_e",
		"agg_k1_1111_e_reply",
		"agg_k1_1111_author",
	}
	if len(desired.tables) != len(wantTables) {
		t.Fatalf("parsed %d tables, want %d: %v", len(desired.tables), len(wantTables), tableNames(desired))
	}
	for _, name := range wantTables {
		if _, ok := desired.tables[name]; !ok {
			t.Errorf("expected table %q to be parsed; got %v", name, tableNames(desired))
		}
	}

	wantViews := []string{
		// static SQL
		"mv_latest_k0",
		"mv_viewer_refs",
		"mv_latest_k3",
		// registry-generated (periodic k1_1111_e_reply has no view)
		"mv_agg_k7_e",
		"mv_agg_k6_16_e",
		"mv_agg_k1_q",
		"mv_agg_k9735_e",
		"mv_agg_k1_1111_author",
	}
	if len(desired.views) != len(wantViews) {
		t.Fatalf("parsed %d views, want %d: %v", len(desired.views), len(wantViews), viewNames(desired))
	}
	for _, name := range wantViews {
		if _, ok := desired.views[name]; !ok {
			t.Errorf("expected view %q to be parsed; got %v", name, viewNames(desired))
		}
	}

	// Representative column set for the protected raw events table.
	wantNostrCols := []string{
		"id", "pubkey", "created_at", "kind", "tags_json",
		"content", "sig", "first_seen_at", "last_seen_at",
	}
	nostrCols := desired.tables["nostr_events"]
	if len(nostrCols) != len(wantNostrCols) {
		t.Fatalf("nostr_events has %d columns, want %d: %v", len(nostrCols), len(wantNostrCols), colNames(nostrCols))
	}
	for _, col := range wantNostrCols {
		if _, ok := nostrCols[col]; !ok {
			t.Errorf("expected nostr_events column %q; got %v", col, colNames(nostrCols))
		}
	}

	// DEFAULT modifiers must be preserved in the definition.
	if def := nostrCols["first_seen_at"]; !strings.Contains(def, "DEFAULT now()") {
		t.Errorf("first_seen_at definition lost its DEFAULT: %q", def)
	}

	// viewer_refs.ingested_at is what the viewer_feed rollup windows on; if it
	// falls out of the desired schema the reconciler drops the live column and
	// the read-model stops filling. Its DEFAULT keeps rows inserted without it
	// (seed/backfill re-derivations) on event time, the best arrival estimate.
	if def := desired.tables["viewer_refs"]["ingested_at"]; !strings.Contains(def, "DEFAULT created_at") {
		t.Errorf("viewer_refs.ingested_at missing or lost its DEFAULT: %q", def)
	}

	// Paren-nested AggregateFunction type must be kept whole in the definition
	// (generated DDL exercises the same parser paths as static SQL).
	actorCols := desired.tables["agg_k7_e"]
	actorsDef, ok := actorCols["actors"]
	if !ok {
		t.Fatalf("agg_k7_e is missing the 'actors' column: %v", colNames(actorCols))
	}
	if !strings.Contains(actorsDef, "AggregateFunction(uniq, FixedString(64))") {
		t.Errorf("actors column lost its nested type; got definition %q", actorsDef)
	}

	// AggregateFunction(sum, UInt64) must also survive (multiple AggregateFunctions in one table).
	zapAgg := desired.tables["agg_k9735_e"]
	if def := zapAgg["value_total"]; !strings.Contains(def, "AggregateFunction(sum, UInt64)") {
		t.Errorf("agg_k9735_e.value_total lost its nested type; got %q", def)
	}

	// Array(String) and a default string literal must survive.
	tagCols := desired.tables["event_tags"]
	if def := tagCols["tag_extra"]; !strings.Contains(def, "Array(String)") {
		t.Errorf("event_tags.tag_extra lost Array(String); got %q", def)
	}
	scoreCols := desired.tables["vertex_scores"]
	if def := scoreCols["source"]; !strings.Contains(def, "DEFAULT 'vertex'") {
		t.Errorf("vertex_scores.source lost its DEFAULT 'vertex'; got %q", def)
	}

	// Nullable(Float64) must survive.
	searchCols := desired.tables["vertex_search_cache"]
	if def := searchCols["rank"]; !strings.Contains(def, "Nullable(Float64)") {
		t.Errorf("vertex_search_cache.rank lost Nullable(Float64); got %q", def)
	}
}

func TestComputeReconcilePlan_DropsExtrasAddsMissing(t *testing.T) {
	desired := DesiredSchema{
		tables: map[string]map[string]string{
			"note_like_counts": {
				"target_event_id": "target_event_id FixedString(64)",
				"likes":           "likes AggregateFunction(uniq, FixedString(64))",
			},
			"latest_k0": {
				"pubkey": "pubkey FixedString(64)",
				"name":   "name String",
			},
		},
		views: map[string]struct{}{
			"mv_note_like_counts": {},
		},
	}

	actualTables := map[string]string{
		"note_like_counts":    "AggregatingMergeTree",
		"latest_k0":           "ReplacingMergeTree",
		"event_embeddings":    "MergeTree",        // dead table -> drop
		"mv_note_like_counts": "MaterializedView", // declared view -> keep
		"mv_dead_view":        "MaterializedView", // dead view -> drop
	}
	actualColumns := map[string]map[string]struct{}{
		"note_like_counts": {
			"target_event_id": {},
			"likes":           {},
		},
		"latest_k0": {
			"pubkey":     {},
			"old_column": {}, // not desired -> drop
			// "name" missing -> add
		},
		"event_embeddings": {"id": {}},
	}

	plan := computeReconcilePlan(desired, actualTables, actualColumns)

	if got := names(plan.dropTables); !equalSet(got, []string{"event_embeddings"}) {
		t.Errorf("dropTables = %v, want [event_embeddings]", got)
	}
	if got := names(plan.dropViews); !equalSet(got, []string{"mv_dead_view"}) {
		t.Errorf("dropViews = %v, want [mv_dead_view]", got)
	}
	if len(plan.dropColumns) != 1 || plan.dropColumns[0].table != "latest_k0" || plan.dropColumns[0].column != "old_column" {
		t.Errorf("dropColumns = %+v, want [{latest_k0 old_column}]", plan.dropColumns)
	}
	if len(plan.addColumns) != 1 || plan.addColumns[0].table != "latest_k0" || plan.addColumns[0].column != "name" {
		t.Errorf("addColumns = %+v, want [{latest_k0 name ...}]", plan.addColumns)
	}
	if def := plan.addColumns[0].definition; def != "name String" {
		t.Errorf("addColumns definition = %q, want %q", def, "name String")
	}
}

func TestComputeReconcilePlan_NeverDropsClickHouseInternalTables(t *testing.T) {
	desired := DesiredSchema{
		tables: map[string]map[string]string{"note_like_counts": {"target_event_id": "target_event_id FixedString(64)"}},
		views:  map[string]struct{}{},
	}
	// A ClickHouse-managed inner table (e.g. a TO-less MV's storage) is never
	// declared — the reconciler must NOT plan to drop it.
	actualTables := map[string]string{
		"note_like_counts":  "AggregatingMergeTree",
		".inner_id.abc-123": "AggregatingMergeTree",
		".inner.some_view":  "MaterializedView",
	}
	actualColumns := map[string]map[string]struct{}{"note_like_counts": {"target_event_id": {}}}

	plan := computeReconcilePlan(desired, actualTables, actualColumns)
	if len(plan.dropTables) != 0 || len(plan.dropViews) != 0 {
		t.Errorf("internal .-prefixed tables must never be dropped; got dropTables=%v dropViews=%v", plan.dropTables, plan.dropViews)
	}
}

func TestComputeReconcilePlan_IdenticalTableNoChanges(t *testing.T) {
	desired := DesiredSchema{
		tables: map[string]map[string]string{
			"note_zaps": {
				"zap_receipt_id":  "zap_receipt_id FixedString(64)",
				"target_event_id": "target_event_id FixedString(64)",
			},
		},
		views: map[string]struct{}{},
	}
	actualTables := map[string]string{"note_zaps": "ReplacingMergeTree"}
	actualColumns := map[string]map[string]struct{}{
		"note_zaps": {
			"zap_receipt_id":  {},
			"target_event_id": {},
		},
	}

	plan := computeReconcilePlan(desired, actualTables, actualColumns)
	if len(plan.dropTables) != 0 || len(plan.dropViews) != 0 || len(plan.dropColumns) != 0 || len(plan.addColumns) != 0 {
		t.Errorf("expected empty plan for identical schema, got %+v", plan)
	}
}

func TestComputeReconcilePlan_DoesNotDropMissingTable(t *testing.T) {
	// A desired table that doesn't exist in actual should not be planned for any
	// column edits (CREATE handles creating it; reconcile shouldn't touch it).
	desired := DesiredSchema{
		tables: map[string]map[string]string{
			"brand_new_table": {"id": "id String"},
		},
		views: map[string]struct{}{},
	}
	plan := computeReconcilePlan(desired, map[string]string{}, map[string]map[string]struct{}{})
	if len(plan.dropTables) != 0 || len(plan.addColumns) != 0 || len(plan.dropColumns) != 0 {
		t.Errorf("expected empty plan when desired table absent from actual, got %+v", plan)
	}
}

func TestGuardProtectedTables_NeverDropsProtected(t *testing.T) {
	plan := ReconcilePlan{
		dropTables: []string{"nostr_events", "event_tags", "event_seen_relays", "some_dead_table"},
		dropColumns: []columnRef{
			{table: "nostr_events", column: "sig"},
			{table: "event_tags", column: "tag_value"},
			{table: "some_dead_table", column: "junk"},
		},
	}

	guarded := guardProtectedTables(plan)

	if got := names(guarded.dropTables); !equalSet(got, []string{"some_dead_table"}) {
		t.Errorf("guarded dropTables = %v, want only [some_dead_table]", got)
	}
	for _, t2 := range guarded.dropTables {
		if _, ok := protectedTables[t2]; ok {
			t.Errorf("protected table %q survived into executed drop list", t2)
		}
	}

	if len(guarded.dropColumns) != 1 || guarded.dropColumns[0].table != "some_dead_table" {
		t.Errorf("guarded dropColumns = %+v, want only [{some_dead_table junk}]", guarded.dropColumns)
	}
	for _, c := range guarded.dropColumns {
		if _, ok := protectedTables[c.table]; ok {
			t.Errorf("column drop on protected table %q survived into executed list", c.table)
		}
	}
}

func TestSchemaReconcileMode_Default(t *testing.T) {
	t.Setenv("NAGG_SCHEMA_RECONCILE", "")
	if got := schemaReconcileMode(); got != "on" {
		t.Errorf("default mode = %q, want on", got)
	}
	t.Setenv("NAGG_SCHEMA_RECONCILE", "OFF")
	if got := schemaReconcileMode(); got != "off" {
		t.Errorf("mode OFF = %q, want off", got)
	}
	t.Setenv("NAGG_SCHEMA_RECONCILE", "dry-run")
	if got := schemaReconcileMode(); got != "dry-run" {
		t.Errorf("mode dry-run = %q, want dry-run", got)
	}
	t.Setenv("NAGG_SCHEMA_RECONCILE", "garbage")
	if got := schemaReconcileMode(); got != "on" {
		t.Errorf("unknown mode = %q, want on (default)", got)
	}
}

// TestReplyParityMigration_IncludesKind1111 guards the reply-count parity fix:
// the reply MV must aggregate NIP-22 comments (kind 1111) alongside kind 1, and
// the migration must DROP the old view first so the change applies on existing
// deployments (CREATE ... IF NOT EXISTS alone is a no-op there). It must also
// backfill historical replies.
// TestDirectRepliesMigration_DeclaresEdgeTable guards the NIP-10/22
// direct-reply edge rebuild: migration 007 must declare the edge table and
// derive the parent with the reply > unmarked-last > root coalesce, excluding
// quotes. The direct-reply COUNT aggregate moved to the rules registry
// (agg_k1_1111_e_reply) and must NOT be declared here anymore.
func TestDirectRepliesMigration_DeclaresEdgeTable(t *testing.T) {
	sql := mustReadMigration("007_direct_replies.sql")
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS ref_edges",
		"argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'reply')",
		"argMaxIf(tag_value, tag_index, tag_key = 'e' AND marker = '')",
		"argMinIf(tag_value, tag_index, tag_key = 'e' AND marker = 'root')",
		"has(quote_targets, target_id)",
		"kind IN (1, 1111)",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("007 direct-replies migration missing %q", want)
		}
	}

	desired, err := parseDesiredSchema(embeddedMigrations())
	if err != nil {
		t.Fatalf("parseDesiredSchema returned error: %v", err)
	}
	if _, ok := desired.tables["ref_edges"]; !ok {
		t.Error("expected table ref_edges declared by 007")
	}
	if _, ok := desired.tables["note_direct_reply_counts"]; ok {
		t.Error("note_direct_reply_counts must no longer be declared by static SQL")
	}

	if !migrationFilePresent("007_direct_replies.sql") {
		t.Error("007_direct_replies.sql is not discovered by embeddedMigrations()")
	}
}

// --- helpers ---

func tableNames(d DesiredSchema) []string {
	out := make([]string, 0, len(d.tables))
	for k := range d.tables {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func viewNames(d DesiredSchema) []string {
	out := make([]string, 0, len(d.views))
	for k := range d.views {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func colNames(cols map[string]string) []string {
	out := make([]string, 0, len(cols))
	for k := range cols {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func names(s []string) []string {
	out := append([]string(nil), s...)
	sort.Strings(out)
	return out
}

func equalSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	a = names(a)
	b = names(b)
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
