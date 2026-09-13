package clickhouse

import (
	"github.com/vertex-lab/nagg/internal/dvm"
	"github.com/vertex-lab/nagg/internal/vertex"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/vertex-lab/nagg/internal/modules"
	"github.com/vertex-lab/nagg/internal/rules"
)

func mintModules(t *testing.T) modules.Set {
	t.Helper()
	set, err := modules.Parse("mint")
	if err != nil {
		t.Fatalf("parse mint modules: %v", err)
	}
	return set
}

// Every migration must declare its owning module on the first line. Without
// this, a new file lands in every deployment by accident — the mint-only
// ClickHouse quietly grows the whole Nostr app-view again.
func TestMigrationsDeclareAModule(t *testing.T) {
	names := migrationNames(nil)
	if len(names) == 0 {
		t.Fatal("no embedded migrations discovered")
	}
	for _, name := range names {
		owner, err := migrationModule(mustReadMigration(name))
		if err != nil {
			t.Errorf("%s: %v", name, err)
			continue
		}
		if owner == "" {
			t.Errorf("%s: empty module tag", name)
		}
	}
}

// The compatibility contract: a deployment that never mentions modules must see
// exactly the migration set it saw before modules existed. If this fails, the
// merge that introduced NAGG_MODULES is not behavior-preserving for production.
func TestAllModulesIsEveryMigration(t *testing.T) {
	nilSet := migrationNames(nil)
	all := migrationNames(modules.All())
	if len(nilSet) != len(all) {
		t.Fatalf("nil Set selected %d migrations, modules.All() selected %d", len(nilSet), len(all))
	}
	for i := range nilSet {
		if nilSet[i] != all[i] {
			t.Fatalf("migration %d differs: nil=%q all=%q", i, nilSet[i], all[i])
		}
	}
}

// A mint-only deployment's whole ClickHouse schema, pinned. Anything appearing
// here that isn't listed is a table the mint service would create and pay for.
func TestMintModuleDeclaresOnlyMintSchema(t *testing.T) {
	reg, err := rules.Mint()
	if err != nil {
		t.Fatalf("rules.Mint: %v", err)
	}
	desired, err := parseDesiredSchema(append(embeddedMigrations(mintModules(t)), reg.GeneratedDDL()...))
	if err != nil {
		t.Fatalf("parse mint schema: %v", err)
	}

	wantTables := []string{
		"event_seen_relays",
		"event_tags",
		"latest_k0",
		"mint_info_observations",
		"mint_info_snapshots",
		"nostr_events",
		"relay_backfill_state",
		"schema_migrations",
	}
	assertNames(t, "tables", desired.tables, wantTables)
	assertNames(t, "views", desired.views, []string{"mv_latest_k0"})
}

// The reconciler's drop pass must be bounded by what ANY module declares.
// Otherwise a mint-mode process pointed at a database holding Nostr data would
// see the entire app-view as undeclared and strip it — the same class of
// accident railway.staging.toml exists to prevent.
func TestReconcileNeverDropsAnotherModulesTables(t *testing.T) {
	mintReg, err := rules.Mint()
	if err != nil {
		t.Fatalf("rules.Mint: %v", err)
	}
	desired, err := parseDesiredSchema(append(embeddedMigrations(mintModules(t)), mintReg.GeneratedDDL()...))
	if err != nil {
		t.Fatalf("parse mint schema: %v", err)
	}
	anyModule, err := parseDesiredSchema(allModuleDDL(nil))
	if err != nil {
		t.Fatalf("parse all-module schema: %v", err)
	}

	// A database carrying the full app-view plus one genuinely dead table.
	actualTables := map[string]string{
		"nostr_events":        "ReplacingMergeTree",
		"mint_info_snapshots": "ReplacingMergeTree",
		"viewer_refs":         "MergeTree",
		"viewer_feed":         "ReplacingMergeTree",
		"pubkey_stats":        "ReplacingMergeTree",
		"rank_features":       "ReplacingMergeTree",
		"latest_k3":           "ReplacingMergeTree",
		"mv_latest_k3":        "MaterializedView",
		"agg_k7_e":            "AggregatingMergeTree",
		"event_embeddings":    "MergeTree", // declared by no module -> genuinely dead
	}

	plan := computeReconcilePlan(desired, anyModule, actualTables, map[string]map[string]struct{}{})

	if got, want := plan.dropTables, []string{"event_embeddings"}; !sameStrings(got, want) {
		t.Fatalf("dropTables = %v, want %v", got, want)
	}
	if len(plan.dropViews) != 0 {
		t.Fatalf("dropViews = %v, want none", plan.dropViews)
	}
}

// Without the all-module allow-list the drop pass keeps its original meaning:
// whatever the active schema omits is retired. This is what a full deployment
// (the only one that declares every module) still gets.
func TestReconcileStillDropsUndeclaredTablesForAFullDeployment(t *testing.T) {
	reg, err := rules.Default(20)
	if err != nil {
		t.Fatalf("rules.Default: %v", err)
	}
	desired, err := parseDesiredSchema(append(embeddedMigrations(modules.All()), reg.GeneratedDDL()...))
	if err != nil {
		t.Fatalf("parse full schema: %v", err)
	}
	anyModule, err := parseDesiredSchema(allModuleDDL(nil))
	if err != nil {
		t.Fatalf("parse all-module schema: %v", err)
	}
	actualTables := map[string]string{
		"nostr_events":     "ReplacingMergeTree",
		"event_embeddings": "MergeTree",
	}
	plan := computeReconcilePlan(desired, anyModule, actualTables, map[string]map[string]struct{}{})
	if got, want := plan.dropTables, []string{"event_embeddings"}; !sameStrings(got, want) {
		t.Fatalf("dropTables = %v, want %v", got, want)
	}
}

func assertNames[V any](t *testing.T, label string, got map[string]V, want []string) {
	t.Helper()
	names := make([]string, 0, len(got))
	for name := range got {
		names = append(names, name)
	}
	if !sameStrings(names, want) {
		sort.Strings(names)
		t.Errorf("%s = %v, want %v", label, names, want)
	}
}

func sameStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	x := append([]string(nil), a...)
	y := append([]string(nil), b...)
	sort.Strings(x)
	sort.Strings(y)
	for i := range x {
		if x[i] != y[i] {
			return false
		}
	}
	return true
}

func TestVertexAdditionPreservesMintSchema(t *testing.T) {
	plugins := dvm.MustRegistry(vertex.NewPlugin())
	before := &Store{modules: mintModules(t), rules: rules.MustMint(), dvm: plugins}
	mods, err := modules.Parse("mint,app,vertex")
	if err != nil {
		t.Fatal(err)
	}
	after := &Store{modules: mods, rules: rules.MustMint(), dvm: plugins}
	oldDDL := append(embeddedMigrations(before.modules), before.generatedDDL()...)
	newDDL := append(embeddedMigrations(after.modules), after.generatedDDL()...)
	if !reflect.DeepEqual(oldDDL, newDDL) {
		t.Fatal("adding app,vertex changed mint DDL")
	}
	for _, name := range []string{"vertex_scores", "vertex_profile_cache", "vertex_search_cache"} {
		count := 0
		for _, stmt := range newDDL {
			if strings.Contains(stmt, "CREATE TABLE IF NOT EXISTS "+name+"\n") {
				count++
			}
		}
		if count != 1 {
			t.Fatalf("%s declared %d times", name, count)
		}
	}
	desired, err := parseDesiredSchema(newDDL)
	if err != nil {
		t.Fatal(err)
	}
	all, err := parseDesiredSchema(allModuleDDL(plugins))
	if err != nil {
		t.Fatal(err)
	}
	tables := map[string]string{}
	columns := map[string]map[string]struct{}{}
	for table, cols := range desired.tables {
		tables[table] = "ReplacingMergeTree"
		columns[table] = map[string]struct{}{}
		for col := range cols {
			columns[table][col] = struct{}{}
		}
	}
	for view := range desired.views {
		tables[view] = "MaterializedView"
	}
	plan := computeReconcilePlan(desired, all, tables, columns)
	if len(plan.dropTables)+len(plan.dropViews)+len(plan.dropColumns)+len(plan.addColumns) != 0 {
		t.Fatalf("unexpected reconcile changes: %+v", plan)
	}
}
