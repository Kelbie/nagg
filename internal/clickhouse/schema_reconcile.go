package clickhouse

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// protectedTables holds the raw ingested data tables. Everything else in the
// schema is rebuildable from these, so the reconciler must NEVER drop them (or
// their columns), even if the parsed schema somehow fails to declare them.
var protectedTables = map[string]struct{}{
	"nostr_events":      {},
	"event_tags":        {},
	"event_seen_relays": {},
}

// DesiredSchema is the schema as declared by the embedded migration SQL files.
// It is the single declarative source of truth that the reconciler enforces.
type DesiredSchema struct {
	// tables maps table name -> (column name -> full column definition).
	tables map[string]map[string]string
	// views holds the names of declared materialized views. MVs are defined by
	// their SELECT, so we don't parse columns for them.
	views map[string]struct{}
}

// ReconcilePlan is the set of mutations required to make the actual ClickHouse
// schema match the DesiredSchema. It is computed by a pure function so it can be
// unit-tested without a database.
type ReconcilePlan struct {
	dropTables  []string
	dropViews   []string
	dropColumns []columnRef
	addColumns  []columnAdd
}

type columnRef struct {
	table  string
	column string
}

type columnAdd struct {
	table      string
	column     string
	definition string
}

// schemaReconcileMode reads the reconcile mode from the environment.
// Values: "off" (skip entirely), "dry-run" (compute + log, execute nothing),
// "on" (execute). Default is "on".
func schemaReconcileMode() string {
	mode := strings.ToLower(strings.TrimSpace(os.Getenv("NAGG_SCHEMA_RECONCILE")))
	switch mode {
	case "off", "dry-run", "on":
		return mode
	default:
		return "on"
	}
}

// embeddedMigrations returns the declarative migration SQL in apply order.
func embeddedMigrations() []string {
	return []string{ingestionMigration, appviewMigration, rankingMigration, derivedMigration, replyParityMigration, notificationsMigration}
}

// parseDesiredSchema parses the embedded migration SQL into a DesiredSchema.
// It understands CREATE TABLE column blocks (paren-aware, so nested types such
// as AggregateFunction(uniq, FixedString(64)) stay intact) and CREATE
// MATERIALIZED VIEW names.
func parseDesiredSchema(migrations []string) (DesiredSchema, error) {
	desired := DesiredSchema{
		tables: map[string]map[string]string{},
		views:  map[string]struct{}{},
	}
	for _, migration := range migrations {
		for _, stmt := range splitSQLStatements(migration) {
			if err := parseStatement(stmt, &desired); err != nil {
				return DesiredSchema{}, err
			}
		}
	}
	return desired, nil
}

func parseStatement(stmt string, desired *DesiredSchema) error {
	upper := strings.ToUpper(stmt)
	switch {
	case strings.HasPrefix(upper, "CREATE MATERIALIZED VIEW"):
		name, ok := parseCreateViewName(stmt)
		if !ok {
			return nil
		}
		desired.views[name] = struct{}{}
	case strings.HasPrefix(upper, "CREATE TABLE"):
		name, columns, err := parseCreateTable(stmt)
		if err != nil {
			return err
		}
		if name == "" {
			return nil
		}
		desired.tables[name] = columns
	}
	return nil
}

// parseCreateViewName extracts the MV name from a
// "CREATE MATERIALIZED VIEW [IF NOT EXISTS] <name> TO ..." statement.
func parseCreateViewName(stmt string) (string, bool) {
	rest := stripPrefixFold(stmt, "CREATE MATERIALIZED VIEW")
	rest = stripPrefixFold(strings.TrimSpace(rest), "IF NOT EXISTS")
	rest = strings.TrimSpace(rest)
	name := firstIdentifier(rest)
	if name == "" {
		return "", false
	}
	return name, true
}

// parseCreateTable extracts the table name and its column definitions from a
// "CREATE TABLE [IF NOT EXISTS] <name> ( <columns> ) ENGINE ..." statement.
func parseCreateTable(stmt string) (string, map[string]string, error) {
	rest := stripPrefixFold(stmt, "CREATE TABLE")
	rest = stripPrefixFold(strings.TrimSpace(rest), "IF NOT EXISTS")
	rest = strings.TrimSpace(rest)
	name := firstIdentifier(rest)
	if name == "" {
		return "", nil, nil
	}

	open := strings.IndexByte(rest, '(')
	if open < 0 {
		return "", nil, fmt.Errorf("parse table %q: no column block", name)
	}
	block, ok := extractMatchingParen(rest, open)
	if !ok {
		return "", nil, fmt.Errorf("parse table %q: unbalanced parentheses in column block", name)
	}

	columns := map[string]string{}
	for _, entry := range splitTopLevelCommas(block) {
		entry = strings.TrimSpace(entry)
		if entry == "" {
			continue
		}
		if isTableLevelClause(entry) {
			continue
		}
		colName := firstWhitespaceToken(entry)
		if colName == "" {
			continue
		}
		columns[stripBackticks(colName)] = entry
	}
	return name, columns, nil
}

// extractMatchingParen returns the content between the '(' at openIdx and its
// matching ')', paren-depth aware. The bool reports whether a match was found.
func extractMatchingParen(s string, openIdx int) (string, bool) {
	depth := 0
	for i := openIdx; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return s[openIdx+1 : i], true
			}
		}
	}
	return "", false
}

// splitTopLevelCommas splits s on commas that are not nested inside parentheses,
// so "a Int, b AggregateFunction(uniq, FixedString(64))" yields two entries.
func splitTopLevelCommas(s string) []string {
	var parts []string
	depth := 0
	start := 0
	for i := 0; i < len(s); i++ {
		switch s[i] {
		case '(':
			depth++
		case ')':
			depth--
		case ',':
			if depth == 0 {
				parts = append(parts, s[start:i])
				start = i + 1
			}
		}
	}
	parts = append(parts, s[start:])
	return parts
}

// isTableLevelClause reports whether a column-block entry is actually a
// table-level clause (INDEX/PRIMARY KEY/CONSTRAINT/PROJECTION) rather than a
// column definition.
func isTableLevelClause(entry string) bool {
	upper := strings.ToUpper(strings.TrimSpace(entry))
	for _, kw := range []string{"INDEX ", "PRIMARY KEY", "CONSTRAINT ", "PROJECTION "} {
		if strings.HasPrefix(upper, kw) {
			return true
		}
	}
	return false
}

// firstIdentifier returns the first whitespace/paren-delimited identifier in s,
// stripping surrounding backticks.
func firstIdentifier(s string) string {
	s = strings.TrimSpace(s)
	end := len(s)
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(' {
			end = i
			break
		}
	}
	return stripBackticks(s[:end])
}

// firstWhitespaceToken returns the first whitespace-delimited token in s.
func firstWhitespaceToken(s string) string {
	s = strings.TrimSpace(s)
	return strings.FieldsFunc(s, func(r rune) bool {
		return r == ' ' || r == '\t' || r == '\n' || r == '\r'
	})[0]
}

func stripBackticks(s string) string {
	return strings.Trim(strings.TrimSpace(s), "`")
}

// stripPrefixFold removes a case-insensitive prefix from s if present.
func stripPrefixFold(s, prefix string) string {
	if len(s) >= len(prefix) && strings.EqualFold(s[:len(prefix)], prefix) {
		return s[len(prefix):]
	}
	return s
}

// computeReconcilePlan is a pure function that diffs the desired schema against
// the actual ClickHouse state and produces the mutations needed to converge.
//
//	actualTables:  name -> engine (engine distinguishes MaterializedView/View from a table)
//	actualColumns: table -> set of column names
func computeReconcilePlan(desired DesiredSchema, actualTables map[string]string, actualColumns map[string]map[string]struct{}) ReconcilePlan {
	plan := ReconcilePlan{}

	for name, engine := range actualTables {
		// ClickHouse-internal tables (e.g. `.inner_id.<uuid>` backing a TO-less
		// materialized view) are managed by ClickHouse, never declared, and must
		// never be dropped by reconcile.
		if strings.HasPrefix(name, ".") {
			continue
		}
		if isViewEngine(engine) {
			if _, ok := desired.views[name]; !ok {
				plan.dropViews = append(plan.dropViews, name)
			}
			continue
		}
		if _, ok := desired.tables[name]; !ok {
			plan.dropTables = append(plan.dropTables, name)
		}
	}

	for table, desiredCols := range desired.tables {
		if _, exists := actualTables[table]; !exists {
			// CREATE (run separately in Migrate) makes desired tables exist;
			// don't plan column edits for a table that isn't there yet.
			continue
		}
		actualCols := actualColumns[table]
		for col := range actualCols {
			if _, ok := desiredCols[col]; !ok {
				plan.dropColumns = append(plan.dropColumns, columnRef{table: table, column: col})
			}
		}
		for col, def := range desiredCols {
			if _, ok := actualCols[col]; !ok {
				plan.addColumns = append(plan.addColumns, columnAdd{table: table, column: col, definition: def})
			}
		}
	}

	return plan
}

// isViewEngine reports whether a ClickHouse engine string denotes a view (so it
// must be dropped with DROP VIEW rather than DROP TABLE).
func isViewEngine(engine string) bool {
	switch engine {
	case "MaterializedView", "View", "LiveView", "WindowView":
		return true
	default:
		return false
	}
}

// reconcileSchema makes the actual ClickHouse schema match the declarative
// embedded SQL: it strips undeclared tables, materialized views and columns and
// adds declared-but-missing columns. This is DESTRUCTIVE; see the guards below.
func (s *Store) reconcileSchema(ctx context.Context, mode string) error {
	if mode == "off" {
		slog.Info("schema reconcile disabled", "mode", mode)
		return nil
	}

	desired, err := parseDesiredSchema(embeddedMigrations())
	if err != nil {
		return fmt.Errorf("schema reconcile: parse desired schema: %w", err)
	}
	// GUARD: never drop everything because the parser produced nothing.
	if len(desired.tables) == 0 {
		return fmt.Errorf("refusing to reconcile: parsed 0 desired tables")
	}

	actualTables, err := s.readActualTables(ctx)
	if err != nil {
		return fmt.Errorf("schema reconcile: read actual tables: %w", err)
	}
	actualColumns, err := s.readActualColumns(ctx)
	if err != nil {
		return fmt.Errorf("schema reconcile: read actual columns: %w", err)
	}

	plan := computeReconcilePlan(desired, actualTables, actualColumns)
	plan = guardProtectedTables(plan)

	slog.Warn("schema reconcile plan",
		"mode", mode,
		"drop_tables_count", len(plan.dropTables),
		"drop_views_count", len(plan.dropViews),
		"drop_columns_count", len(plan.dropColumns),
		"add_columns_count", len(plan.addColumns),
		"drop_tables", plan.dropTables,
		"drop_views", plan.dropViews,
		"drop_columns", columnRefStrings(plan.dropColumns),
		"add_columns", columnAddStrings(plan.addColumns),
	)

	if mode == "dry-run" {
		slog.Info("schema reconcile dry-run: executing nothing", "mode", mode)
		return nil
	}

	return s.executeReconcilePlan(ctx, plan)
}

// guardProtectedTables removes any protected table (and its columns) from the
// destructive parts of the plan, logging loudly if the plan tried to drop one.
func guardProtectedTables(plan ReconcilePlan) ReconcilePlan {
	keptDropTables := plan.dropTables[:0:0]
	for _, t := range plan.dropTables {
		if _, ok := protectedTables[t]; ok {
			slog.Error("schema reconcile would drop protected table "+t+" — skipping; check the schema files/parser", "table", t)
			continue
		}
		keptDropTables = append(keptDropTables, t)
	}
	plan.dropTables = keptDropTables

	keptDropColumns := plan.dropColumns[:0:0]
	for _, c := range plan.dropColumns {
		if _, ok := protectedTables[c.table]; ok {
			slog.Error("schema reconcile would drop column on protected table "+c.table+" — skipping; check the schema files/parser", "table", c.table, "column", c.column)
			continue
		}
		keptDropColumns = append(keptDropColumns, c)
	}
	plan.dropColumns = keptDropColumns

	return plan
}

func (s *Store) executeReconcilePlan(ctx context.Context, plan ReconcilePlan) error {
	for _, view := range plan.dropViews {
		stmt := fmt.Sprintf("DROP VIEW IF EXISTS %s", quoteIdent(view))
		slog.Warn("schema reconcile dropping view", "view", view)
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema reconcile: drop view %s: %w", view, err)
		}
	}
	for _, table := range plan.dropTables {
		stmt := fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdent(table))
		slog.Warn("schema reconcile dropping table", "table", table)
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema reconcile: drop table %s: %w", table, err)
		}
	}
	for _, add := range plan.addColumns {
		stmt := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", quoteIdent(add.table), add.definition)
		slog.Warn("schema reconcile adding column", "table", add.table, "column", add.column)
		if err := s.conn.Exec(ctx, stmt); err != nil {
			return fmt.Errorf("schema reconcile: add column %s.%s: %w", add.table, add.column, err)
		}
	}
	for _, drop := range plan.dropColumns {
		stmt := fmt.Sprintf("ALTER TABLE %s DROP COLUMN IF EXISTS %s", quoteIdent(drop.table), quoteIdent(drop.column))
		slog.Warn("schema reconcile dropping column", "table", drop.table, "column", drop.column)
		if err := s.conn.Exec(ctx, stmt); err != nil {
			// Error-tolerant: ClickHouse may reject dropping a sort-key column.
			// Log and continue rather than aborting the whole reconcile.
			slog.Warn("schema reconcile: drop column failed, continuing", "table", drop.table, "column", drop.column, "error", err)
			continue
		}
	}
	return nil
}

func (s *Store) readActualTables(ctx context.Context) (map[string]string, error) {
	rows, err := s.conn.Query(ctx, "SELECT name, engine FROM system.tables WHERE database = currentDatabase()")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	tables := map[string]string{}
	for rows.Next() {
		var name, engine string
		if err := rows.Scan(&name, &engine); err != nil {
			return nil, err
		}
		tables[name] = engine
	}
	return tables, rows.Err()
}

func (s *Store) readActualColumns(ctx context.Context) (map[string]map[string]struct{}, error) {
	rows, err := s.conn.Query(ctx, "SELECT table, name FROM system.columns WHERE database = currentDatabase()")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := map[string]map[string]struct{}{}
	for rows.Next() {
		var table, name string
		if err := rows.Scan(&table, &name); err != nil {
			return nil, err
		}
		if columns[table] == nil {
			columns[table] = map[string]struct{}{}
		}
		columns[table][name] = struct{}{}
	}
	return columns, rows.Err()
}

func quoteIdent(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

func columnRefStrings(refs []columnRef) []string {
	out := make([]string, 0, len(refs))
	for _, r := range refs {
		out = append(out, r.table+"."+r.column)
	}
	return out
}

func columnAddStrings(adds []columnAdd) []string {
	out := make([]string, 0, len(adds))
	for _, a := range adds {
		out = append(out, a.table+"."+a.column)
	}
	return out
}
