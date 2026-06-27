package clickhouse

import (
	"regexp"
	"strings"
	"testing"
)

// migrationFilePresent reports whether the embedded migrations include name.
func migrationFilePresent(name string) bool {
	for _, n := range migrationNames() {
		if n == name {
			return true
		}
	}
	return false
}

var migrationFilenameRe = regexp.MustCompile(`^\d{3}_[a-z0-9_]+\.sql$`)

// TestMigrations_FilenamesAreNNNPrefixed keeps lexical sort == apply order: a
// stray name (missing the zero-padded prefix, or a 4-digit one) would reorder
// silently. Three digits is safe to 999 migrations.
func TestMigrations_FilenamesAreNNNPrefixed(t *testing.T) {
	names := migrationNames()
	if len(names) == 0 {
		t.Fatal("no embedded migrations discovered")
	}
	for _, name := range names {
		if !migrationFilenameRe.MatchString(name) {
			t.Errorf("migration %q must match NNN_name.sql (3-digit prefix; lexical==apply order)", name)
		}
	}
}

// idempotentInsertTargets are tables whose engine makes a repeated INSERT
// converge instead of double-count — AggregatingMergeTree (uniqState/*State) or
// ReplacingMergeTree keyed on the inserted id. Migrations re-run on every deploy
// (there is no applied-versions ledger, by design — the backfills must keep
// converging with the live MVs), so an INSERT into any other table double-applies.
var idempotentInsertTargets = map[string]struct{}{
	"note_reply_counts":        {},
	"note_like_counts":         {},
	"note_repost_counts":       {},
	"note_quote_counts":        {},
	"note_direct_reply_counts": {},
	"note_reply_edges":         {},
	"user_contacts_latest":     {},
	"user_post_counts":         {},
}

// allowedStmtPrefixes are the re-runnable DDL shapes migrations may use. Every
// ALTER form in use (MODIFY TTL / ADD COLUMN IF NOT EXISTS / RENAME COLUMN) is
// naturally re-runnable; tighten to sub-prefixes if a bare destructive ALTER
// is ever added.
var allowedStmtPrefixes = []string{
	"CREATE TABLE IF NOT EXISTS",
	"CREATE MATERIALIZED VIEW IF NOT EXISTS",
	"CREATE DICTIONARY IF NOT EXISTS",
	"DROP TABLE IF EXISTS",
	"DROP VIEW IF EXISTS",
	"TRUNCATE TABLE IF EXISTS",
	"ALTER TABLE",
}

var insertTargetRe = regexp.MustCompile(`(?i)^INSERT\s+INTO\s+([a-zA-Z_][a-zA-Z0-9_]*)`)

// TestMigrations_OnlyIdempotentStatements enforces the invariant that every
// migration statement is safe to re-execute on every deploy. A new
// non-idempotent statement (bare INSERT into a non-convergent table, CREATE
// without IF NOT EXISTS, one-shot UPDATE/DELETE) fails here and must be reworked
// or, for a genuinely convergent INSERT target, added to idempotentInsertTargets.
func TestMigrations_OnlyIdempotentStatements(t *testing.T) {
	for _, name := range migrationNames() {
		for _, raw := range splitSQLStatements(mustReadMigration(name)) {
			stmt := stripLeadingComments(raw)
			if stmt == "" {
				continue
			}
			upper := strings.ToUpper(stmt)
			if strings.HasPrefix(upper, "INSERT") {
				m := insertTargetRe.FindStringSubmatch(stmt)
				if m == nil {
					t.Errorf("%s: unparseable INSERT: %.70q", name, stmt)
					continue
				}
				if _, ok := idempotentInsertTargets[m[1]]; !ok {
					t.Errorf("%s: INSERT INTO %q is not a known convergent target — it would double-apply on redeploy. "+
						"Only add it to idempotentInsertTargets if its engine dedupes/merges.", name, m[1])
				}
				continue
			}
			if !hasAnyStmtPrefix(upper, allowedStmtPrefixes) {
				t.Errorf("%s: non-idempotent statement (must be safe to re-run every deploy): %.70q", name, stmt)
			}
		}
	}
}

// stripLeadingComments drops leading blank and `--` comment lines so the
// statement's leading keyword can be matched.
func stripLeadingComments(stmt string) string {
	lines := strings.Split(stmt, "\n")
	for len(lines) > 0 {
		t := strings.TrimSpace(lines[0])
		if t == "" || strings.HasPrefix(t, "--") {
			lines = lines[1:]
			continue
		}
		break
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func hasAnyStmtPrefix(s string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(s, p) {
			return true
		}
	}
	return false
}
