package clickhouse

import (
	"reflect"
	"testing"
)

func TestSplitSQLStatements(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []string
	}{
		{
			name: "simple statements",
			sql:  "CREATE TABLE a (id Int);\nCREATE TABLE b (id Int);",
			want: []string{"CREATE TABLE a (id Int)", "CREATE TABLE b (id Int)"},
		},
		{
			name: "semicolon inside line comment does not split",
			sql:  "-- kind = 1 only; then recreate\nDROP VIEW v;\nCREATE VIEW v AS SELECT 1;",
			want: []string{"DROP VIEW v", "CREATE VIEW v AS SELECT 1"},
		},
		{
			name: "semicolon inside string literal does not split",
			sql:  "INSERT INTO t VALUES ('a;b');\nSELECT 1;",
			want: []string{"INSERT INTO t VALUES ('a;b')", "SELECT 1"},
		},
		{
			name: "escaped quote inside string literal",
			sql:  "SELECT 'it''s; fine';",
			want: []string{"SELECT 'it''s; fine'"},
		},
		{
			name: "block comment with semicolon is dropped",
			sql:  "/* drop; me */ SELECT 1; SELECT 2;",
			want: []string{"SELECT 1", "SELECT 2"},
		},
		{
			name: "backtick identifier with semicolon",
			sql:  "SELECT `weird;col` FROM t;",
			want: []string{"SELECT `weird;col` FROM t"},
		},
		{
			name: "trailing empty statement after final semicolon",
			sql:  "SELECT 1;\n",
			want: []string{"SELECT 1"},
		},
		{
			name: "no trailing semicolon",
			sql:  "SELECT 1",
			want: []string{"SELECT 1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := splitSQLStatements(tc.sql)
			if len(tc.want) == 0 && len(got) == 0 {
				return
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("splitSQLStatements(%q)\n got = %#v\nwant = %#v", tc.sql, got, tc.want)
			}
		})
	}
}

// TestSplitSQLStatements_RealMigrationsParseToValidStatements guards that every
// embedded migration splits into non-empty statements that each begin with a
// SQL keyword — i.e. no statement was broken apart mid-comment.
func TestSplitSQLStatements_RealMigrationsParseToValidStatements(t *testing.T) {
	for _, migration := range embeddedMigrations() {
		for _, stmt := range splitSQLStatements(migration) {
			if stmt == "" {
				t.Fatal("got an empty statement from a real migration")
			}
			if stmt[0] == '-' || stmt[0] == '*' || stmt[0] == '/' {
				t.Fatalf("statement starts with a comment fragment (splitter broke a statement): %q", stmt)
			}
		}
	}
}
