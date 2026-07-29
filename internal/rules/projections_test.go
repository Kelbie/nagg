package rules

import (
	"strings"
	"testing"
)

func TestProjectionValidation(t *testing.T) {
	valid := Projection{
		Name:   "k0",
		Kinds:  []int{0},
		Fields: []ProjField{{Name: "name", JSONPath: "name"}},
	}
	cases := []struct {
		name    string
		mutate  func(p *Projection)
		wantErr string
	}{
		{"valid", func(p *Projection) {}, ""},
		{"empty name", func(p *Projection) { p.Name = "" }, "empty name"},
		{"bad ident", func(p *Projection) { p.Name = "K0" }, "lowercase identifier"},
		{"no kinds", func(p *Projection) { p.Kinds = nil }, "no kinds"},
		{"no fields", func(p *Projection) { p.Fields = nil }, "no fields"},
		{"reserved field", func(p *Projection) {
			p.Fields = []ProjField{{Name: "pubkey", JSONPath: "x"}}
		}, "reserved column"},
		{"dup field", func(p *Projection) {
			p.Fields = []ProjField{{Name: "a", JSONPath: "x"}, {Name: "a", RawContent: true}}
		}, "duplicate"},
		{"no source", func(p *Projection) {
			p.Fields = []ProjField{{Name: "a"}}
		}, "exactly one"},
		{"two sources", func(p *Projection) {
			p.Fields = []ProjField{{Name: "a", JSONPath: "x", RawContent: true}}
		}, "exactly one"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			proj := valid
			proj.Fields = append([]ProjField(nil), valid.Fields...)
			c.mutate(&proj)
			_, err := New(nil, []Projection{proj}, nil, nil, nil, nil, nil)
			if c.wantErr == "" {
				if err != nil {
					t.Fatalf("unexpected error: %v", err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), c.wantErr) {
				t.Fatalf("error = %v, want containing %q", err, c.wantErr)
			}
		})
	}

	if _, err := New(nil, []Projection{valid, valid}, nil, nil, nil, nil, nil); err == nil || !strings.Contains(err.Error(), "duplicate name") {
		t.Fatalf("duplicate projection names must fail; got %v", err)
	}
}

func TestProjectionDDL(t *testing.T) {
	r := mustDefault(t)
	ddl := strings.Join(r.GeneratedDDL(), "\n\n")

	wantK0Table := `CREATE TABLE IF NOT EXISTS latest_k0
(
    pubkey FixedString(64),
    event_id FixedString(64),
    created_at DateTime,
    name String,`
	if !strings.Contains(ddl, wantK0Table) {
		t.Errorf("missing latest_k0 table DDL; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "ENGINE = ReplacingMergeTree(created_at)\nORDER BY pubkey;") {
		t.Errorf("projection tables must be RMT(created_at) keyed by pubkey")
	}
	wantK0View := `CREATE MATERIALIZED VIEW IF NOT EXISTS mv_latest_k0
TO latest_k0
AS
SELECT
    pubkey,
    id AS event_id,
    created_at,
    JSONExtractString(content, 'name') AS name,`
	if !strings.Contains(ddl, wantK0View) {
		t.Errorf("missing latest_k0 view DDL; got:\n%s", ddl)
	}
	if !strings.Contains(ddl, "content AS raw_json") {
		t.Errorf("RawContent field must select content")
	}
	// k3: tag-values array extraction, exactly the shape the hand-written
	// projection used.
	for _, want := range []string{
		"CREATE TABLE IF NOT EXISTS latest_k3",
		"refs Array(FixedString(64))",
		"t[1] = 'p' AND length(t[2]) = 64",
		"JSONExtract(tags_json, 'Array(Array(String))')))",
	} {
		if !strings.Contains(ddl, want) {
			t.Errorf("latest_k3 DDL missing %q", want)
		}
	}
}

func TestProjectionBackfillSQL(t *testing.T) {
	r := mustDefault(t)
	var k0 Projection
	for _, p := range r.Projections() {
		if p.Name == "k0" {
			k0 = p
		}
	}
	sql := ProjectionBackfillSQL(k0)
	if !strings.HasPrefix(sql, "INSERT INTO latest_k0\nSELECT") {
		t.Errorf("backfill = %q", sql)
	}
	if !strings.Contains(sql, "FROM nostr_events FINAL") {
		t.Errorf("backfill must read nostr_events FINAL:\n%s", sql)
	}
}
