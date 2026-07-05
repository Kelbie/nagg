package dvm

import (
	"strings"
	"testing"
)

type fakePlugin struct {
	name  string
	kinds []KindPair
	ddl   []string
}

func (p fakePlugin) Name() string           { return p.name }
func (p fakePlugin) Kinds() []KindPair      { return p.kinds }
func (p fakePlugin) CacheDDL() []string     { return p.ddl }
func (p fakePlugin) Policy() Policy         { return Policy{} }
func (p fakePlugin) ScoreProvider() any     { return nil }
func (p fakePlugin) SearchProvider() any    { return nil }
func (p fakePlugin) RecommendProvider() any { return nil }

func valid() fakePlugin {
	return fakePlugin{
		name:  "acme",
		kinds: []KindPair{{Request: 5000, Response: 6000}},
		ddl:   []string{"CREATE TABLE IF NOT EXISTS acme_cache (x String) ENGINE = MergeTree ORDER BY x;"},
	}
}

func TestNewRegistryValidation(t *testing.T) {
	cases := []struct {
		name    string
		mutate  func(*fakePlugin)
		wantErr string
	}{
		{"valid", func(p *fakePlugin) {}, ""},
		{"empty name", func(p *fakePlugin) { p.name = "" }, "lowercase identifier"},
		{"uppercase name", func(p *fakePlugin) { p.name = "Acme" }, "lowercase identifier"},
		{"no kinds", func(p *fakePlugin) { p.kinds = nil }, "declares no kinds"},
		{"bad ddl", func(p *fakePlugin) { p.ddl = []string{"DROP TABLE users"} }, "CREATE TABLE IF NOT EXISTS"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			p := valid()
			c.mutate(&p)
			_, err := NewRegistry(p)
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
}

func TestRegistryDuplicateAndLookup(t *testing.T) {
	if _, err := NewRegistry(valid(), valid()); err == nil || !strings.Contains(err.Error(), "twice") {
		t.Fatalf("duplicate registration must fail: %v", err)
	}
	r := MustRegistry(valid())
	if !r.Has("acme") || r.Plugin("acme") == nil || r.Plugin("other") != nil {
		t.Fatalf("lookup misbehaves: %v", r.Names())
	}
	if got := len(r.CacheDDL()); got != 1 {
		t.Fatalf("CacheDDL = %d statements, want 1", got)
	}
	if got := len(Empty().CacheDDL()); got != 0 {
		t.Fatalf("empty registry CacheDDL = %d, want 0", got)
	}
}
