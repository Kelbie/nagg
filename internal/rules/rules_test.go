package rules

import (
	"strings"
	"testing"
	"time"
)

func TestCanonicalName(t *testing.T) {
	cases := []struct {
		kinds []int
		ref   string
		want  string
	}{
		{[]int{7}, "e", "k7_e"},
		{[]int{16, 6}, "e", "k6_16_e"},
		{[]int{1}, "q", "k1_q"},
		{[]int{1, 1111}, "author", "k1_1111_author"},
	}
	for _, c := range cases {
		if got := CanonicalName(c.kinds, c.ref); got != c.want {
			t.Errorf("CanonicalName(%v, %q) = %q, want %q", c.kinds, c.ref, got, c.want)
		}
	}
}

func TestNewValidation(t *testing.T) {
	valid := Relationship{
		Name:    "k7_e",
		Kinds:   []int{7},
		Ref:     Ref{TagKey: "e", Target: TargetEventID},
		Metrics: []Metric{{Name: "actors", Agg: AggUniqActors}},
		Refresh: RefreshIngest,
	}

	cases := []struct {
		name    string
		mutate  func(r *Relationship)
		wantErr string
	}{
		{"valid", func(r *Relationship) {}, ""},
		{"empty name", func(r *Relationship) { r.Name = "" }, "empty name"},
		{"bad ident", func(r *Relationship) { r.Name = "K7-e" }, "lowercase identifier"},
		{"no kinds", func(r *Relationship) { r.Kinds = nil }, "no kinds"},
		{"no source", func(r *Relationship) { r.Ref.TagKey = "" }, "exactly one"},
		{"two sources", func(r *Relationship) { r.Ref.Extractor = "zap_target" }, "exactly one"},
		{"unknown extractor", func(r *Relationship) { r.Ref.TagKey = ""; r.Ref.Extractor = "nope" }, "unknown extractor"},
		{"marker on extractor", func(r *Relationship) {
			r.Ref.TagKey = ""
			r.Ref.Extractor = "zap_target"
			r.Ref.Marker = "reply"
		}, "marker filters"},
		{"bad target", func(r *Relationship) { r.Ref.Target = "thing" }, "invalid target"},
		{"no metrics", func(r *Relationship) { r.Metrics = nil }, "no metrics"},
		{"dup metric", func(r *Relationship) {
			r.Metrics = []Metric{{Name: "a", Agg: AggUniqActors}, {Name: "a", Agg: AggUniqSources}}
		}, "duplicate"},
		{"sum on tag ref", func(r *Relationship) {
			r.Metrics = []Metric{{Name: "v", Agg: AggSumValue}}
		}, "requires an extractor"},
		{"bad refresh", func(r *Relationship) { r.Refresh = "sometimes" }, "invalid refresh"},
		{"author bad target", func(r *Relationship) {
			r.Ref = Ref{Author: true, Target: TargetEventID}
			r.Metrics = []Metric{{Name: "sources", Agg: AggUniqSources}}
		}, "must target pubkeys"},
		{"author bad metric", func(r *Relationship) {
			r.Ref = Ref{Author: true, Target: TargetPubkey}
			r.Metrics = []Metric{{Name: "actors", Agg: AggUniqActors}}
		}, "uniq_sources metrics only"},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rel := valid
			rel.Metrics = append([]Metric(nil), valid.Metrics...)
			c.mutate(&rel)
			_, err := New([]Relationship{rel}, nil, nil)
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

func TestNewRejectsDuplicateNames(t *testing.T) {
	rel := Relationship{
		Name:    "k7_e",
		Kinds:   []int{7},
		Ref:     Ref{TagKey: "e", Target: TargetEventID},
		Metrics: []Metric{{Name: "actors", Agg: AggUniqActors}},
		Refresh: RefreshIngest,
	}
	_, err := New([]Relationship{rel, rel}, nil, nil)
	if err == nil || !strings.Contains(err.Error(), "duplicate name") {
		t.Fatalf("error = %v, want duplicate name", err)
	}
}

func TestLifetimeValidation(t *testing.T) {
	rel := Relationship{
		Name:    "k7_e",
		Kinds:   []int{7},
		Ref:     Ref{TagKey: "e", Target: TargetEventID},
		Metrics: []Metric{{Name: "actors", Agg: AggUniqActors}},
		Refresh: RefreshIngest,
	}
	_, err := New([]Relationship{rel}, []Lifetime{{
		Name:   "bad",
		Kinds:  []int{1},
		Policy: MaxAgeUnlessReferenced{Age: time.Hour, ByRules: []string{"missing"}},
	}}, nil)
	if err == nil || !strings.Contains(err.Error(), "unknown relationship") {
		t.Fatalf("error = %v, want unknown relationship", err)
	}
}

func TestCapValidation(t *testing.T) {
	_, err := New(nil, nil, []Cap{{Name: "c", Kinds: []int{1}, Max: 0}})
	if err == nil || !strings.Contains(err.Error(), "non-positive max") {
		t.Fatalf("error = %v, want non-positive max", err)
	}
	if _, err := New(nil, nil, []Cap{{Name: "c", Kinds: []int{1}, Max: 5}}); err != nil {
		t.Fatalf("lifetime cap (zero window) should validate: %v", err)
	}
}

func TestDefaultRegistry(t *testing.T) {
	r, err := Default(20)
	if err != nil {
		t.Fatalf("Default: %v", err)
	}
	for _, name := range []string{"k7_e", "k6_16_e", "k1_q", "k9735_e", "k1_1111_e_reply", "k1_1111_author"} {
		if r.Relationship(name) == nil {
			t.Errorf("missing relationship %q", name)
		}
	}
	if got := len(r.IngestExtractorRules()); got != 1 {
		t.Errorf("ingest extractor rules = %d, want 1", got)
	}
	if len(r.Lifetimes()) != 3 || len(r.Caps()) != 1 {
		t.Errorf("lifetimes/caps = %d/%d, want 3/1", len(r.Lifetimes()), len(r.Caps()))
	}
	if r.Caps()[0].Window != 24*time.Hour {
		t.Errorf("cap window = %v, want 24h", r.Caps()[0].Window)
	}
}
