package vertex

import (
	"time"

	"testing"

	"github.com/vertex-lab/nagg/internal/dvm"
)

func TestPluginIdentity(t *testing.T) {
	p := NewPlugin()
	if p.Name() != "vertex" {
		t.Fatalf("name = %q", p.Name())
	}
	kinds := p.Kinds()
	want := []dvm.KindPair{{5312, 6312}, {5313, 6313}, {5315, 6315}}
	if len(kinds) != len(want) {
		t.Fatalf("kinds = %v", kinds)
	}
	for i, k := range want {
		if kinds[i] != k {
			t.Fatalf("kinds[%d] = %v, want %v", i, kinds[i], k)
		}
	}
	if len(p.CacheDDL()) != 3 {
		t.Fatalf("cache DDL statements = %d, want 3", len(p.CacheDDL()))
	}
}

func TestPluginCapabilitiesNilUntilAttached(t *testing.T) {
	p := NewPlugin()
	if p.SearchProvider() != nil || p.RecommendProvider() != nil || p.ScoreProvider() != nil {
		t.Fatal("capabilities must be nil before attachment")
	}
	sp := &SearchProvider{}
	if p.WithSearch(sp).SearchProvider() != sp {
		t.Fatal("attached search provider must be returned")
	}
	if _, err := dvm.NewRegistry(p); err != nil {
		t.Fatalf("vertex plugin must validate: %v", err)
	}
}

func TestPluginPolicy(t *testing.T) {
	p := NewPlugin()
	policy := p.Policy()
	if policy.CacheTTL != 7*24*time.Hour {
		t.Errorf("CacheTTL = %v, want 7 days", policy.CacheTTL)
	}
	if policy.MinInboundRefs != 500 {
		t.Errorf("MinInboundRefs = %d, want 500", policy.MinInboundRefs)
	}
}
