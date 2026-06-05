package cache

import (
	"strings"
	"testing"
)

func TestGraphQLKeyStableAcrossWhitespace(t *testing.T) {
	vars := map[string]any{"input": map[string]any{"viewer": "abc", "limit": 10}}
	a := GraphQLKey("query Q { events }", "Q", vars, "")
	b := GraphQLKey("query   Q {\n  events\n}", "Q", vars, "")
	if a != b {
		t.Fatalf("expected whitespace-insensitive keys, got %q vs %q", a, b)
	}
}

func TestGraphQLKeyVariesWithVariables(t *testing.T) {
	q := "query Q { events }"
	a := GraphQLKey(q, "Q", map[string]any{"v": 1}, "")
	b := GraphQLKey(q, "Q", map[string]any{"v": 2}, "")
	if a == b {
		t.Fatal("expected different keys for different variables")
	}
}

func TestGraphQLKeyViewerSuffix(t *testing.T) {
	viewer := strings.Repeat("a", 64)
	key := GraphQLKey("query Q { notifications }", "Q", nil, viewer)
	if !strings.HasSuffix(key, ":viewer="+viewer) {
		t.Fatalf("expected viewer suffix, got %q", key)
	}
}

func TestRESTKeyDropsRefreshAndSortsParams(t *testing.T) {
	a := RESTKey("GET", "/nostr/feed", "pubkey=abc&limit=10&refresh=1", "")
	b := RESTKey("GET", "/nostr/feed", "limit=10&pubkey=abc", "")
	if a != b {
		t.Fatalf("expected refresh-insensitive, order-insensitive keys, got %q vs %q", a, b)
	}
}

func TestKeysCarrySchemaPrefix(t *testing.T) {
	if !strings.Contains(GraphQLKey("q", "", nil, ""), schemaPrefix()) {
		t.Fatal("graphql key missing schema prefix")
	}
	if !strings.Contains(RESTKey("GET", "/x", "", ""), schemaPrefix()) {
		t.Fatal("rest key missing schema prefix")
	}
}

func TestIsHex64(t *testing.T) {
	if !isHex64(strings.Repeat("a", 64)) {
		t.Fatal("expected 64-char hex to be valid")
	}
	if isHex64(strings.Repeat("a", 63)) {
		t.Fatal("expected 63-char string to be invalid")
	}
	if isHex64(strings.Repeat("g", 64)) {
		t.Fatal("expected non-hex to be invalid")
	}
}
