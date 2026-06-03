package graphqlapi

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestGraphiQLHandlerServesExplorerPage(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/graphiql", nil)
	rec := httptest.NewRecorder()

	GraphiQLHandler("/graphql")(rec, req)

	res := rec.Result()
	if res.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want %d", res.StatusCode, http.StatusOK)
	}
	if got := res.Header.Get("Content-Type"); got != "text/html; charset=utf-8" {
		t.Fatalf("content-type = %q", got)
	}
	body, err := io.ReadAll(res.Body)
	if err != nil {
		t.Fatal(err)
	}
	html := string(body)
	for _, want := range []string{
		"GraphiQL",
		"@graphiql/plugin-explorer@5.1.2",
		"explorerPlugin()",
		`const graphqlEndpoint = "/graphql";`,
		"query RecentNotes",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("body does not contain %q", want)
		}
	}
}

func TestGraphiQLHandlerDefaultsEndpoint(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/graphiql", nil)
	rec := httptest.NewRecorder()

	GraphiQLHandler(" ")(rec, req)

	body, err := io.ReadAll(rec.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `const graphqlEndpoint = "/graphql";`) {
		t.Fatalf("body does not contain default graphql endpoint")
	}
}

func TestGraphiQLHandlerRejectsNonGetMethods(t *testing.T) {
	req := httptest.NewRequest(http.MethodPost, "/graphiql", nil)
	rec := httptest.NewRecorder()

	GraphiQLHandler("/graphql")(rec, req)

	if rec.Result().StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d, want %d", rec.Result().StatusCode, http.StatusMethodNotAllowed)
	}
}
