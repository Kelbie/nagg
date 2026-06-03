package graphqlapi

import (
	"html/template"
	"net/http"
	"strings"
)

const (
	graphiqlVersion         = "5.2.3"
	graphiqlExplorerVersion = "5.1.2"
	graphiqlReactLibVersion = "0.37.5"
	reactVersion            = "19.2.7"
	graphiqlGraphQLVersion  = "16.14.1"
)

var graphiqlTemplate = template.Must(template.New("graphiql").Parse(`<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Nagg GraphiQL</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/graphiql@` + graphiqlVersion + `/dist/style.css">
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@graphiql/plugin-explorer@` + graphiqlExplorerVersion + `/dist/style.css">
  <style>
    html,
    body,
    #graphiql {
      height: 100%;
      margin: 0;
    }
  </style>
  <script type="importmap">
    {
      "imports": {
        "react": "https://esm.sh/react@` + reactVersion + `",
        "react/jsx-runtime": "https://esm.sh/react@` + reactVersion + `/jsx-runtime",
        "react/jsx-dev-runtime": "https://esm.sh/react@` + reactVersion + `/jsx-dev-runtime",
        "react-dom": "https://esm.sh/react-dom@` + reactVersion + `",
        "react-dom/client": "https://esm.sh/react-dom@` + reactVersion + `/client",
        "graphql": "https://esm.sh/graphql@` + graphiqlGraphQLVersion + `",
        "@graphiql/react": "https://esm.sh/@graphiql/react@` + graphiqlReactLibVersion + `?external=react,react-dom,graphql"
      }
    }
  </script>
</head>
<body>
  <div id="graphiql">Loading Nagg GraphiQL...</div>
  <script type="module">
    import "https://cdn.jsdelivr.net/npm/@graphiql/react@` + graphiqlReactLibVersion + `/dist/setup-workers/esm.sh.js";
    import React from "react";
    import { createRoot } from "react-dom/client";
    import { languages as monacoGraphQLLanguages } from "https://esm.sh/monaco-graphql@^1.8.0/esm/monaco-editor?external=graphql&target=es2022";
    import { GraphiQL } from "https://esm.sh/graphiql@` + graphiqlVersion + `?external=react,react-dom,graphql,@graphiql/react";
    import { explorerPlugin } from "https://esm.sh/@graphiql/plugin-explorer@` + graphiqlExplorerVersion + `?external=react,react-dom,graphql,@graphiql/react";

    if (!monacoGraphQLLanguages.json) {
      const jsonDefaults = {
        diagnosticsOptions: { schemas: [] },
        setDiagnosticsOptions(options) {
          this.diagnosticsOptions = options;
        }
      };
      monacoGraphQLLanguages.json = { jsonDefaults };
    }

    const graphqlEndpoint = {{ .GraphQLEndpoint }};
    const fetcher = async (graphQLParams) => {
      const response = await fetch(graphqlEndpoint, {
        method: "POST",
        headers: {
          "content-type": "application/json"
        },
        body: JSON.stringify(graphQLParams)
      });
      return response.json();
    };

    const defaultQuery = ` + "`" + `query RecentNotes {
  events(input: { kinds: [1], limit: 10 }) {
    nodes {
      id
      pubkey
      kind
      createdAt
      content
      tags
    }
    pageInfo {
      endCursor
      hasNextPage
    }
  }
}` + "`" + `;

    createRoot(document.getElementById("graphiql")).render(
      React.createElement(GraphiQL, {
        fetcher,
        defaultQuery,
        plugins: [explorerPlugin()]
      })
    );
  </script>
</body>
</html>
`))

type graphiqlPageData struct {
	GraphQLEndpoint string
}

// GraphiQLHandler serves the browser IDE with the Explorer plugin enabled.
func GraphiQLHandler(graphQLEndpoint string) http.HandlerFunc {
	graphQLEndpoint = strings.TrimSpace(graphQLEndpoint)
	if graphQLEndpoint == "" {
		graphQLEndpoint = "/graphql"
	}
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			http.Error(w, "GET /graphiql only", http.StatusMethodNotAllowed)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		if r.Method == http.MethodHead {
			return
		}
		if err := graphiqlTemplate.Execute(w, graphiqlPageData{GraphQLEndpoint: graphQLEndpoint}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	}
}
