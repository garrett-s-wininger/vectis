# Gerrit Action Extension

`extensions/actions/gerrit` owns Gerrit-specific REST behavior for Vectis'
standard Gerrit integration.

The package intentionally stays outside `internal/`: Gerrit is an extension
point, not core control-plane logic. It provides reusable helpers for:

- querying Gerrit changes from the provider-neutral `sdk/scm.Query` shape;
- resolving the current revision and fetch ref;
- posting review messages and label votes;
- decoding Gerrit's XSSI-prefixed JSON responses.

Generic SCM change discovery and polling lives in `sdk/scm`; this package owns
only the Gerrit REST API, query syntax, authentication, and review behavior.

The `gerrit/review@v1` action descriptor lives in `review/action.json` and runs
the process entrypoint in `review/`. Configure `extensions/actions` as an action
registry local root to use it from jobs.
