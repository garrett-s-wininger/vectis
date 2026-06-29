# Gerrit SCM Provider

The Gerrit SCM provider implements `sdk/scm.PollProvider` by querying Gerrit's
changes REST API and comparing each change's current revision against the stored
cursor.

Use it for change-level triggers where a Gerrit review update should dispatch a
Vectis run once per current revision. The provider translates Gerrit change
metadata into stable SCM poll events; the core SCM poller owns trigger claims,
event dedupe, run creation, and dispatch.

`base_url` must point at the Gerrit server. `project`, `branch`, and `query`
are combined into a Gerrit query. When `query` is omitted, the provider adds
`status:open`.
