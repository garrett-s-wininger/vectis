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

`vectis-scm-poller` registers this provider with the key `gerrit`. Configure
HTTP credentials on the poller with `--gerrit-username` plus
`--gerrit-password-file` or `--gerrit-password`; the matching environment
variables are `VECTIS_SCM_POLLER_PROVIDERS_GERRIT_USERNAME`,
`VECTIS_SCM_POLLER_PROVIDERS_GERRIT_PASSWORD_FILE`, and
`VECTIS_SCM_POLLER_PROVIDERS_GERRIT_PASSWORD`. Username and password must be
configured together; omit both for anonymous Gerrit queries.

## Smoke

Run the provider smoke against an already reachable Gerrit:

```sh
make gerrit-scm-smoke-check
```

Or start the local Gerrit container first:

```sh
make gerrit-scm-smoke
```

The smoke queries Gerrit's changes API through this SCM provider, validates the
cursor returned by the provider, and can optionally force matching existing
changes to emit events with `GERRIT_SCM_SMOKE_EMIT_EXISTING=true` plus
`GERRIT_SCM_SMOKE_MIN_EVENTS=1`.

Useful knobs:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `GERRIT_SCM_SMOKE_URL` | `http://127.0.0.1:18088` | Gerrit base URL. |
| `GERRIT_SCM_SMOKE_PROJECT` | empty | Optional Gerrit project query term. |
| `GERRIT_SCM_SMOKE_BRANCH` | empty | Optional Gerrit branch query term. |
| `GERRIT_SCM_SMOKE_QUERY` | `status:open` | Additional Gerrit query. |
| `GERRIT_SCM_SMOKE_CURSOR` | empty | Existing provider cursor JSON. |
| `GERRIT_SCM_SMOKE_USERNAME` | empty | Optional Gerrit HTTP username. |
| `GERRIT_SCM_SMOKE_PASSWORD` | empty | Optional Gerrit HTTP password. |
| `GERRIT_SCM_SMOKE_PASSWORD_FILE` | empty | File containing the optional Gerrit HTTP password. |
| `GERRIT_SCM_SMOKE_EMIT_EXISTING` | `false` | Use an empty bootstrapped cursor so existing matches emit events. |
| `GERRIT_SCM_SMOKE_MIN_EVENTS` | `0` | Minimum events required for success. |
| `GERRIT_SCM_SMOKE_TIMEOUT` | `30s` | Maximum wait for the provider smoke. |
