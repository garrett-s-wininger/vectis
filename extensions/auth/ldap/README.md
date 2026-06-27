# LDAP Login Provider

`extensions/auth/ldap` adds LDAP-backed API login to `vectis-api`.

Enable API auth and configure the provider on the API server:

```sh
VECTIS_API_AUTH_ENABLED=true
VECTIS_API_AUTH_LDAP_PROVIDER_ID=corp-ldap
VECTIS_API_AUTH_LDAP_URL=ldap://openldap:389
VECTIS_API_AUTH_LDAP_BIND_DN=cn=vectis,ou=service-accounts,dc=example,dc=org
VECTIS_API_AUTH_LDAP_BIND_PASSWORD_FILE=/run/secrets/vectis-ldap-bind-password
VECTIS_API_AUTH_LDAP_BASE_DN=ou=people,dc=example,dc=org
VECTIS_API_AUTH_LDAP_USER_FILTER='(uid={username})'
VECTIS_API_AUTH_LDAP_USERNAME_ATTRIBUTE=uid
VECTIS_API_AUTH_LDAP_DISPLAY_NAME_ATTRIBUTE=cn
VECTIS_API_AUTH_LDAP_AUTO_CREATE_USERS=false
```

Equivalent flags are available on `vectis-api-server`: `--ldap-provider-id`,
`--ldap-url`, `--ldap-bind-dn`, `--ldap-bind-password-file`, `--ldap-base-dn`,
`--ldap-user-filter`, `--ldap-username-attribute`,
`--ldap-display-name-attribute`, `--ldap-start-tls`, `--ldap-timeout`, and
`--ldap-auto-create-users`.

Login flow:

1. The API keeps local username/password login enabled.
2. If local password login does not authenticate the request, configured
   external providers are tried.
3. LDAP binds with the service account when configured, searches for exactly one
   user entry under `base_dn`, then binds as that user with the submitted
   password.
4. The configured LDAP provider ID and LDAP subject are linked to a Vectis
   local user. If the link already exists, Vectis uses that local user even if
   the current username claim changed.
5. If no link exists, the LDAP username attribute maps to a Vectis local user.
   If `auto_create_users` is false, that local user must already exist. If
   true, Vectis creates a local user row with a random unknown local password.
   The successful login then records the provider/subject link.
6. Sessions, API tokens, role bindings, and audit actor IDs remain tied to the
   local Vectis user ID.

Group-to-role synchronization is intentionally not part of this provider slice.
Grant roles to the mapped local users through Vectis role bindings.

Provider IDs are instance IDs, not only provider kinds. Use distinct values such
as `corp-ldap` and `contractor-ldap` when multiple directories can authenticate
users. Vectis stores external identity links by `(provider_id, subject)` and
allows at most one subject from the same provider to link to a local user.

## Real Service Smoke

Provider unit tests use a fake LDAP connection. The real-service smoke exercises
an actual OpenLDAP server with the same provider code used by `vectis-api`, then
runs an in-memory Vectis API login/session/token flow against that provider:

```sh
make ldap-smoke
```

`ldap-smoke-up` starts a local OpenLDAP container with a read-only bind user and
a seeded login user from `extensions/auth/ldap/testdata/bootstrap`.
`ldap-smoke-check` authenticates the seeded user, checks the mapped subject,
username, and display name, and verifies that a wrong password is rejected.
`ldap-api-smoke-check` completes API setup in an in-memory SQLite database, logs
in through `POST /api/v1/login` with LDAP credentials, verifies the returned
bearer token against an authenticated API endpoint, and verifies that a wrong
LDAP password is rejected through the API path.

Run `make ldap-smoke-check` for the provider-only check or
`make ldap-api-smoke-check` when OpenLDAP is already running and only the API
login path needs to be rechecked.

Useful knobs:

| Variable | Default | Purpose |
| --- | ---: | --- |
| `VECTIS_API_AUTH_LDAP_PROVIDER_ID` | `ldap` | Stable provider instance ID used by the API login path. |
| `LDAP_SMOKE_IMAGE` | `docker.io/osixia/openldap:1.5.0` | Local OpenLDAP image. |
| `LDAP_SMOKE_CONTAINER` | `vectis-openldap` | Reusable local container name. |
| `LDAP_SMOKE_PORT` | `1389` | Host port mapped to LDAP. |
| `LDAP_SMOKE_URL` | `ldap://127.0.0.1:1389` | URL passed to the smoke binary. |
| `LDAP_SMOKE_BOOTSTRAP_DIR` | `extensions/auth/ldap/testdata/bootstrap` | LDIF directory mounted into the container. |
| `LDAP_SMOKE_BASE_DN` | `ou=people,dc=example,dc=org` | Base DN searched by the provider. |
| `LDAP_SMOKE_BIND_USERNAME` | `vectis` | Read-only bind username created by OpenLDAP. |
| `LDAP_SMOKE_BIND_DN` | `cn=vectis,dc=example,dc=org` | Read-only bind DN used before user search. |
| `LDAP_SMOKE_BIND_PASSWORD` | `service-secret` | Service-account password. |
| `LDAP_SMOKE_USERNAME` | `alice` | User authenticated by the smoke. |
| `LDAP_SMOKE_PASSWORD` | `alice-secret` | User password expected to succeed. |
| `LDAP_SMOKE_WRONG_PASSWORD` | `wrong-secret` | User password expected to fail. |
| `LDAP_SMOKE_TIMEOUT` | `30s` | Maximum wait for the endpoint and smoke operations. |

To point the same smoke at a separately managed directory:

```sh
go run ./extensions/auth/ldap/smoke \
  --url ldap://127.0.0.1:389 \
  --bind-dn cn=vectis,ou=service-accounts,dc=example,dc=org \
  --bind-password-file /run/secrets/vectis-ldap-bind-password \
  --base-dn ou=people,dc=example,dc=org \
  --username alice \
  --password alice-secret
```
