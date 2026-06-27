# LDAP Login Provider

`extensions/auth/ldap` adds LDAP-backed API login to `vectis-api`.

Enable API auth and configure the provider on the API server:

```sh
VECTIS_API_AUTH_ENABLED=true
VECTIS_API_AUTH_LDAP_URL=ldap://openldap:389
VECTIS_API_AUTH_LDAP_BIND_DN=cn=vectis,ou=service-accounts,dc=example,dc=org
VECTIS_API_AUTH_LDAP_BIND_PASSWORD_FILE=/run/secrets/vectis-ldap-bind-password
VECTIS_API_AUTH_LDAP_BASE_DN=ou=people,dc=example,dc=org
VECTIS_API_AUTH_LDAP_USER_FILTER='(uid={username})'
VECTIS_API_AUTH_LDAP_USERNAME_ATTRIBUTE=uid
VECTIS_API_AUTH_LDAP_DISPLAY_NAME_ATTRIBUTE=cn
VECTIS_API_AUTH_LDAP_AUTO_CREATE_USERS=false
```

Equivalent flags are available on `vectis-api-server`: `--ldap-url`,
`--ldap-bind-dn`, `--ldap-bind-password-file`, `--ldap-base-dn`,
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
4. The LDAP username attribute maps to a Vectis local user. If
   `auto_create_users` is false, that local user must already exist. If true,
   Vectis creates a local user row with a random unknown local password.
5. Sessions, API tokens, role bindings, and audit actor IDs remain tied to the
   local Vectis user ID.

Group-to-role synchronization is intentionally not part of this provider slice.
Grant roles to the mapped local users through Vectis role bindings.
