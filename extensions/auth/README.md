# Auth Extensions

Auth extensions implement the public `sdk/auth` login-provider contract. They
authenticate username/password login attempts before the API maps the resulting
external identity onto a normal Vectis local user row.

That mapping keeps existing sessions, API tokens, role bindings, audit actor
IDs, and namespace authorization anchored in Vectis. External providers prove
who the user is; Vectis still decides what that local user can do.

Current implementations:

| Provider | Package | Notes |
| --- | --- | --- |
| LDAP | `extensions/auth/ldap` | Service-account search plus user bind against LDAP-compatible directories. |
