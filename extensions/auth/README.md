# Auth Extensions

Auth extensions implement the public `sdk/auth` login-provider contract. They
authenticate username/password login attempts before the API maps the resulting
external identity onto a normal Vectis local user row.

That mapping keeps existing sessions, API tokens, role bindings, audit actor
IDs, and namespace authorization anchored in Vectis. External providers prove
who the user is; Vectis still decides what that local user can do.

`Identity.Provider` is a stable provider instance ID, not just the provider
kind. The API records external identities by `(provider_id, subject)` and links
them to local users. New providers should return a durable subject from the
upstream authority and use distinct provider IDs for distinct directories or
issuers.

Current implementations:

| Provider | Package | Notes |
| --- | --- | --- |
| LDAP | `extensions/auth/ldap` | Service-account search plus user bind against LDAP-compatible directories. |
