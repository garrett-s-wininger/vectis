# Trusted reverse proxy and client IP

When the HTTP API sits behind a reverse proxy or load balancer, the TCP peer address (`RemoteAddr`) is the proxy, not the end user. That breaks **unauthenticated rate limits** (everyone shares one bucket) and makes **audit log IP** fields less useful.

## Configuration

- **TOML:** [`api.client_ip.trusted_proxy_cidrs`](../../internal/config/defaults.toml) — array of CIDR strings (or bare IPs, treated as `/32` or `/128`).
- **Environment:** `VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS` — comma-separated list; overrides TOML when set (non-empty).

When `trusted_proxy_cidrs` is **empty** (default), the API **never** trusts `X-Forwarded-For` or `X-Real-IP`: the client IP is always taken from `RemoteAddr`.

When the peer address is **inside** one of the configured CIDRs:

1. The **first valid IP** in `X-Forwarded-For` (left to right, comma-separated) is used.
2. If none, **`X-Real-IP`** is used if it parses as a single IP.
3. Otherwise the peer IP from `RemoteAddr` is used.

If the peer is **not** in a trusted CIDR, forwarded headers are **ignored** so arbitrary clients cannot spoof `X-Forwarded-For`.

`0.0.0.0/0` and `::/0` are **rejected** at startup: listing “the whole internet” as trusted would allow any client to supply a forged forwarded chain.

## Operational notes

- List only subnets from which **your** proxies connect (e.g. link-local to a sidecar, or an internal LB range).
- Your proxy must **append or replace** `X-Forwarded-For` in a way you trust; the API assumes the **left-most** address is the original client (common when each hop appends).
- This affects **rate limit keys** for unauthenticated routes, **audit** `IPAddress` fields, and the **`client_ip`** field on structured HTTP access log lines (`http_request` events).
