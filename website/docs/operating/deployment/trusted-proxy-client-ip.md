# Trusted Proxy Headers

Use this setting when `vectis-api` runs behind a reverse proxy or load balancer and you want Vectis to trust selected forwarded headers from that proxy. Trusted proxy headers affect the resolved client IP used by rate limits, audit logs, and API access logs. They also let Vectis recognize that the original browser-facing request used HTTPS when the API itself receives plaintext traffic from a trusted TLS-terminating proxy.

By default, Vectis does not trust forwarded headers. It uses the TCP peer address from `RemoteAddr`.

## When To Configure It

Configure trusted proxy CIDRs only when both are true:

1. All client traffic reaches `vectis-api` through proxies or load balancers you control.
2. You know the source IP ranges those proxies use when connecting to the API.

Do not configure this for direct internet clients. A client can set forwarded headers itself, so Vectis only reads them when the TCP peer is inside one of your trusted proxy CIDRs.

## Configuration

| Method | Setting |
| --- | --- |
| Environment | `VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS` as a comma-separated list |
| TOML | `api.client_ip.trusted_proxy_cidrs` as an array |

Examples:

```sh
VECTIS_API_CLIENT_IP_TRUSTED_PROXY_CIDRS=10.0.0.10,10.0.1.0/24
```

```toml
[api.client_ip]
trusted_proxy_cidrs = ["10.0.0.10", "10.0.1.0/24"]
```

Bare IPs are accepted and treated as a single-host range: IPv4 as `/32`, IPv6 as `/128`.

`0.0.0.0/0` and `::/0` are rejected at startup. Trusting the whole address space would let arbitrary clients forge their client IP.

## How Vectis Chooses The Client IP

When `trusted_proxy_cidrs` is empty, Vectis always uses the TCP peer IP.

When the TCP peer is not inside a trusted CIDR, Vectis ignores `X-Forwarded-For` and `X-Real-IP` and uses the TCP peer IP.

When the TCP peer is inside a trusted CIDR, Vectis chooses in this order:

1. The first valid IP in `X-Forwarded-For`, reading left to right.
2. `X-Real-IP`, if it contains one valid IP.
3. The TCP peer IP from `RemoteAddr`.

This means your proxy should set or sanitize `X-Forwarded-For` consistently. If your proxy appends to an incoming header without clearing untrusted client-provided values, the left-most value may be attacker-controlled.

## How Vectis Detects Original HTTPS

Direct TLS requests are always treated as HTTPS.

When the TCP peer is not inside a trusted CIDR, Vectis ignores `X-Forwarded-Proto` and `Forwarded` and treats the request as plaintext unless the API connection itself uses TLS.

When the TCP peer is inside a trusted CIDR, Vectis treats the original request as HTTPS when either header reports `https`:

1. `X-Forwarded-Proto: https`
2. `Forwarded: proto=https`

This controls request-aware API `Secure` session-cookie inference and whether the API emits its configured `Strict-Transport-Security` policy on a response that arrived through a trusted TLS-terminating proxy. When API auth is enabled behind an HTTPS edge, still set `api.session.cookie_secure = true` explicitly: trusted proxy headers do not satisfy startup secure-cookie validation because direct HTTP bypasses could issue insecure cookies.

## What This Affects

| Area | Impact |
| --- | --- |
| Unauthenticated rate limits | The client IP becomes part of the rate-limit key. Without this setting, all clients behind one proxy may share one bucket. |
| Audit logs | Audit `IPAddress` fields use the resolved client IP. |
| API access logs | Structured HTTP access log lines include the resolved `client_ip` field when access logs are enabled. |
| Browser session cookies | Trusted forwarded `https` lets the API set `Secure` cookies when TLS terminates at the proxy. |
| HSTS | Trusted forwarded `https` lets the API emit its configured `Strict-Transport-Security` policy through a TLS-terminating proxy. |

This setting does not authenticate the client and does not replace API auth, TLS, or network policy.

## Safe Setup Checklist

1. Identify the exact proxy or load-balancer source CIDRs that connect to `vectis-api`.
2. Configure only those CIDRs.
3. Make sure the proxy overwrites or safely normalizes `X-Forwarded-For`, `X-Real-IP`, `X-Forwarded-Proto`, and `Forwarded`.
4. Confirm direct clients cannot connect around the proxy.
5. Enable API access logs temporarily and check that `client_ip` matches the real client you expect.
6. Confirm browser logins receive `Secure` cookies when requests arrive through the proxy over HTTPS.
7. Watch rate-limit behavior after rollout, especially on login and setup routes.

## Related Documentation

| Topic | Document |
| --- | --- |
| API auth and security posture | [Security](../../concepts/security.md) |
| Configuration reference | [Configuration](../configuration.md) |
| Reference deployment posture | [Reference Deployment Posture](./reference-deployment-posture.md) |
