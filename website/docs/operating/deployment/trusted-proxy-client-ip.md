# Trusted Proxy Client IP

Use this setting when `vectis-api` runs behind a reverse proxy or load balancer and you want rate limits, audit logs, and API access logs to record the original client IP instead of the proxy's IP.

By default, Vectis does not trust forwarded headers. It uses the TCP peer address from `RemoteAddr`.

## When To Configure It

Configure trusted proxy CIDRs only when both are true:

1. All client traffic reaches `vectis-api` through proxies or load balancers you control.
2. You know the source IP ranges those proxies use when connecting to the API.

Do not configure this for direct internet clients. A client can set `X-Forwarded-For` itself, so Vectis only reads forwarded headers when the TCP peer is inside one of your trusted proxy CIDRs.

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

## What This Affects

| Area | Impact |
| --- | --- |
| Unauthenticated rate limits | The client IP becomes part of the rate-limit key. Without this setting, all clients behind one proxy may share one bucket. |
| Audit logs | Audit `IPAddress` fields use the resolved client IP. |
| API access logs | Structured HTTP access log lines include the resolved `client_ip` field when access logs are enabled. |

This setting does not authenticate the client and does not replace API auth, TLS, or network policy.

## Safe Setup Checklist

1. Identify the exact proxy or load-balancer source CIDRs that connect to `vectis-api`.
2. Configure only those CIDRs.
3. Make sure the proxy overwrites or safely normalizes `X-Forwarded-For`.
4. Confirm direct clients cannot connect around the proxy.
5. Enable API access logs temporarily and check that `client_ip` matches the real client you expect.
6. Watch rate-limit behavior after rollout, especially on login and setup routes.

## Related Documentation

| Topic | Document |
| --- | --- |
| API auth and security posture | [Security](../../concepts/security.md) |
| Configuration reference | [Configuration](../configuration.md) |
| Reference deployment posture | [Reference Deployment Posture](./reference-deployment-posture.md) |
