# Federation and multi-site deployment (deferred)

This document preserves a **target** design for multi-site Vectis. It is **not implemented** in the codebase. For the current single-site architecture, see [PLANNING.md](PLANNING.md) §2.

## Architecture overview

Vectis supports multi-site deployments where each site operates independently but shares centralized configuration. This avoids WAN complexity in orchestration while providing a single pane of glass for operators.

**Database architecture:** Centralized database with read replicas per site for HA, or single centralized database for non-HA setups.

**Database replication:** Follow database vendor recommendations (PostgreSQL streaming replication, etc.). Vectis does not implement custom replication — relies on the database's native capabilities.

```
┌─────────────────────────────────────────────────────────────────┐
│                      CENTRAL SERVICES                           │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐           │
│  │   Projects   │  │   Secrets    │  │   Accounts   │           │
│  │  (Central)   │  │  (Vault)     │  │  (Frontend)  │           │
│  │     DB       │  │              │  │              │           │
│  └──────────────┘  └──────────────┘  └──────────────┘           │
└──────────────────────┬──────────────────────────────────────────┘
                       │ WAN (read-replicas for HA)
         ┌─────────────┼─────────────┐
         ▼             ▼             ▼
    ┌──────────┐  ┌──────────┐  ┌──────────┐
    │  Site A  │  │  Site B  │  │  Site C  │
    │ (us-east)│  │ (eu-west)│  │ (ap-south│
    │          │  │          │  │          │
    │ Queue    │  │ Queue    │  │ Queue    │
    │ Workers  │  │ Workers  │  │ Workers  │
    │ Logs     │  │ Logs     │  │ Logs     │
    └──────────┘  └──────────┘  └──────────┘
```

## Site model

Each deployment site consists of:

- **Queue service** — Local job queuing and scheduling
- **Workers** — Local execution capacity
- **Log service** — Local log storage (accessed via site-specific endpoint)
- **Heartbeat service** (target) — Local heartbeat tracking and orphan detection
- **API Server** — Local API endpoint, healthcheck-enabled

Sites are independent for execution — no cross-site job routing in orchestration.

## Centralized vs local data

| Data | Storage | Replication |
| --- | --- | --- |
| **Projects** | Central DB | Read-replicas per site |
| **Secrets** | External (Vault, etc.) | Shared access |
| **Accounts** | Central (Frontend) | Central only |
| **Pipelines** | Git repos | Cloned per site |
| **Jobs** | Local per site | None |
| **Logs** | Local per site | None |
| **Artifacts** | Local per site | None |

## Site routing

Jobs specify which site they should run on:

```yaml
# .vectis.yml
name: "Build Service"

site: "us-east-1"  # Specific site, or "any" for current (trigger's) site
```

**Default site** is configured at cluster level:

```toml
[cluster]
default_site = "us-east-1"

[sites]
  [sites.us-east-1]
    api_addr = "https://vectis-east.example.com"
  [sites.eu-west-1]
    api_addr = "https://vectis-eu.example.com"
```

When a trigger submits a job:

1. Trigger reads project config to determine site
2. If site is "any", uses default site
3. Trigger submits directly to that site's API Server

## Frontend aggregation

The frontend provides a unified view across sites:

- **Single-site (default):** No site configuration needed — frontend connects directly to local API Server
- **Multi-site:** Frontend auto-discovers sites from cluster configuration
- **Healthchecks:** Frontend probes each site's API Server periodically (multi-site only)
- **Cross-site logs:** Frontend opens **SSE** to that site's API and/or log service where the job is running (multi-site only)
- **Status aggregation:** Jobs shown in context of their site (multi-site only)

```toml
# Multi-site (explicit configuration)
[frontend]
sites = ["us-east-1", "eu-west-1", "ap-south"]
```

## Secrets integration

Secrets are retrieved from an external secret manager (Vault, Knox, etc.) at job execution time. Since each site connects to the same secret manager, secrets are consistently available regardless of which site runs the job.

For development/testing, the built-in encrypted filesystem backend works for single-site or multi-site deployments.

## Trigger site selection

Triggers determine site at job submission time:

1. Trigger fetches project configuration (from central DB)
2. Project config specifies `site` (e.g., "us-east-1") or "any"
3. If "any", use current site's API Server (the trigger's site)
4. Submit job to appropriate site's API Server

This keeps site selection simple and explicit — no complex routing logic.
