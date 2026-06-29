# Configuration Key Reference

This page catalogs the embedded configuration keys Vectis loads from `internal/config/defaults.toml`. Use it when auditing a deployment, writing environment files, comparing staging and production, or checking whether a value is a Vectis default or an operator override.

For configuration workflow and common examples, start with [Configuration](../configuration.md). For the production minimum set, see [Production Config And Secrets Contract](../deployment/production-config-contract.md).

## How To Read This Page

| Column | Meaning |
| --- | --- |
| Key | Embedded configuration path. Dotted keys match the TOML structure and Viper lookup path. |
| Default | Value Vectis starts with before environment variables and flags are applied. |
| Operator note | What the setting controls or when to override it. |

Durations use Go-style strings such as `30s`, `5m`, or `24h`. Empty strings (`""`) mean the feature is unset or uses discovery/fallback behavior. Empty lists (`[]`) mean no entries are configured. `database.dsn` supports `{{data_home}}`, which resolves to the process data-home path at runtime.

Environment variables are described in [Configuration](../configuration.md#service-prefixes). In short: service binaries usually use their service prefix, while shared groups such as database, gRPC TLS, metrics TLS, dispatch TTL, API auth, API edge security, and action registry use global `VECTIS_*` names or explicit aliases.

## Identity And Dispatch

| Key | Default | Operator note |
| --- | --- | --- |
| `cell.id` | `local` | Local cell identity. Override with `VECTIS_CELL_ID` in multi-cell deployments. |
| `dispatch.start_ttl` | `24h` | Maximum time a root or task execution may wait before start; expired dispatches fail with `dispatch_expired`. |

## API

| Key | Default | Operator note |
| --- | --- | --- |
| `api.host` | `localhost` | API HTTP bind host. |
| `api.port` | `8080` | API HTTP bind port. |
| `api.log_format` | `text` | API access log format. |
| `api.log.address` | `""` | Optional log service address override for API log reads. |
| `api.cell_ingress.endpoints` | `[]` | Static cell ingress routes in `cell_id=url` form. |
| `api.auth.enabled` | `false` | Enables protected API authentication. |
| `api.auth.bootstrap_token` | `""` | Setup bootstrap token for a new auth-enabled database. |
| `api.authz.engine` | `hierarchical_rbac` | Authorization engine, normally `hierarchical_rbac` or `authenticated_full`. |
| `api.audit.enabled` | `true` | Enables API audit event emission. |
| `api.audit.durability_overrides` | `""` | Optional comma-separated audit durability overrides. |
| `api.tls.cert_file` | `""` | Direct browser-facing API TLS certificate path. |
| `api.tls.key_file` | `""` | Direct browser-facing API TLS private key path. |
| `api.tls.reload_interval` | `0s` | Poll interval for API TLS file reload; `0s` disables polling. |
| `api.hsts.max_age_seconds` | `31536000` | HSTS max age for HTTPS API responses. |
| `api.hsts.include_subdomains` | `false` | Adds `includeSubDomains` to API HSTS when true. |
| `api.hsts.preload` | `false` | Adds `preload` to API HSTS when the policy is preload-valid. |
| `api.session.ttl` | `168h` | Absolute browser session lifetime. |
| `api.session.idle_ttl` | `24h` | Idle browser session lifetime. |
| `api.session.cookie_secure` | `false` | Requires secure browser cookies; set true behind production HTTPS. |
| `api.session.allow_insecure_cookies` | `false` | Local-only escape hatch for insecure browser cookies. |
| `api.cache.backend` | `database` | API session and rate-limit cache backend. |
| `api.rate_limit.auth_refill_rate` | `12s` | Refill period for authentication route rate limits. |
| `api.rate_limit.auth_burst_size` | `5` | Burst size for authentication route rate limits. |
| `api.rate_limit.token_refill_rate` | `3s` | Refill period for token-management route rate limits. |
| `api.rate_limit.token_burst_size` | `20` | Burst size for token-management route rate limits. |
| `api.rate_limit.general_refill_rate` | `600ms` | Refill period for general API route limits. |
| `api.rate_limit.general_burst_size` | `150` | Burst size for general API route limits. |
| `api.client_ip.trusted_proxy_cidrs` | `[]` | Direct proxy CIDRs trusted for forwarded client IP and original scheme. |
| `api.host_validation.allowed_hosts` | `[]` | Browser-facing API Host allowlist; empty allows bind host plus loopback. |
| `api.cors.allowed_origins` | `[]` | Exact browser origins allowed for credentialed CORS. |

## Queue, Orchestrator, And Registry

| Key | Default | Operator note |
| --- | --- | --- |
| `queue.port` | `8081` | Queue gRPC port. |
| `queue.metrics_host` | `localhost` | Queue metrics bind host. |
| `queue.metrics_port` | `9081` | Queue metrics port. |
| `queue.register_with_registry` | `true` | Registers queue with the registry when discovery is enabled. |
| `queue.advertise_address` | `""` | Queue address advertised through registry; empty derives from listener context. |
| `orchestrator.port` | `8087` | Orchestrator gRPC port. |
| `orchestrator.metrics_host` | `localhost` | Orchestrator metrics bind host. |
| `orchestrator.metrics_port` | `9090` | Orchestrator metrics port. |
| `orchestrator.shards` | `0` | Number of task-state shards; `0` lets the service choose its default. |
| `orchestrator.register_with_registry` | `true` | Registers orchestrator with the registry when discovery is enabled. |
| `orchestrator.advertise_address` | `""` | Orchestrator address advertised through registry. |
| `registry.port` | `8082` | Registry gRPC port. |
| `registry.cluster.node_id` | `""` | Stable node ID for clustered registry gossip. |
| `registry.cluster.advertise_address` | `""` | Registry gossip advertise address. |
| `registry.cluster.peer_addresses` | `[]` | Static registry peer addresses. |
| `registry.cluster.gossip_interval` | `2s` | Periodic gossip interval. |
| `registry.cluster.anti_entropy_interval` | `30s` | Full anti-entropy sync interval. |
| `registry.cluster.lease_ttl` | `2m` | Registry service lease TTL. |
| `registry.cluster.tombstone_ttl` | `5m` | Registry tombstone retention TTL. |
| `registry.cluster.peer_dial_timeout` | `3s` | Timeout for registry peer dials. |

## Logs, Artifacts, And Forwarding

| Key | Default | Operator note |
| --- | --- | --- |
| `log.host` | `localhost` | Log service host for HTTP/SSE surfaces. |
| `log.metrics_host` | `localhost` | Log service metrics bind host. |
| `log.metrics_port` | `9083` | Log service metrics port. |
| `log.max_run_buffers` | `1024` | Maximum live in-memory run buffers. |
| `log.storage_read_only_min_free_bytes` | `1073741824` | Free-byte floor before log storage turns read-only. |
| `log.grpc.port` | `8083` | Log gRPC ingest/read port. |
| `log.grpc.register_with_registry` | `true` | Registers log gRPC endpoint with the registry. |
| `log.grpc.advertise_address` | `""` | Log gRPC address advertised through registry. |
| `artifact.metrics_host` | `localhost` | Artifact metrics bind host. |
| `artifact.metrics_port` | `9089` | Artifact metrics port. |
| `artifact.storage_read_only_min_free_bytes` | `1073741824` | Free-byte floor before artifact storage turns read-only. |
| `artifact.grpc.port` | `8086` | Artifact gRPC port. |
| `artifact.grpc.register_with_registry` | `true` | Registers artifact gRPC endpoint with the registry. |
| `artifact.grpc.advertise_address` | `""` | Artifact gRPC address advertised through registry. |
| `log_forwarder.metrics_host` | `localhost` | Log forwarder metrics bind host. |
| `log_forwarder.metrics_port` | `9088` | Log forwarder metrics port. |

## Secrets And Action Registry

| Key | Default | Operator note |
| --- | --- | --- |
| `secrets.port` | `8090` | Secrets gRPC port. |
| `secrets.metrics_host` | `localhost` | Secrets metrics bind host. |
| `secrets.metrics_port` | `9091` | Secrets metrics port. |
| `secrets.encryptedfs.root` | `""` | Root directory for encryptedfs secret envelopes. |
| `secrets.encryptedfs.key_file` | `""` | Key file for encryptedfs envelopes. |
| `secrets.policy.allow` | `[]` | Default-deny secret policy allow rules. |
| `action_registry.local_roots` | `[]` | Local custom action manifest roots. |
| `action_registry.allowed_namespaces` | `[]` | Optional custom action namespace allowlist. |
| `action_registry.allowed_sources` | `[]` | Optional custom action source allowlist. |
| `action_registry.require_digest_pins` | `false` | Requires custom actions to use digest-pinned references. |

## Discovery, Cell Ingress, And Service Identity

| Key | Default | Operator note |
| --- | --- | --- |
| `discovery.registry_resolver_refresh` | `10s` | Refresh interval for registry-backed resolvers. |
| `discovery.registry_resolver_poll_timeout` | `5s` | Poll timeout for registry-backed resolver waits. |
| `discovery.registry_resolver_error_refresh` | `2s` | Refresh interval after resolver errors. |
| `discovery.registry_registration_refresh` | `45s` | Service registration refresh interval. |
| `discovery.registry.addresses` | `[]` | Registry resolver addresses. |
| `discovery.orchestrator.address` | `""` | Static orchestrator address fallback. |
| `cell_ingress.host` | `localhost` | Private cell ingress HTTP bind host. |
| `cell_ingress.port` | `8085` | Private cell ingress HTTP port. |
| `cell_ingress.metrics_host` | `localhost` | Cell ingress metrics bind host. |
| `cell_ingress.metrics_port` | `9087` | Cell ingress metrics port. |
| `cell_ingress.repair_interval` | `30s` | Cell ingress repair scan interval. |
| `cell_ingress.registry.address` | `""` | Registry address used by cell ingress. |
| `cell_ingress.queue.address` | `""` | Queue address used by cell ingress. |
| `service_identity.registry_allowed_client_identities` | `[]` | SPIFFE URI SAN allowlist for registry clients. |
| `service_identity.queue_allowed_client_identities` | `[]` | SPIFFE URI SAN allowlist for queue clients. |
| `service_identity.log_allowed_client_identities` | `[]` | SPIFFE URI SAN allowlist for log clients. |
| `service_identity.artifact_allowed_client_identities` | `[]` | SPIFFE URI SAN allowlist for artifact clients. |
| `service_identity.orchestrator_allowed_client_identities` | `[]` | SPIFFE URI SAN allowlist for orchestrator clients. |
| `service_identity.worker_control_allowed_client_identities` | `[]` | SPIFFE URI SAN allowlist for worker-control clients. |
| `service_identity.secrets_allowed_client_identities` | `[]` | Optional static allowlist for secrets callers; keep empty for dynamic execution SVID callers unless deliberately adding a second gate. |
| `service_identity.cell_ingress_allowed_producer_identities` | `[]` | SPIFFE URI SAN allowlist for cell ingress producers. |

## Database

| Key | Default | Operator note |
| --- | --- | --- |
| `database.driver` | `sqlite3` | SQL driver; use `pgx` for Postgres production deployments. |
| `database.dsn` | `{{data_home}}/vectis/db.sqlite3` | Default SQLite DSN template. |
| `database.pgx.plan_cache_mode` | `""` | Optional pgx Postgres plan cache mode override; empty leaves the driver default. |
| `database.pgx_pool.max_open_conns` | `25` | Max open Postgres connections per process. |
| `database.pgx_pool.max_idle_conns` | `10` | Max idle Postgres connections per process. |
| `database.pgx_pool.conn_max_lifetime` | `1h` | Maximum Postgres connection lifetime. |
| `database.pgx_pool.conn_max_idle_time` | `15m` | Maximum Postgres idle connection lifetime. |

Production deployments normally set `VECTIS_DATABASE_DRIVER=pgx` and either one shared `VECTIS_DATABASE_DSN` or role-specific `VECTIS_GLOBAL_DATABASE_DSN` and `VECTIS_CELL_DATABASE_DSN`.

## Source Control

| Key | Default | Operator note |
| --- | --- | --- |
| `source.checkout_root` | `{{data_home}}/vectis/source-checkouts` | Managed source checkout root. |
| `source.sync_configured_repositories_on_startup` | `false` | Sync enabled configured repositories during API startup. |
| `source.sync_configured_repositories_interval` | `0s` | Background refresh interval for enabled configured repositories; `0s` disables periodic sync. |
| `source.sync_configured_repositories_max_concurrency` | `1` | Maximum concurrent configured-repository syncs. |
| `source.sync_configured_repositories_failure_backoff` | `5m` | Delay before retrying repositories with recent failed syncs. |
| `source.sync_running_timeout` | `15m` | Timeout for a single source repository sync reservation. |
| `source.repositories` | `[]` | Static source repository declarations; set `worker_cache_mode=persistent` for repositories workers should keep warm. |
| `source.schedules` | `[]` | Static source-backed schedule declarations. |

## Worker

| Key | Default | Operator note |
| --- | --- | --- |
| `worker.metrics_host` | `localhost` | Worker metrics bind host. |
| `worker.metrics_port` | `9082` | Worker metrics port. |
| `worker.register_with_registry` | `true` | Registers worker control endpoint with the registry when applicable. |
| `worker.orchestrator.address` | `""` | Static orchestrator address override. |
| `worker.secrets.address` | `localhost:8090` | Secrets service address. |
| `worker.artifact_max_bytes` | `1073741824` | Max bytes for one artifact upload; `0` disables the worker-level cap. |
| `worker.artifact_max_run_bytes` | `10737418240` | Max artifact bytes recorded for one run; `0` disables the cap. |
| `worker.artifact_max_count` | `1000` | Max artifact manifests recorded for one run; `0` disables the cap. |
| `worker.queue.dequeue_poll_base_interval` | `250ms` | Base queue dequeue retry interval after an empty poll. |
| `worker.queue.dequeue_poll_jitter_ratio` | `0.2` | Queue dequeue retry jitter ratio applied around the poll interval. |
| `worker.queue.dequeue_poll_max_interval` | `1s` | Maximum queue dequeue retry interval. |
| `worker.queue.dequeue_sticky_success_budget` | `64` | Number of successful sticky dequeues before a worker rebalances shard probing. |
| `worker.queue.continuation_inline_job_max_bytes` | `65536` | Max continuation job payload size kept inline before spillover behavior. |
| `worker.execution.backend` | `host` | Worker-core execution backend. |
| `worker.execution.workspace_root` | `""` | Workspace root used by execution backend. |
| `worker.execution.lima.path` | `limactl` | Lima CLI path for VM-backed execution. |
| `worker.execution.lima.instance` | `""` | Lima instance name. |
| `worker.execution.lima.guest_workspace_root` | `""` | Guest-visible workspace root for Lima execution. |
| `worker.execution.lima.start` | `false` | Whether worker-core starts the Lima instance. |
| `worker.execution.lima.preserve_env` | `false` | Whether Lima execution preserves selected host environment. |
| `worker.control.mode` | `static` | Worker-control port allocation mode. |
| `worker.control.port` | `9084` | Static worker-control gRPC port. |
| `worker.control.port_min` | `20000` | Dynamic worker-control port range minimum. |
| `worker.control.port_max` | `30000` | Dynamic worker-control port range maximum. |
| `worker.execution_identity.enabled` | `false` | Enables per-execution identity derivation. |
| `worker.execution_identity.trust_domain` | `""` | SPIFFE trust domain for derived execution identities. |
| `worker.execution_identity.path_template` | `/cell/{cell}/namespace/{namespace}/job/{job}/run/{run}/execution/{execution}` | Template for derived execution SPIFFE paths. |
| `worker.spiffe.enabled` | `false` | Requires worker execution SVID acquisition before action code runs. |
| `worker.spiffe.workload_api_address` | `""` | SPIFFE Workload API address. |
| `worker.spiffe.fetch_timeout` | `5s` | Maximum SVID fetch time. |
| `worker.spiffe.registration.enabled` | `false` | Enables worker-controlled SPIFFE Entry API registration. |
| `worker.spiffe.registration.server_address` | `""` | SPIFFE Entry API Unix socket address. |
| `worker.spiffe.registration.parent_id` | `""` | Parent SPIFFE ID for execution entries. |
| `worker.spiffe.registration.selectors` | `[]` | Trusted selectors for execution entries. |
| `worker.spiffe.registration.x509_svid_ttl` | `0s` | Optional SVID TTL; `0s` leaves authority default in effect. |
| `worker.spiffe.registration.min_ttl` | `0s` | Optional minimum accepted registration lifetime. |
| `worker.spiffe.registration.max_ttl` | `0s` | Optional maximum accepted registration lifetime. |

## Cron, Reconciler, And Catalog

| Key | Default | Operator note |
| --- | --- | --- |
| `cron.claim_ttl` | `5m` | Scheduler claim TTL for cron trigger processing. |
| `reconciler.interval` | `30s` | Reconciler scan interval. |
| `reconciler.lease_ttl` | `2m` | Reconciler service lease TTL. |
| `reconciler.redispatch_limit` | `1000` | Maximum executions the reconciler redispatches in one pass. |
| `reconciler.metrics_host` | `localhost` | Reconciler metrics bind host. |
| `reconciler.metrics_port` | `9085` | Reconciler metrics port. |
| `catalog.interval` | `1s` | Catalog processor drain interval. |
| `catalog.batch_size` | `100` | Catalog events processed per batch. |
| `catalog.metrics_host` | `localhost` | Catalog metrics bind host. |
| `catalog.metrics_port` | `9086` | Catalog metrics port. |

## TLS

| Key | Default | Operator note |
| --- | --- | --- |
| `grpc_tls.insecure` | `true` | Disables internal gRPC TLS by default for local development. |
| `grpc_tls.ca_file` | `""` | Root CA bundle for internal gRPC clients. |
| `grpc_tls.cert_file` | `""` | Internal gRPC server certificate. |
| `grpc_tls.key_file` | `""` | Internal gRPC server private key. |
| `grpc_tls.client_ca_file` | `""` | Client CA bundle for mTLS listeners. |
| `grpc_tls.client_cert_file` | `""` | Internal gRPC client certificate. |
| `grpc_tls.client_key_file` | `""` | Internal gRPC client private key. |
| `grpc_tls.server_name` | `""` | Override server name for internal gRPC verification. |
| `grpc_tls.reload_interval` | `0s` | Poll interval for internal gRPC TLS reload; `0s` disables polling. |
| `metrics_tls.insecure` | `true` | Disables dedicated metrics TLS by default for local development. |
| `metrics_tls.cert_file` | `""` | Metrics listener certificate. |
| `metrics_tls.key_file` | `""` | Metrics listener private key. |
| `metrics_tls.reload_interval` | `0s` | Poll interval for metrics TLS reload; `0s` disables polling. |

## Env-Only And Runtime Settings

Some important operator knobs are flags or explicit environment variables rather than embedded TOML defaults.

| Setting | Used by | Purpose |
| --- | --- | --- |
| `VECTIS_LOG_FORMAT` | Most binaries | Process log format, commonly `json` in production. |
| `VECTIS_LOG_DIR` | Most binaries | Mirrors service logs to files under this directory. |
| `VECTIS_API_TOKEN` | `vectis-cli` | Overrides the saved CLI bearer token for one shell or automation process. |
| `VECTIS_GLOBAL_DATABASE_DSN` | Global-control services | Role-specific global database DSN. |
| `VECTIS_CELL_DATABASE_DSN` | Cell-local services | Role-specific cell-local database DSN. |
| `VECTIS_QUEUE_POOL` | `vectis-queue` | Queue pool name for delivery routing and default persistence paths. |
| `VECTIS_QUEUE_INSTANCE_ID` | `vectis-queue` | Stable queue shard instance ID. |
| `VECTIS_QUEUE_PERSISTENCE_DIR` | `vectis-queue` | Queue WAL/snapshot directory; empty disables persistence. |
| `VECTIS_LOG_STORAGE_DIR` | `vectis-log` | Durable log storage directory. |
| `VECTIS_ARTIFACT_STORAGE_DIR` | `vectis-artifact` | Durable artifact CAS directory. |
| `VECTIS_LOG_FORWARDER_SPOOL_DIR` | `vectis-log-forwarder` | Durable forwarder spool directory. |
| `VECTIS_CATALOG_CELL_DATABASE_DSNS` | `vectis-catalog` | Per-cell source database DSNs for catalog fan-in. |
| `VECTIS_LOCAL_*` | `vectis-local` | Dev-stack profile, local TLS, docs, cells, and local SPIFFE smoke-test settings. |

## Related Docs

| Need | Doc |
| --- | --- |
| Common operator configuration examples | [Configuration](../configuration.md) |
| Production minimum config and secret contract | [Production Config And Secrets Contract](../deployment/production-config-contract.md) |
| Production environment-file skeleton | [Production Environment Template](../deployment/production-env-template.md) |
| Default ports and scrape surfaces | [Metrics Catalog](./metrics-catalog.md) |
| Runtime health and config-adjacent checks | [Health Check Catalog](./health-check-catalog.md) |
