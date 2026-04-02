# Failure domains, dependency outages, and expectations

This document is for **operators and deployment planning**. It explains how failures are isolated by component, what happens when a dependency is unavailable, reasonable **operational expectations**, and how the current system compares to a heavily hardened production setup. For topology and protocols, see [ARCHITECTURE.md](ARCHITECTURE.md); for the same material in planning context, see [PLANNING.md](PLANNING.md) §2. For vocabulary (**queue**, **run**, **dispatch**, etc.), see [GLOSSARY.md](GLOSSARY.md). For **trust boundaries and secrets**, see [SECURITY.md](SECURITY.md).

## Operational spine

The **database** and **queue** are the pair to protect first. The database is the **durable source of truth**—definitions, runs, schedules, and worker leases. The queue is **where work waits and is handed off** from producers (API, cron, reconciler) to workers. Registry, log, API, and the rest all matter, but **most systemic outages** are explained by database health, queue health (including on-disk persistence when it is enabled), or the handoff between them.

## Dependency overview (as implemented)

| Component | Depends on (runtime) |
| --- | --- |
| **vectis-api** | Database; **queue** (fixed address or **registry**) |
| **vectis-queue** | Optional **registry** (when configured to register and publish its address) |
| **vectis-log** | Optional **registry** (when configured to register) |
| **vectis-worker** | **Registry** (unless queue and log addresses are pinned in config); **queue**; **log**; **database** |
| **vectis-cron** | **Database**; **queue** |
| **vectis-reconciler** | **Database**; **queue** |
| **Clients** (browser, CLI, automation) | **API** over HTTP |

If queue and log addresses are **pinned** in configuration, consumers do not need the registry to find them. Components that **register** themselves still need the registry up at startup unless you change that deployment pattern.

---

## By component

### Database

**Failure domain:** One logical database holds job definitions, runs, schedules, and worker lease information. It is the **source of truth** for what should run and what state each run is in. Supported engines include **SQLite** and **PostgreSQL**; outage behavior is the same at this layer.

**If the database is offline or returning errors**

- **API:** Creating or updating jobs, triggering runs, listing runs, and healthy startup (including schema migrations) fail or the API process may exit during startup. After the process is up, many handlers treat **transient database unavailability** (connection loss, certain driver and network errors classified in [`internal/database/errors.go`](../internal/database/errors.go)) as **HTTP 503 Service Unavailable**, so load balancers and clients can distinguish “dependency down” from arbitrary **500** failures. **404** semantics for missing resources are preserved where applicable (e.g. get-by-id flows check not-found before the unavailable path).
- **Worker:** Cannot claim work, keep leases, or record success or failure.
- **Cron:** Cannot read schedules or record activity; process typically exits on startup if the database is unreachable.
- **Reconciler:** Cannot find stuck runs or submit them to the queue; process typically exits on startup if the database is unreachable.

**Expectations (what Vectis provides vs what you run)**

- The application ships **embedded schema migrations** and talks to **one configured SQL backend** at a time (SQLite or PostgreSQL). It does **not** implement connection failover, replica routing, or backup/restore—those are entirely the operator’s and database platform’s concern.
- For **PostgreSQL** (`pgx`), each process configures the shared `*sql.DB` pool (**max open/idle connections**, **connection max lifetime / idle time**) via [CONFIGURATION.md](CONFIGURATION.md#postgresql-connection-pool-pgx-only) and [`internal/config/defaults.toml`](../internal/config/defaults.toml). That bounds total connections across many processes and helps shed stale connections after network blips; it is **not** HA by itself.
- Every component that writes state must see the **same** logical database and schema. Multi-instance API/workers are only safe when the backend gives you the concurrency semantics you need (see [PLANNING.md](PLANNING.md) §2.5 for persistence scope and roadmap).

**Where we are now**

- No separate “read-only” or degraded API mode when the database is unhealthy.
- **503** on classified unavailable errors is implemented on **key read and write** API paths, not a full “every possible error code” matrix.

---

### vectis-registry

**Failure domain:** When used, the registry tells callers where the **queue** and **log** services listen. It matters whenever addresses are not fully pinned in config.

**If the registry is offline**

- **API, cron, reconciler:** Usually **cannot start** until they can reach the queue (which may require the registry to resolve the queue address).
- **Worker:** Usually **cannot start** without the registry unless both queue and log addresses are pinned.
- **Queue and log:** If they are configured to register, **startup fails** until they can register successfully.

**Expectations**

- Bring the registry up with—or before—dependents, or use **pinned** queue/log addresses so discovery is not on the critical path for those processes. For resilience, plan for redundant registry instances or fixed bootstrap addresses (beyond what this repo prescribes).

**Where we are now**

- A single registry deployment is the common pattern. Discovery refresh behavior is tunable via configuration (discovery-related settings).

---

### vectis-queue

**Failure domain:** Buffers work between **producers** (API, cron, reconciler) and **workers**. By default the queue can **persist** its backlog and delivery state to disk under a configured data directory; turning persistence off keeps everything in memory only.

**If the queue is unavailable**

- **API:** Triggers and ephemeral runs can still be **accepted** (HTTP 202) after the run is recorded in the database; handing work to the queue happens **asynchronously** with a **small number of retries**. If that handoff keeps failing, the failure is **logged** and the run may sit in “queued” in the database until the **reconciler** tries again.
- **Worker:** Pulling work fails; the worker **backs off** and retries rather than exiting immediately.
- **Cron / reconciler:** Submissions during the outage fail for that cycle; the reconciler continues to run and logs issues per run where applicable.

**If the queue process restarts**

- **Persistence on:** Pending work and in-flight delivery metadata are reloaded from disk when supported.
- **Persistence off:** In-memory work is **lost**; the database may still show runs as queued—the **reconciler** is meant to detect long-undispatched runs and submit them again.

**Expectations**

- Treat the queue as **durable** only when persistence is enabled and the data directory is on reliable storage. Expect **transient** outages to be retried by producers and workers (reconnect and limited enqueue retries).

**Where we are now**

- One active queue service; no built-in multi-writer or active/active queue cluster.

---

### vectis-log

**Failure domain:** Collects **log streams** from workers and serves them to clients. It does **not** own authoritative run status (the database does).

**If the log service is offline**

- **Worker:** The worker must open a log stream **before** running the job. If the log service is down or does not respond in time (on the order of **tens of seconds**), the **run fails** before meaningful job steps execute. There is no “run without central logging” path today.
- **Anyone consuming build logs:** Cannot receive log streams until the service is back. Run status in the API is separate from the log service.

**Expectations**

- Operators should see failures clearly in run status and messaging. A more available design would make logging optional or buffer locally (not implemented).

**Where we are now**

- Log service is a **hard dependency** for standard job execution.

---

### vectis-api

**Failure domain:** HTTP API for definitions, triggers, and run history. Workers already running a job do not need the API.

**If the API is offline**

- Clients cannot drive or query the system over HTTP.
- Queue, workers, log, cron, and reconciler keep running according to their own dependencies.

**Process lifecycle and probes**

- **Graceful shutdown:** On **SIGINT** / **SIGTERM**, the API stops accepting new HTTP connections and uses **`http.Server.Shutdown`** with a bounded timeout so in-flight requests and SSE streams can finish when clients disconnect cooperatively.
- **HTTP health (orchestration):** **`GET /health/live`** returns **200** if the process is serving (liveness). **`GET /health/ready`** returns **200** only when the API’s **database** ping succeeds and, when the server uses a managing queue client, the queue’s **gRPC connectivity** is **READY**—so readiness reflects “can this replica do work?” for common paths. Mock-only test servers may skip parts of this check.
- **Deploy manifests:** Wiring **`livenessProbe` / `readinessProbe`** in [`deploy/podman/kube-spec.yaml`](../deploy/podman/kube-spec.yaml) to these paths (or equivalents) is recommended for Kubernetes-style rollouts; the in-repo spec may not include probes yet.

**If the database or queue is unhealthy while the API is running**

- See the **Database** and **vectis-queue** sections: many API routes return **503** when the database error is classified as **unavailable**; queue connectivity affects **readiness** and enqueue paths, but the API still **fatals on startup** if it cannot connect to the queue before serving (no lazy dial yet).

**Expectations**

- Multiple API replicas do **not** share per-client session state; clients that rely on a single long-lived connection to one instance should plan for reconnects or use another instance explicitly.

**Where we are now**

- No built-in authentication on the HTTP API; restrict access at the network or edge.
- **Background enqueue** after HTTP **202** (`finishTriggerEnqueue` / `finishRunJobEnqueue`) still uses a **detached context**; there is no explicit wait for those goroutines during HTTP shutdown—the **reconciler** remains the backstop for runs stuck in **queued** in the database (see [adr/0001-async-enqueue-after-http-202.md](adr/0001-async-enqueue-after-http-202.md)).

---

### vectis-worker

**Failure domain:** Each worker process runs **one job at a time**. Workers coordinate **who owns a run** through the database.

**If workers are offline or overloaded**

- The queue and database can show work waiting; throughput drops until capacity returns.

**If a worker stops mid-run**

- **SIGINT / SIGTERM (graceful):** The worker stops **dequeuing** new work, but the **current** job—if any—continues on a **non-signal** context: **TryClaim**, **Ack**, **ExecuteJob**, **lease renewal**, and **MarkRunSucceeded / MarkRunFailed / MarkRunOrphaned** are not canceled by the same signal that stops dequeue. That avoids leaving a claimed run without a terminal row update when the process exits cleanly after the job finishes. **`SIGKILL`** and abrupt crashes do **not** run this path; leases, queue **delivery timeouts**, and reconciler behavior still apply.
- In all hard-stop cases, leases, queue **delivery timeouts**, and reconciler timing together determine whether work is retried or stuck. Tune queue persistence, delivery policy, and reconciler interval if you see stranded runs.

**If a worker loses database access mid-run**

- Lease renewal and final status updates may fail; runs may end as failed or remain in an ambiguous state until an operator or reconciler intervenes. The drain path above assumes the database becomes available again for finalize retries; a **long** outage during execution can still strand or fail runs.

**Where we are now**

- Scale by running more worker processes; no built-in autoscaling.
- **Remote cancel** of a specific run from the API is **not** implemented; stopping a run today is operational (worker/process lifecycle or external action), not an HTTP cancel RPC.

---

### vectis-cron

**Failure domain:** Fires **scheduled** work by submitting to the queue. Independent of the HTTP API.

**If cron is offline**

- Schedules do not run; **manual or API triggers** still work if the API and queue are healthy.

**Expectations**

- **Scale:** With a very large number of schedules, you typically **shard** work—each cron process (or group) owns a **partition** of jobs so evaluation stays bounded; that is separate from simply running duplicate full copies of the same schedule set.
- **Correctness / HA:** Each schedule should have **exactly one** firing path at a time. That may mean one leader-elected `vectis-cron`, **or** moving time-based triggers out of Vectis (e.g. your platform invokes the HTTP API on a cadence) so you are not running several uncoordinated `vectis-cron` replicas that would each enqueue the same tick.

**Where we are now**

- No built-in sharding or leader election; multiple instances without an external partitioning strategy can **double-fire** schedules—treat that as an operational risk.

---

### vectis-reconciler

**Failure domain:** Finds runs that are **queued in the database** but never successfully handed to the queue (for example after an accepted trigger during a queue outage), and tries to submit them again.

**If the reconciler is offline**

- Automatic cleanup of that gap **stops** until it runs again; the API and queue do not fully replace this behavior.

**Where we are now**

- Runs on a **configurable interval** and waits a **configurable minimum time** after queuing before attempting redispatch, to avoid fighting normal dispatch latency.

---

## Summary: current posture vs stronger expectations

| Area | Current behavior (short) | Stronger expectation (operations) |
| --- | --- | --- |
| **Registry** | Single discovery point; hard dependency at startup unless addresses are pinned | Redundancy, pins, or fixed service addresses |
| **Queue** | Optional disk persistence; retries on transient errors | Monitored depth, durable storage, capacity planning |
| **Log** | Required before a run executes | Optional or replicated logging if you need higher availability |
| **Database** | Embedded migrations; SQLite default and PostgreSQL supported; one DSN for all writers; **pgx** pool limits per process ([CONFIGURATION.md](CONFIGURATION.md#postgresql-connection-pool-pgx-only)); **503** on classified unavailable errors on many API paths; no in-app failover or replica routing | Roadmap: harden PostgreSQL + multi-instance story in-tree ([PLANNING.md](PLANNING.md) §2.5); backups/HA remain outside the codebase |
| **API** | Graceful HTTP **Shutdown** on signal; **`/health/live`** and **`/health/ready`**; trigger may succeed before work reaches the queue; async enqueue not joined on shutdown; horizontal replicas are not session-coherent | Wire Kube/Podman probes to health URLs; client idempotency; run the **reconciler** (it retries handoff for runs queued in the DB but not in the queue); alert on reconciler health or persistent backlog |
| **Worker** | **SIGINT/SIGTERM:** stop dequeue, **drain** current job for DB finalize; **SIGKILL:** no drain | Same; optional future: remote cancel and bounded drain timeouts |
| **Cron** | No sharding or leader election; duplicate processes risk double-firing | **Scale:** partition schedules across shards. **HA:** one firing path per schedule (leader election or external trigger → API) |
| **Auth** | None on the HTTP API | TLS and authentication at the edge or in the application |
