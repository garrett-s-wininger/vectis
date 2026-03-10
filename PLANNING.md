# Vectis - Modern Build System Planning Document

## 1. Project Overview

**Vectis** is a self-hosted, modern build system inspired by Jenkins, designed for large-scale CI/CD workloads. It provides a component-based architecture with well-defined boundaries, enabling independent replacement, upgrade, and reimplementation of each component.

**Design Goals:**

- Language-agnostic component boundaries (gRPC + JSON REST)
- Scale to hundreds of builds/day initially; validated scale targets added as benchmarks are performed
- Easy local development + production-grade deployment
- Database agnostic (SQLite dev, PostgreSQL prod)
- No required external dependencies beyond storage
- Vectis tests Vectis (dogfooding)

**Naming Conventions:**

- Pipeline file: `.vectis.yml`
- Binary: `vectis`
- Docker images: `vectis/`*
- Config paths: `/etc/vectis/`, `/var/lib/vectis/`
- Environment variable prefix: `VECTIS_*`
- gRPC package: `vectis.v1`

---

## 2. High-Level Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                         External Clients                        │
│   (Git hooks, CI scripts, other services)                       │
└─────────────────────┬───────────────────────────────────────────┘
                      │
          ┌───────────┴───────────┐
          ▼                       ▼
┌──────────────────┐    ┌─────────────────────────────────────────┐
│    TRIGGERS      │    │               FRONTEND                  │
│  - Webhook       │    │   - Web UI (SPA - React/Vue)            │
│  - Cron          │    │   - Serves static assets                │
│  - Polling       │    └─────────────┬───────────────────────────┘
│  - Manual        │                  │
│  (standalone)    │                  ▼ HTTP (REST)
└────────┬─────────┘    ┌─────────────────────────────────────────┐
         │              │          API SERVER (Port 8080)         │
         │              │   - Public REST API (authenticated)     │
         │              │   - Frontend proxy                      │
         │              └─────────────┬───────────────────────────┘
         │                            │
         │ gRPC                       │ gRPC
         ▼                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                     QUEUE (Port 8081)                           │
│   - Job scheduling & prioritization                             │
│   - Internal gRPC (Triggers, API Server, Workers: 8081)         │
│   - Append-only log for crash recovery (Phase 2+)               │
│   - Capability-based job filtering                              │
└─────────────────────────────────────────────────────────────────┘
                      │
                      │ gRPC
         ┌────────────┼────────────┐
         ▼            ▼            ▼
┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│   WORKER     │ │ LOG SERVICE  │ │  HEARTBEAT   │
│ (Port 8086)  │ │ (Port 8084)  │ │   SERVICE    │
│   jobs       │ │ - gRPC input │ │ (Port 8085)  │
│ - Workspace  │ │ - WebSocket  │ │ - Orphan     │
│ - VCS        │ │   output     │ │   detection  │
└──────────────┘ └──────────────┘ └──────────────┘

PROTOCOL SUMMARY:
- External/Frontend → API Server: HTTP REST only (Port 8080)
- API Server → Queue: gRPC (Port 8081)
- Triggers → Queue: gRPC only (Port 8081)
- Workers → Queue: gRPC (Port 8081) for job fetch/claim/status
- API Server → Worker: gRPC (Port 8086) for cancellation
- API Server → Heartbeat Service: gRPC (Port 8085) for worker address lookup (Phase 2+)
- Workers → Log Service: gRPC (Port 8084)
- Workers → Heartbeat Service: gRPC (Port 8085) (Phase 2+)
- Log Service → Frontend: WebSocket (direct connection)
```

**Worker Parallelism:** Each worker process handles exactly one job at a time. For parallel execution on a single machine, run multiple worker processes.

**Worker Queue Subscription:** Workers are configured to subscribe to specific queue(s) (e.g., `critical`, `default`, `batch`). A worker only pulls jobs from its subscribed queues.

**Component Responsibilities:**

- **Queue**: Job scheduling, persistence (Phase 2+), state transitions, capability-based job filtering
- **API Server**: Public REST API, authentication, proxies requests to Queue, cancellation via direct gRPC to worker
- **Heartbeat Service** (Phase 2+): Tracks worker heartbeats, detects orphaned jobs, tracks job-to-worker mapping for cancellation
- **Log Service**: Receives logs from workers via gRPC, serves WebSocket connections to frontend, persists to storage backend
- **Workers**: Execute jobs, fetch secrets, send notifications, report status to database, handle retries internally

**Trigger Service** (single binary, handles all trigger types):

- Webhook endpoint: receives webhooks, maps to project via secret
- Cron scheduler: executes scheduled builds
- Polling: checks VCS for changes at intervals
- All trigger types submit jobs to Queue via gRPC

**Core**: Queue/Database is the core. Workers and Triggers couple with it directly.

---

## Testing Strategy

### Philosophy: Vectis Tests Vectis

Vectis uses itself for CI/CD. Development workflow:

- Local: `make test` runs all tests locally
- Pre-commit: `make test-quick` runs fast unit tests
- PR review: Vectis instance runs full test suite on branch
- Release: Vectis builds and tests release artifacts

### Test Levels


| Level       | Scope                  | Runtime   | Purpose                                 |
| ----------- | ---------------------- | --------- | --------------------------------------- |
| Unit        | Individual packages    | <30s      | Fast feedback during development        |
| Integration | Component interactions | 2-5 min   | Validate API contracts, DB interactions |
| E2E         | Full build pipelines   | 10-30 min | Validate real-world scenarios           |
| Self-Test   | Vectis building Vectis | Full CI   | Production validation                   |


### Local Development Testing

```bash
# Quick feedback loop
make test-unit        # Run unit tests only
make test-integration # Docker Compose with test DB
make test-e2e        # Full pipeline tests

# Run specific component tests
make test-queue
make test-worker
```

### Test Infrastructure

- **Test doubles:** In-memory Queue/DB implementations for fast unit tests
- **Docker Compose:** Integration tests with real PostgreSQL
- **Golden files:** Expected log outputs, API responses in `testdata/`

---

## 3. Component Boundaries (APIs)

### 3.1 API Server → Queue API


| Method                              | Direction              | Purpose                                                |
| ----------------------------------- | ---------------------- | ------------------------------------------------------ |
| `POST /api/v1/jobs`                 | API → Queue            | Submit new build job (from project or inline)          |
| `POST /api/v1/jobs/inline`          | API → Queue            | Submit ad-hoc job (inline pipeline, no project needed) |
| `GET /api/v1/jobs/:id`              | API → Queue            | Get job status                                         |
| `GET /api/v1/jobs`                  | API → Queue            | List/query jobs (paginated, cursor-based)              |
| `POST /api/v1/jobs/:id/cancel`      | API → Worker           | Cancel job directly (bypasses Queue)                   |
| `GET /api/v1/projects`              | API → Queue            | List projects                                          |
| `POST /api/v1/projects`             | API → Queue            | Create project                                         |
| `GET /api/v1/projects/:id`          | API → Queue            | Get project details                                    |
| `PUT /api/v1/projects/:id`          | API → Queue            | Update project                                         |
| `DELETE /api/v1/projects/:id`       | API → Queue            | Delete project                                         |
| `GET /api/v1/jobs/:id/artifacts`    | API → Queue            | List job artifacts                                     |
| `GET /api/v1/artifacts/:id`         | API → Storage          | Download artifact (proxied by API Server)              |
| `POST /api/v1/artifacts/:id/upload` | API → Storage          | Upload artifact (chunked, resumable)                   |
| `GET /ws/jobs/:id/logs`             | Frontend ← Log Service | Real-time log stream (WebSocket)                       |


**Queue Persistence (Phase 2+):**

- Append-only log written to local disk (not DB)
- On restart: replay log to reconstruct in-memory state
- Log rotation: configurable retention (e.g., last N events or 24h)
- Database stores full job history; queue log is for crash recovery only
- Jobs in `pending` or `running` states at shutdown are replayed as `pending`

```toml
# Queue persistence config (Phase 2+)
[queue]
    persistence_wal_enabled = true
    persistence_snapshot_interval = 30s
    persistence_wal_max_size = "1GB"  # Force snapshot if WAL exceeds this
    max_pending_jobs = 100000  # Reject new jobs if queue exceeds this
```

### 3.2 Queue ↔ Worker API (gRPC)

Workers authenticate via static token in gRPC metadata. No registration required - workers simply present their token when fetching/claiming jobs.

```proto
// Worker → Queue (Port 8081)
service WorkerQueue {
  // Long poll for available jobs matching worker capabilities
  rpc FetchJob(FetchJobRequest) returns (Job);

  // Atomically claim a specific job
  rpc ClaimJob(ClaimRequest) returns (ClaimResponse);

  // Report job status changes (started, step completed, finished, failed)
  rpc ReportJobStatus(JobStatusReport) returns (Empty);
}

// API Server → Worker (Port 8086, for cancellation)
service WorkerControl {
  rpc CancelJob(CancelJob) returns (Empty);
}

// Worker declares capabilities and subscribed queues in FetchJobRequest
message FetchJobRequest {
  string worker_name = 1;           // For logging/debugging
  string worker_id = 2;             // Self-assigned UUID
  repeated string capabilities = 3; // For server-side job filtering
  repeated string queues = 4;       // Queues this worker subscribes to (e.g., ["critical", "default"])
}

message ClaimRequest {
  string worker_id = 1;             // Self-assigned UUID
  string worker_name = 2;
  repeated string capabilities = 3;
  string job_id = 4;
}

message ControlMessage {
  oneof message {
    CancelJob cancel_job = 1;
    ConfigUpdate config_update = 2;
  }
}

message CancelJob {
  string job_id = 1;
  string reason = 2;
}

// Internal API (Port 8081) - for Triggers and API Server
service QueueControl {
  rpc EnqueueJob(Job) returns (JobAck);
  rpc GetJobStatus(JobId) returns (JobStatus);
  rpc CancelJob(CancelJobRequest) returns (JobAck);
}

message CancelJobRequest {
  string job_id = 1;
  string requested_by = 2;  // User ID or system
}

// Worker → Heartbeat Service (Port 8085) - Phase 2+
service Heartbeat {
  rpc SendHeartbeat(HeartbeatRequest) returns (Empty);

  // API Server queries for worker address to cancel job
  rpc GetWorkerForJob(JobId) returns (WorkerAddress);
}

message HeartbeatRequest {
  string job_id = 1;
  string worker_id = 2;
  string worker_addr = 3;  // e.g., "worker-host:8086"
}

message JobId {
  string id = 1;
}

message WorkerAddress {
  string addr = 1;  // e.g., "worker-host:8086"
}

// Worker → Log Service (Port 8084)
service LogStreaming {
  rpc StreamLogs(stream LogChunk) returns (Empty);
}

message LogChunk {
  string job_id = 1;
  bytes data = 2;
  int64 sequence = 3;
  string stream = 4;  // "stdout" or "stderr"
}
```

**Token passed via gRPC metadata:** `authorization: Bearer <token>`

**Token authentication:** Single token type with role claims. Tokens include permissions (worker, trigger, user) as claims. This simplifies token management - one token type, different roles.

**Token storage:**

- **Production**: External secret manager (Vault, Knox, etc.)
- **Development**: Local encrypted file (AES-256-GCM, key via `VECTIS_TOKEN_KEY` env var)

```toml
[secrets]
    backend = "vault"  # or "filesystem" for dev

# Development (filesystem backend with encryption)
[secrets]
    backend = "filesystem"
    encryption_key = "${VECTIS_TOKEN_KEY}"  # 32-byte key for AES-256-GCM
```

**Worker ID:** Self-assigned UUID. No validation in Phase 1 (security enforced via token). Consider JWT-based authentication in Phase 2 for more robust worker identity binding.

**Queue Subscription:** Workers are configured with which queue(s) to subscribe to. A worker only receives jobs from its subscribed queues.

**Capability Filtering:** Queue filters jobs server-side based on worker capabilities declared in `FetchJobRequest`.

### 3.3 Worker Execution Model (Pull-based)

Workers pull work at their own pace - this keeps Queue isolated and focused on queuing:

**Worker → Queue:**

```
Worker calls: FetchJob() (long poll)
Queue filters jobs by worker's declared capabilities and subscribed queues
Queue responds with: pending job matching capabilities (or timeout)

Worker calls: ClaimJob(jobId)
Queue marks job as running, returns full job details

Worker executes job, periodically calls: ReportJobStatus()

**Crash handling:**
- Before reporting final status, worker buffers result to local disk
- After successful report to Queue, local buffer cleared
- If worker crashes: buffer persists, next worker startup or cleanup detects and handles
- Orphan detection triggers alert for admin review

**Benefits of pull model:**
- Queue doesn't track workers - simpler, no worker state in Queue
- Natural backpressure - workers only pull when ready
- Workers can have different capacities (fast/slow workers)
- Workers can implement their own scheduling (priorities, affinity)
- Queue failure doesn't affect running workers
- Workers can come and go without Queue knowing

**Worker Configuration:**
```toml
[worker]
    # Which queues this worker subscribes to (in priority order)
    subscribed_queues = ["critical", "default"]

    # Worker capabilities (auto-detected or manual)
    capabilities = ["docker", "golang", "nodejs"]

    # Or manual override of auto-detection
    auto_detect_capabilities = true
```

**Worker responsibilities:**

- Long polling for jobs from subscribed queues
- Claiming jobs atomically (prevents duplicate execution)
- Writing job status directly to database (Phase 1+)
- Streaming logs to Log Service via gRPC
- Sending heartbeats to Heartbeat Service (Phase 2+)
- Fetching secrets from secret manager at runtime
- Fetching VCS credentials from secrets for checkout
- Fetching `.vectis.yml` from repository during checkout
- Validating pipeline after checkout, before executing steps
- Handling auto-retry internally (re-executes job up to configured limit)
- Sending notifications on job completion/failure with exponential backoff retry
- Uploading artifacts directly to storage backend
- Managing workspace (VCS checkout, cleanup)

**Job Execution Flow:**

1. Worker fetches job from Queue via `FetchJob()` (filtered by capabilities)
2. Worker claims job via `ClaimJob()`
3. Worker fetches VCS credentials from secret manager
4. Worker checks out repository (including `.vectis.yml`)
5. Worker checks `vectis/overrides` branch for override (same credentials)
  - If branch does not exist: no override applied (silent ignore)
  - If override file does not exist at same path: no override applied
6. Worker parses and validates `.vectis.yml` - fails fast if invalid
7. Worker executes steps sequentially
8. Worker reports status after each step via `ReportJobStatus()`
9. On failure: Worker checks auto_retry_count, re-executes if remaining retries
10. On final failure: Worker marks job failed, sends notifications
11. On success: Worker collects artifacts, sends notifications
12. Worker cleans up workspace

**Heartbeat & Orphan Detection (Phase 2+):**

- Worker sends heartbeat to Heartbeat Service every 30 seconds while job is running
- Heartbeat Service monitors heartbeats with its own database connection
- If heartbeat missed for configurable grace period (default: 2 min), Heartbeat Service:
  - Writes `orphaned` status directly to job in database
  - Sends notification to configured admins/channels
- When job is orphaned:
- Job stays in `orphaned` state until admin intervention
- Admin must manually: cancel job, re-queue to different worker, or wait for worker recovery
- Orphan detection prevents jobs from hanging indefinitely due to worker failure
- **No automatic recovery** - manual admin intervention required for orphaned jobs

**Phase 2:** Background job periodically scans for stale jobs (older than configured timeout) and marks them orphaned. Only checks workers that have recently heartbeated.

**Multi-Site Heartbeat:** Each site has its own Heartbeat Service. Workers heartbeat to their local site's Heartbeat Service.

**Graceful Shutdown (Phase 2+):**

- Worker receives SIGTERM:
  1. Stop accepting new jobs
  2. Wait for running job to complete (configurable timeout, default: 5 min)
  3. If timeout reached: job stays in `running` state until worker recovers
  4. Shutdown worker
- Queue shutdown (Phase 1):
  1. Stop accepting new jobs
  2. Wait for current claim cycle to complete
  3. Shutdown (in-memory state lost)
- Queue shutdown (Phase 2+):
  1. Stop accepting new jobs
  2. Wait for current claim cycle to complete
  3. Persist queue state to append-only log
  4. Shutdown

**Cancellation Flow (Phase 1 - best effort):**

- User initiates cancel via API Server (authenticated)
- API Server sends `CancelJob` directly to Worker via gRPC
- Worker receives cancel message
- Worker interrupts job immediately (SIGTERM, then SIGKILL)
- Worker reports cancelled status to database
- **Note:** If worker is unreachable (network partition), cancel may fail. Phase 2 adds fallback to polling database flag.

```toml
[worker]
    shutdown_timeout = 5m
    # heartbeat_interval and orphan_grace_period (Phase 2+)
    heartbeat_interval = 30s
    orphan_grace_period = 2m
```

### 3.4 Multi-Queue Architecture

**Queue Groups (for scaling high-priority workloads):**

- Multiple independent queues (e.g., `default`, `critical`, `batch`)
- Each queue has a **drain priority** (order in which queues are checked by default)
- **Weighted drain**: After N jobs from high-priority, force-check lower queues
- Enables horizontal scaling per queue tier

**Worker Queue Subscription:**
Workers subscribe to queues to pull jobs from. By default, workers subscribe to all queues (auto-discovered). Administrators can explicitly declare which queues a worker should subscribe to.

```toml
[worker]
    # Default: auto-discover all queues from Queue service
    # subscribed_queues = []  # empty = all queues

    # Explicit subscription (in priority order)
    subscribed_queues = ["critical", "default"]
```

**Queue Selection Logic:**

```
Worker with subscribed_queues = ["critical", "default"]:
1. Check critical queue first → if jobs available, claim
2. Check default queue → if jobs available, claim
3. Wait (long poll timeout)

Worker with subscribed_queues = ["batch"]:
1. Check batch queue only → if jobs available, claim
2. Wait (long poll timeout)
```

**Example configuration:**

```toml
[queues]
  [queues.critical]
    drain_order = 1
  [queues.default]
    drain_order = 2
  [queues.batch]
    drain_order = 3

[drain_weight]
    critical = 5  # Hint: after 5 from critical, suggest checking default
```

**Sensible defaults:**

- If no queues configured: single "default" queue auto-created
- Queues auto-created on first reference (submit job to "priority" queue → auto-created)

### 3.5 Filtering & Priority

**Filtering (capability-based, queue-side):**

- Jobs declare required capabilities (e.g., `docker`, `gpu`, `ubuntu-22.04`)
- Workers declare available capabilities in `FetchJobRequest`
- **Queue filters jobs server-side** based on worker capabilities
- Worker only receives jobs it can execute
- Jobs without capability requirements match any worker

```proto
message FetchJobRequest {
  string worker_name = 1;
  string worker_id = 2;
  repeated string capabilities = 3;  // Queue filters jobs matching these
  repeated string queues = 4;        // Worker's subscribed queues
}
```

**Priority within queue:**

- Default: Fair queuing (prevent scheduling stalls)
- Optional `priority` flag on job: If set, drains before non-priority jobs
- Within priority level: FIFO or fair by project/age

**Capability Discovery:**

- Workers can auto-detect installed tools at startup
- Probes for: docker, go, node, python, etc.
- Or manual configuration

```toml
[worker]
    auto_detect_capabilities = true

# Or manual override
# [worker.capabilities]
#   - docker
#   - golang
#   - nodejs
```

### 3.6 Trigger → Queue API

Triggers are standalone components that submit jobs to Queue via gRPC only:


| Method                | Direction       | Purpose                     |
| --------------------- | --------------- | --------------------------- |
| `EnqueueJob(Job)`     | Trigger → Queue | Submit new build job (gRPC) |
| `GetJobStatus(JobId)` | Trigger → Queue | Check job status            |


Triggers authenticate using tokens with trigger role claim.

**Webhook→Project Mapping:**

- Trigger service maps incoming webhooks to projects using secret-based mapping
- Webhook secrets are unique per project
- When webhook received, trigger looks up project by matching the webhook secret
- This allows the same webhook endpoint to handle multiple projects

```toml
# Project configuration (in central DB)
[projects.my-project]
    webhook_secret_ref = "my-project-webhook-secret"  # Reference to secret

# In secret manager
[secrets.my-project-webhook-secret]
    value = "whsec_xxxxx"  # Unique secret for this project
```

Triggers receive job definitions via:

- Pipeline configuration in `.vectis.yml` (stored in repository, fetched on demand)
- API call from Frontend (manual trigger)

The trigger service reads pipeline configs from the repository to discover which triggers are defined for each project.

### 3.7 Trigger Service Types

The unified trigger service handles multiple trigger types:


| Trigger Type | Input Source          | Description                                                      |
| ------------ | --------------------- | ---------------------------------------------------------------- |
| webhook      | Custom payload parser | Receives webhooks, parses payload, maps to project/job (Phase 1) |
| cron         | Pipeline config       | Cron expressions for scheduled builds                            |
| polling      | Pipeline config       | Checks VCS for changes at intervals                              |
| manual       | Frontend API          | User-initiated builds via UI/API                                 |


**Webhook parsing (Phase 1):**

- Custom parsing for each VCS (GitHub, GitLab, Bitbucket, etc.)
- Normalize payload to common format (commit SHA, branch, repo URL)
- Add VCS integrations later as needed

### 3.8 Pipeline Configuration (Pipeline-as-Code)

Job definitions are stored in the repository alongside the code being built, enabling pipeline-as-code with hotfix override support.

#### Pipeline Definition Sources

Pipeline definitions can come from multiple sources:


| Source                 | Description                                                   |
| ---------------------- | ------------------------------------------------------------- |
| In-repo, root          | `.vectis.yml` at repo root (default, low friction)            |
| In-repo, custom path   | `.vectis/services/foo.yml` (for monorepos)                    |
| Separate pipeline repo | /Pipeline in a dedicated repo                                 |
| Inline (no repo)       | Pipeline defined in project settings (for manual/ad-hoc jobs) |


```toml
[projects.my-project]
pipeline_repo = null  # null = use build's repo
pipeline_path = ".vectis.yml"  # or custom path like ".vectis/services/api.yml"

# For jobs without a repository:
[projects.ad-hoc-project]
# No repo_url - enables inline pipeline mode
inline_pipeline = """
name: "Ad-hoc Job"
steps:
  - name: Run script
    type: shell
    command: echo "hello"
"""
```

**Pipeline resolution order:**

1. If project has `inline_pipeline` defined → use that (repo-less jobs)
2. If project has `pipeline_repo` → fetch from separate repo
3. Otherwise → fetch from build's repo (`.vectis.yml`)

#### Override Mechanism

Overrides allow hotfixing job configurations without modifying the source repository. Overrides are stored in a separate branch:

- **Override branch**: `vectis/overrides` (configurable)
- **Colocated**: Override file at same path in override branch
- **Tied to commit**: Override applies to a specific commit SHA
- **Validation**: Validated before merge (pre-receive hook or CI)

```
# Pipeline at: .vectis.yml (commit abc123)
# Override at: vectis/overrides/.vectis.yml (commit override-xyz)

# If job triggers for commit abc123, override is applied
# New commits don't inherit override until re-applied
```

#### Pipeline Schema

```yaml
# .vectis.yml
name: "Build Service"
description: "Builds the main service"

# Trigger configuration (defined in pipeline, not external config)
on:
  webhook:
    token: "my-secret-token"  # or reference secret
  poll:
    interval: 60s
    branch: main
  cron:
    schedule: "0 2 * *"
    branch: main

# Which queue to use
queue: default

# Required worker capabilities
requires_capabilities:
  - docker

# Resource requests (for scheduling - matching worker capabilities)
# Limits are enforced via cgroups (Phase 2+)
resources:
  request:
    cpu: 2        # number of cores (for scheduling)
    memory: 4GB   # RAM allocation (for scheduling)
  limit:
    cpu: 2        # enforced via cgroups (Phase 2+)
    memory: 4GB   # enforced via cgroups (Phase 2+)
  disk: 10GB    # workspace disk (scheduling hint only)

# Job timeout (default: 30m, 0 = no timeout)
timeout: 30m

# Step timeout (default: 10m)
# When step exceeds timeout:
# 1. SIGTERM - graceful shutdown request
# 2. Wait (grace_period, default: 30s)
# 3. SIGTERM again
# 4. Wait (grace_period, default: 30s)
# 5. SIGKILL - force kill
step_timeout: 10m
step_timeout_grace_period: 30s

# Step output limit (default: 1GB per step, admin-configurable)
# If exceeded: step fails with output limit exceeded error
step_output_limit: "1GB"

# Environment variables available to all steps
env:
  GO_VERSION: "1.21"

# Concurrency control (replaces project-level max_concurrent_jobs)
concurrency:
  mode: single  # single | concurrent | queue
  # For mode: queue, optionally:
  max_queue: 5

# Pipeline validation
validation:
  strict: true  # fail if schema invalid
  schema_version: "1.0"

# Notifications (sent by worker on completion/failure)
notifications:
  on_success:
    - type: webhook
      url: https://hooks.example.com/success
  on_failure:
    - type: webhook
      url: https://hooks.example.com/failure
  on_complete: []

# Notification retry (exponential backoff)
notification_retry:
  max_attempts: 3      # Retry up to 3 times
  initial_delay: 1s    # Start with 1 second delay
  max_delay: 30s       # Cap at 30 seconds
  multiplier: 2        # Double delay each retry

# Notification queue (post-action, async)
# Notifications are sent after job completes (non-blocking)
# Prevents job from failing if notification backend is down
notification_queue:
  max_size = 10000  # Max pending notifications before dropping
  worker_concurrency = 5  # Parallel notification sends

# Job steps (executed serially in order)
steps:
  - name: Checkout
    type: checkout

  - name: Install deps
    type: shell
    command: go mod download
    cache_on: [go.sum]
    timeout: 5m  # step-level timeout (default: 10m)
    resources:  # step-level overrides
      cpu: 1
      memory: 2GB

  - name: Build
    type: shell
    command: go build -o bin/service ./cmd/service

  - name: Test
    type: shell
    command: go test ./...
    continue_on_failure: false

# Artifact collection
artifacts:
  paths:
    - bin/service
  retention_days: 30  # artifact retention policy

# Pre/Post hooks (Phase 2+)
# hooks:
#   before_job:
#     - name: Setup
#       command: ./setup.sh
#   after_job:
#     - name: Cleanup
#       command: ./cleanup.sh
```

**Concurrency modes:**


| Mode         | Behavior (if trigger fires while running) |
| ------------ | ----------------------------------------- |
| `single`     | Skip this trigger (default)               |
| `concurrent` | Start new run in parallel                 |
| `queue`      | Add to queue, run after current completes |


**Note:** Pipeline-level concurrency control replaces the deprecated project-level `max_concurrent_jobs` setting.

#### Config-as-Code Enforcement

Projects can be configured to require pipeline-as-code, preventing manual overrides:

```toml
[projects.my-project]
enforce_pipeline_as_code = true  # false allows manual overrides via UI
```

---

## Error Handling

### Error Categories


| Category       | Examples                     | Strategy                     |
| -------------- | ---------------------------- | ---------------------------- |
| Transient      | Network timeout, rate limit  | Retry with backoff           |
| Permanent      | Syntax error, test failure   | Fail immediately             |
| Infrastructure | Worker crash, disk full      | Orphan detection, reschedule |
| User           | Invalid config, auth failure | Fail fast with clear message |


### Job Failure Modes

**Step failure (worker-driven):**

- Worker detects step failure (non-zero exit code)
- Worker reports step status to Queue via `ReportJobStatus()`
- Worker decides to stop job and mark as failed (unless `continue_on_failure: true`)
- `always` steps run regardless of previous step status
- Failed step logs preserved for debugging

**Job-level failure:**

- Worker terminates running steps (SIGTERM, then SIGKILL after grace period)
- Worker preserves workspace for investigation period (configurable)
- Worker marks job `failed`, sends notifications
- Worker reports final status to Queue

**Partial completion:**

- Steps before failure: outputs/artifacts kept
- Steps after failure: not executed
- Resume not supported (re-run from beginning)

### Network Partition Handling

**Worker → Queue partition:**

- Worker continues running current job
- Job status updates buffered locally, retried periodically until Queue reconnects
- Logs buffered locally (disk), retried to push to Log Service
- In Phase 1: no orphan detection; job stays in "running" until manual intervention or Phase 2+ background job
- When worker reconnects: flush buffered status updates and logs

**Queue → Database partition:**

- Queue buffers operations in memory (limited size)
- If buffer full: reject new jobs, return unavailable status
- In-flight job status updates: buffered in Queue memory, retried periodically until DB reconnects
- Jobs stay in current state (e.g., `running`) until DB recovers and updates are persisted

**Service Availability:**

- **Log Service down**: Worker continues job execution, buffers logs to local disk, flushes when service recovers
- **Phase 1**: No heartbeat service; worker continues execution normally
- **Phase 2+**: Heartbeat Service tracks worker health; on partition, background job marks stale jobs orphaned

### Error Response Format

**REST errors:**

```json
{
  "error": {
    "code": "JOB_STEP_FAILED",
    "message": "Step 'test' exited with code 1",
    "details": {
      "step": "test",
      "exit_code": 1,
      "retryable": false,
      "log_url": "/api/v1/jobs/123/logs"
    }
  }
}
```

---

## 4. Data Model

### Core Entities

**JobStatus Enum:**

- `pending` - Job queued, waiting for worker
- `running` - Job claimed by worker, executing
- `succeeded` - Job completed successfully
- `failed` - Job failed (step failure, timeout, etc.)
- `cancelled` - Job cancelled by user
- `orphaned` - Worker lost, heartbeat missed
- `timeout` - Job exceeded time limit

**Project**

- `id`, `name`, `description`, `created_at`, `updated_at`
- VCS configuration (repo URL, branch, credentials ref) - optional for repo-less jobs
- `inline_pipeline` (YAML blob) - pipeline definition for jobs without repo
- `log_retention_days`, `max_log_size_mb`
- `auto_retry_count`

**Queue**

- `id`, `name`, `drain_order`, `description`

**Job**

- `id`, `project_id`, `queue_id`, `status` (JobStatus enum), `priority` (boolean)
- `trigger_type`, `commit_sha`, `branch`, `required_capabilities`
- `worker_id` (which worker is running this job - informational only)
- `started_at`, `finished_at`, `created_at`
- `pipeline_source`: "repo" or "inline"

**Step**

- `id`, `job_id`, `order`, `name`, `type` (shell, script, artifact)
- `config` (JSON - script content, image, etc.)

**StepResult**

- `id`, `step_id`, `job_id`
- `status` (pending, running, succeeded, failed, skipped)
- `exit_code` (int, nullable)
- `started_at`, `finished_at`
- `output_summary` (text, truncated output)
- `duration_ms`

**Artifact**

- `id`, `job_id`, `step_id` (nullable)
- `filename`, `storage_path`, `storage_backend` (s3, filesystem)
- `size_bytes`, `checksum_sha256`
- `created_at`, `expires_at`

**User**

- `id`, `username`, `email`, `password_hash` (if built-in auth)
- `created_at`, `updated_at`, `last_login_at`
- `roles` (JSON array of role names)

**Token**

- `id`, `user_id` (nullable for service tokens)
- `token_hash`, `name`, `roles` (JSON array of role names: worker, trigger, user, admin)
- `created_at`, `expires_at`, `revoked_at`

**Secret**

- `id`, `project_id`, `key`, `value_encrypted`
- `created_at`, `updated_at`

**Notification**

- `id`, `job_id`, `type` (webhook, email, slack)
- `config` (JSON - url, recipients, etc.)
- `sent_at`, `status` (pending, sent, failed)

**AuditLog**

- `id`, `timestamp`, `actor_type` (user, worker, system)
- `actor_id`, `action` (cancel_job, modify_project, login, etc.)
- `target_type`, `target_id`, `metadata` (JSON)
- `ip_address`, `user_agent`

**Actions tracked:**

- Job: submit, cancel, retry, modify
- Project: create, update, delete
- User: login, logout, permission_change
- Override: create, update, delete
- Token: create, revoke

**Retention:** Configurable, default 1 year (separate from job logs)

### Database Abstraction

- Use an ORM/query builder (GORM for Go)
- Support SQLite for development
- Support PostgreSQL for production
- Interfaces for repository pattern to allow swapping backends

### Database Migration Strategy

- Use **golang-migrate** (migrations as SQL files)
- Migrations stored in `migrations/` directory
- Both up and down migrations required for every change
- Embedded in binary via Go's `embed` for single-binary deployment

```bash
# Manual migration execution (production)
./vectis migrate up        # Run pending migrations
./vectis migrate down 1    # Rollback last migration
./vectis migrate version   # Check current version
```

**Migration naming:**

```
migrations/
  001_create_projects_table.up.sql
  001_create_projects_table.down.sql
  002_add_job_timeouts.up.sql
  002_add_job_timeouts.down.sql
```

**Safe vs Unsafe Changes:**

- Safe: Adding nullable columns, adding tables, adding indexes
- Unsafe: Dropping columns, renaming, changing types (require coordination)

**Phase 1:** Manual migrations, can tear down and recreate
**Production:** Migrations required before upgrade

---

## 5. Storage Architecture

### 5.1 Logs

**Real-time streaming**: Workers stream logs to Log Service via gRPC → Log Service persists to backend → Log Service multiplexes to frontend clients via WebSocket

**Log Service Endpoints:**

- gRPC `StreamLogs` (Port 8084) - Workers stream logs
- WebSocket `/ws/jobs/:id/logs` (Port 8084) - Frontend connects for real-time viewing

**Pluggable backends**:

- **Filesystem** (dev) - Simple, local storage
- **S3-compatible** (prod) - Standard, scalable
- **Loki** (via Alloy OTEL) - Unified with metrics/traces

**Log service sends to Alloy** (OTEL protocol). Production config swaps to Loki backend.

**Format**: Plain text (shell output cannot be normalized)

**Retention**: Per-project configurable (age-based only)

```toml
[projects.my-project]
    log_retention_days = 30
```

### Log Streaming Reliability

**WebSocket behavior:**

- Client requests: `GET /ws/jobs/:id/logs` with `Authorization: Bearer <token>` header
- Server validates token on WebSocket upgrade handshake
- Server streams existing logs + real-time updates
- Heartbeat/ping every 30s to detect dead connections

**Reconnection:**

- Client can resume with `?offset=<bytes>` parameter
- Server maintains 5-minute buffer of recent logs (in-memory, not persisted)
- If offset falls outside the buffered range: return 409 Conflict, client fetches full log via REST
- Buffer is per-job; when job completes, buffer is retained for 5 minutes then discarded

**Ordering:**

- Worker assigns monotonic sequence numbers to log chunks
- Log Service reorders if necessary for display

**Log limits:**

```toml
[projects.defaults]
  max_log_size = "100MB"
  log_truncation_policy = "head"  # "head" keeps most recent, "tail" keeps oldest
```

**Truncation behavior:**

- `head`: When log exceeds max_size, discard oldest bytes first (keep recent)
- `tail`: When log exceeds max_size, discard newest bytes first (keep oldest - useful for debugging startup failures)

**Multiple viewers:**

- Log Service multicasts to all connected clients
- Max 10 concurrent viewers per job (configurable)

### 5.2 Artifacts

**Pluggable backends**:

- Filesystem (dev)
- S3-compatible (prod)

**Artifact API:**

- `GET /api/v1/artifacts/:id` - Download artifact (served by API Server, fetched from storage)
- `GET /api/v1/jobs/:id/artifacts` - List artifacts for a job
- `POST /api/v1/artifacts/:id/upload` - Upload artifact (chunked, resumable)
- Metadata tracked in `Artifact` entity (filename, size, checksum, storage_path, storage_backend)
- Workers move artifacts from workspace to storage backend (not copy), then report artifact metadata to Queue
- Retention policies per-project (configured in pipeline)
- Upload failures: exponential backoff retry (same as notifications: 3 attempts, 1s initial, 2x multiplier, 30s cap)

**Artifact upload:**

- Chunked upload for large files (1MB chunks)
- Resumable: track uploaded chunks, resume from last successful
- Concurrent chunk uploads: up to 4 parallel chunks
- Checksum validation after upload complete

### 5.3 Cache (Dependency Caching)

**Purpose:** Cache dependencies between jobs to speed up builds

**Pluggable backends**:

- Filesystem (local worker disk)
- S3-compatible (shared between workers)
- Redis (metadata/coordination)

**Configuration:**

```toml
[cache]
    backend = "s3"  # or "filesystem", "redis"
    bucket = "vectis-cache"
```

**Pipeline usage:**

```yaml
cache:
  backend: s3
  paths:
    - node_modules/
    - .cache/go/
  key: "{{.Project}}/{{checksum "package-lock.json"}}"
  compression: gzip
```

**Cache key format:**

- Template in Go `text/template` syntax
- Functions: `{{.Project}}`, `{{.Branch}}`, `{{.Commit}}`, `{{checksum "filename"}}`
- Result is hashed (SHA256) to produce the final cache key

### 5.4 Secrets

**Pluggable backends**:

- Built-in encrypted filesystem (dev/local)
- External (Vault, etc.) for production

**Workers fetch secrets at runtime** - when a worker needs a secret (VCS credentials, webhook tokens, etc.), it fetches directly from the configured secret manager. External secret managers (Vault, Knox, etc.) are designed for read-heavy workloads - rely on their built-in caching/HA.

```toml
[secrets]
    backend = "filesystem"  # or "vault"
```

### 5.5 System Logs

Same pluggable backends as job logs:

- stdout/stderr (containerized/SystemD)
- Filesystem
- OTEL

---

## 6. Workspace & VCS

### Workspace Management

- **Ephemeral per-job**: Each job gets fresh workspace
- **Opt-in reuse**: Configurable for large repos (cache subdirectories or use git alternates)
- **Shared filesystem**: Workers configured with shared location can resume from others
- **Resource model**: Pipeline defines `resources.request` (for scheduling) and `resources.limit` (for enforcement via cgroups in Phase 2+)
- **Phase 1**: No enforcement; workers run as separate process group (best-effort isolation)

```toml
[worker]
    workspace_root = "/var/lib/vectis/workspaces"
    reuse_workspace = false  # true enables caching
```

### VCS Abstraction

Worker-controlled, pluggable checkout:

- **Git plugin** (default)
- **Null/no-op plugin** (for no-repo jobs, local dev)

**VCS Credentials:** Workers fetch VCS credentials from the secret manager at checkout time. The project configuration contains a reference to the secret, which the worker resolves.

```toml
[worker.vcs]
    type = "git"  # or "null"
```

---

## 7. Job Recovery & Cancellation

### Job Recovery

Configurable per-project:

- **Default**: manual retry (safest)
- **Optional auto-retry**: configurable N times before requiring manual

```toml
[projects.my-project]
    auto_retry_count = 0  # 0 = manual only
```

### Job Cancellation

- User initiates cancel via API Server (authenticated)
- API Server queries Heartbeat Service for worker address handling the job
- API Server sends cancel directly to Worker via gRPC
- Worker receives cancel message and interrupts job (SIGTERM, then SIGKILL after grace period)
- Worker reports cancelled status to Queue via `ReportJobStatus()`
- Permissioned - only authorized users can cancel

---

## Resource Cleanup

### Workspace Lifecycle

**Creation:**

- Fresh workspace per job start
- Optional: opt-in workspace reuse for large repos

**Cleanup:**

- **Successful jobs**: immediate cleanup after artifact collection
- **Failed jobs**: preserved per retention policy, cleaned by background job
- **Cancelled jobs**: cleanup after grace period

**Cleanup mechanism:**

- Worker cleans up immediately after job completes (normal case)
- Background cron job handles stale workspace cleanup (orphan detection)
- Per-job folder structure: `/{project_id}/{job_id}/`

**Failed Workspace Retention:**

```toml
[worker]
  preserve_failed_workspaces = true
  failed_workspace_retention = "24h"  # default: keep last failed per job
  max_failed_workspace_size = "10GB"  # skip preservation if larger
```

### Artifact Retention

**Policy levels:**

1. Project-level (configured in project settings)
2. Global system default

**Enforcement:**

- Batched cleanup runs daily at low-traffic hour
- Or on-demand: `vectis admin cleanup-artifacts`

### Log Retention

```toml
[projects.defaults]
  log_retention_days = 30
```

### Orphaned Resource Detection

- Daily scan for stale containers, processes, temp files
- Configurable auto-cleanup or alert-only mode

---

## 8. Rate Limiting & Quotas

- **Per-project API rate limiting**: Configurable request limits
- **Per-token rate limiting**: Token bucket per token (stored in Redis)
- **Resource quotas**: Optional per-project, configured on individual workers

```toml
[rate_limit]
    backend = "redis"  # or "memory" (single-node dev)
    requests_per_minute = 1000
    burst = 50

[rate_limit.redis]
    addr = "redis:6379"
```

**Note:** Job concurrency is controlled at the pipeline level via the `concurrency` setting in `.vectis.yml`, not at the project level.

---

## 9. Metrics & Observability

### Unified Observability Stack

Vectis uses Grafana's open telemetry ecosystem for all observability:

- **Alloy** (OTEL collector): Receives metrics, traces, logs → forwards to storage
- **Grafana**: Visualization for metrics + traces + logs
- **Pluggable backends**: Configure for dev/prod (Loki/Tempo for prod, embedded for dev)

### Metrics

Every job tracked from trigger → queue → worker → completion:

- `trigger_received` → `job_enqueued` → `job_claimed` → `step_started` → `step_completed` → `job_finished`

**Key Metrics:**


| Metric               | Component    | Purpose                          |
| -------------------- | ------------ | -------------------------------- |
| Queue depth          | Queue        | Backlog detection                |
| Worker idle time     | Worker       | Capacity planning                |
| Job start latency    | Queue→Worker | Performance tracking             |
| Time to first step   | Worker       | Execution efficiency (like TTFI) |
| DB connection pool   | Queue        | Database health                  |
| Append-only log size | Queue        | Storage monitoring (Phase 2+)    |


### Log Service

**Real-time streaming**: Workers stream logs to Log Service via gRPC → Log Service persists to backend → Log Service multiplexes to frontend clients via WebSocket

**Pluggable backends** (swappable for dev/prod):

- **Filesystem** (dev) - Simple, local storage
- **S3-compatible** (prod) - Standard, scalable
- **Loki** (via Alloy OTEL) - Unified with metrics/traces

### Bundled Monitoring (Development)

```yaml
# docker-compose.yml (development)
services:
  alloy:
    image: grafana/alloy
    volumes:
      - ./alloy.config:/etc/alloy/config.alloy
    ports:
      - "12345:12345"  # OTEL ingest
  grafana:
    image: grafana/grafana
    volumes:
      - ./dashboards:/etc/grafana/provisioning/dashboards
    ports:
      - "3001:3000"
  # ... vectis services
```

All components send metrics/traces/logs to Alloy (OTEL protocol). Swap Alloy config for production backends (Prometheus, Tempo, Loki).

---

## 10. Service Discovery

### Cluster Catalog

All services (including Frontend) discover each other via a cluster catalog that tracks:

- Service name → address(es) mapping
- Health status
- Tags/labels (e.g., site, region)

Components register themselves on startup and periodically send heartbeats.

**Frontend discovery:**

- Frontend queries catalog for Log Service address
- Frontend connects directly to Log Service WebSocket for real-time logs

### Service Inventory (Bare-metal / VMs)

For bare-metal or VM deployments where services can't self-register:

- **Consul**: Native support for service registration and discovery
- **Static inventory file**: JSON/YAML file listing all services and addresses (fallback)
- **Watch mode**: Reload inventory on file change (no restart needed)

```toml
[inventory]
    backend = "consul"  # or "file"
    path = "/etc/vectis/inventory.yml"  # for file backend

[inventory.file]
    reload_on_change = true  # Auto-reload when file updates
```

**Note**: Kubernetes deployments use native K8s service discovery - no inventory needed.

```yaml
# inventory.yml example
services:
  queue:
    - addr: "queue-1:8081"
      site: "us-east-1"
  heartbeat:
    - addr: "heartbeat-1:8085"
      site: "us-east-1"
  workers:
    - addr: "worker-1:8086"
      capabilities: ["docker", "golang"]
      site: "us-east-1"
```

### Development

- Static config with environment variables (`QUEUE_ADDR`, etc.)
- Config file with queue list

### Kubernetes

- Use K8s Services for DNS-based discovery
- Workers discover Queue via service name
- Built-in load balancing and health checks

### Production (non-K8s)

- Service registry (Consul/etcd) for large scale
- Or DNS + health checks

### Health Checks

All services expose a single health endpoint:


| Service           | Endpoint  | Response                                                   |
| ----------------- | --------- | ---------------------------------------------------------- |
| API Server        | `/health` | `{"alive": true, "ready": true, "details": {...}}`         |
| Queue             | `/health` | `{"alive": true, "ready": true, "details": {...}}`         |
| Worker            | `/health` | `{"alive": true, "ready": true}` (always ready when alive) |
| Log Service       | `/health` | `{"alive": true, "ready": true, "details": {...}}`         |
| Heartbeat Service | `/health` | `{"alive": true, "ready": true, "details": {...}}`         |
| Trigger           | `/health` | `{"alive": true, "ready": true, "details": {...}}`         |


- **alive**: Process is running and not stuck
- **ready**: Service can serve traffic (DB connected, queue available, storage connected)
- **details**: Service-specific info (e.g., DB ping time, queue depth)

---

## 11. Security & Authentication

### 11.1 API Boundaries

Vectis separates public and internal APIs to enforce encapsulation:


| API Type     | Service    | Boundary          | Auth                          |
| ------------ | ---------- | ----------------- | ----------------------------- |
| Public API   | API Server | External clients  | Token-based (user or service) |
| Internal API | Queue      | Workers, Triggers | mTLS + Worker/Trigger token   |


**Public API** (via API Server):

- Job submission, status, logs
- Project management
- User authentication

**Internal API** (direct to Queue):

- Worker token validation (for workers)
- Trigger token validation (for triggers)
- Job fetching
- Job status updates

This separation ensures internal operations remain implementation details - admins have access via configuration, but external systems interact through controlled APIs.

### 11.2 User Authentication

**Production:**

- OIDC (OpenID Connect) - Users authenticate via external identity provider
- Frontend redirects to OIDC provider, receives token
- Token used for API requests

**Development:**

- Auth disabled: No authentication required (single-node dev)
- Built-in username/password: Local user accounts with hashed passwords

**Frontend Auth:**

- Bundled auth backend manages OIDC flow and session state
- Stores refresh tokens securely
- API Server validates tokens from frontend

**Initial Admin Setup:**

- **Config-managed**: Configure OIDC settings or DB connection for user/password auth - first user to authenticate becomes admin automatically
- **Non-config-managed**: Auth disabled by default, no admin required

### 11.2 RBAC Roles


| Role         | Permissions                                           |
| ------------ | ----------------------------------------------------- |
| **Viewer**   | Read-only access to jobs, logs, projects              |
| **Trigger**  | Submit jobs, view status                              |
| **Operator** | Cancel jobs, retry jobs, view all                     |
| **Admin**    | Modify project config, manage overrides, manage users |


Roles can be assigned per-project or globally.

### 11.3 Worker & Trigger Authentication

**Workers** authenticate using static tokens configured at deployment:

```toml
[worker]
token = "${VECTIS_WORKER_TOKEN}"  # Static token in config file
```

**Tokens** use a single token type with role claims (worker, trigger, user, admin):

```toml
[trigger]
token = "${VECTIS_TRIGGER_TOKEN}"  # Static token in config file
```

**Authentication model:**

- mTLS proves "I'm a legitimate host within the certificate domain"
- Token proves "I'm a legitimate worker/trigger"

This allows config-as-code deployment while maintaining security.

### 11.4 mTLS Support

For production deployments within a certificate domain:

- All services generate certificates signed by cluster CA
- Services verify peer certificates before accepting connections
- mTLS is complementary to token-based auth

```toml
[mtls]
enabled = true
ca_cert = "/etc/vectis/certs/ca.pem"
cert = "/etc/vectis/certs/worker.pem"
key = "/etc/vectis/certs/worker-key.pem"
```

### 11.5 API Rate Limiting

- Token bucket algorithm for rate limiting
- Per-user and per-project limits
- Configurable request limits

```toml
[api]
rate_limit_requests = 1000  # per minute
rate_limit_burst = 50
```

**Response headers:**

- `X-RateLimit-Limit`: Maximum requests per window
- `X-RateLimit-Remaining`: Requests remaining in window
- `X-RateLimit-Reset`: Unix timestamp when limit resets

### 11.6 API Versioning

**Version Strategy:**

- URL path versioning for REST: `/api/v1/jobs`, `/api/v2/jobs`
- gRPC uses package versioning: `vectis.v1.QueueService`
- Configuration: `version = "1.0"` field in config.toml

**Compatibility Guarantees:**

- Minor versions: backward compatible additions
- Major versions: may break compatibility
- Support N and N-1 major versions simultaneously

**Deprecation Policy:**

- Feature deprecated in vN
- Warning header returned: `Deprecation: true`
- Removed in vN+2
- Migration guide published with each major release

### 11.7 Webhook Security

**Phase 1:** Basic token validation
**Production:** Full security controls required

**HMAC signatures (GitHub/GitLab compatible):**

```yaml
on:
  webhook:
    secret: "${VECTIS_WEBHOOK_SECRET}"
    signature_header: "X-Hub-Signature-256"
```

**Simple token (generic webhooks):**

```yaml
on:
  webhook:
    token: "my-secret-token"
```

**Security controls (Production):**

- Reject payloads older than 5 minutes (replay protection, based on server time)
- Optional IP allowlist per project
- Rate limiting per webhook source
- Audit logging for all webhook events

---

## 12. Federation & Multi-Site Deployment

### 12.1 Architecture Overview

Vectis supports multi-site deployments where each site operates independently but shares centralized configuration. This avoids WAN complexity in orchestration while providing a single pane of glass for operators.

**Database Architecture:** Centralized database with read replicas per site for HA, or single centralized database for non-HA setups.

**Database replication:** Follow database vendor recommendations (PostgreSQL streaming replication, etc.). Vectis does not implement custom replication - relies on the database's native capabilities.

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

### 12.2 Site Model

Each deployment site consists of:

- **Queue service** - Local job queuing and scheduling
- **Workers** - Local execution capacity
- **Log service** - Local log storage (accessed via site-specific endpoint)
- **Heartbeat service** (Phase 2+) - Local heartbeat tracking and orphan detection
- **API Server** - Local API endpoint, healthcheck-enabled

Sites are independent for execution - no cross-site job routing in orchestration.

### 12.3 Centralized vs Local Data


| Data          | Storage                | Replication            |
| ------------- | ---------------------- | ---------------------- |
| **Projects**  | Central DB             | Read-replicas per site |
| **Secrets**   | External (Vault, etc.) | Shared access          |
| **Accounts**  | Central (Frontend)     | Central only           |
| **Pipelines** | Git repos              | Cloned per site        |
| **Jobs**      | Local per site         | None                   |
| **Logs**      | Local per site         | None                   |
| **Artifacts** | Local per site         | None                   |


### 12.4 Site Routing

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

### 12.5 Frontend Aggregation

The frontend provides a unified view across sites:

- **Single-site (default)**: No site configuration needed - frontend connects directly to local API Server
- **Multi-site**: Frontend auto-discovers sites from cluster configuration
- **Healthchecks**: Frontend probes each site's API Server periodically (multi-site only)
- **Cross-site logs**: Frontend connects to the specific site's log service where the job is running (multi-site only)
- **Status aggregation**: Jobs shown in context of their site (multi-site only)

```toml
# Single-site (default - no config needed)
# [frontend]
#   # No sites configured = single-site mode

# Multi-site (explicit configuration)
[frontend]
sites = ["us-east-1", "eu-west-1", "ap-south"]
```

### 12.6 Secrets Integration

Secrets are retrieved from an external secret manager (Vault, Knox, etc.) at job execution time. Since each site connects to the same secret manager, secrets are consistently available regardless of which site runs the job.

For development/testing, the built-in encrypted filesystem backend works for single-site or multi-site deployments.

### 12.7 Trigger Site Selection

Triggers determine site at job submission time:

1. Trigger fetches project configuration (from central DB)
2. Project config specifies `site` (e.g., "us-east-1") or "any"
3. If "any", use current site's API Server (the trigger's site)
4. Submit job to appropriate site's API Server

This keeps site selection simple and explicit - no complex routing logic.

---

## 13. Features by Priority

### Phase 1: Core Loop (POC)

- In-memory Queue service (no persistence)
- Worker long-polling for jobs
- Worker token authentication
- Basic job status (pending → running → succeeded/failed)
- Worker executes shell commands
- Worker writes job status directly to database
- Simple REST API for job submission/status
- PostgreSQL backend

### Phase 2: Persistence & Robustness

- Append-only log for queue crash recovery
- Basic cgroups resource limits (separate process group, memory limits)
- Job timeouts
- Graceful worker shutdown
- SQLite backend for development

### Phase 3: Triggers & Pipeline

- Trigger service (unified binary)
- Webhook endpoint
- Cron scheduler
- Pipeline-as-code (`.vectis.yml`)
- Manual trigger via API

---

## 14. Deployment

### Development

```bash
# All-in-one binary (includes all components)
./vectis run --dev

# Or separate components
./vectis run api --port 8080
./vectis run frontend --port 3000
./vectis run queue --port 8081
./vectis run worker
./vectis run trigger --port 8083  # unified: webhook, cron, polling
./vectis run log-service
./vectis run heartbeat-service
```

### Production

**Docker**

```yaml
services:
  vectis-api:
    image: vectis/api:latest
    ports: ["8080:8080"]
    environment:
      - DATABASE_URL=postgres://...
  vectis-frontend:
    image: vectis/frontend:latest
    ports: ["3000:3000"]
  vectis-queue:
    image: vectis/queue:latest
    ports: ["8081:8081"]
    environment:
      - DATABASE_URL=postgres://...
  vectis-worker:
    image: vectis/worker:latest
    environment:
      - VECTIS_QUEUE_ADDR=queue:8081
      - VECTIS_LOG_SERVICE_ADDR=log-service:8084
      - VECTIS_HEARTBEAT_SERVICE_ADDR=heartbeat-service:8085
      - VECTIS_WORKER_TOKEN=${WORKER_TOKEN}
  vectis-log-service:
    image: vectis/log-service:latest
    ports: ["8084:8084"]
    environment:
      - VECTIS_STORAGE_BACKEND=s3
      - VECTIS_S3_BUCKET=vectis-logs
  vectis-heartbeat-service:
    image: vectis/heartbeat-service:latest
    ports: ["8085:8085"]
    environment:
      - VECTIS_DATABASE_URL=postgres://...  # Own DB connection
      - VECTIS_ORPHAN_GRACE_PERIOD=2m
  vectis-trigger:
    image: vectis/trigger:latest
    ports: ["8083:8083"]
    environment:
      - VECTIS_QUEUE_ADDR=queue:8081
      - VECTIS_TRIGGER_TOKEN=${TRIGGER_TOKEN}
```

**Kubernetes**

- Deployments for API, frontend, queue, log-service, heartbeat-service, workers, triggers
- ConfigMap for shared config
- Secret for database and vault credentials
- Services for API, queue, log-service, heartbeat-service
- Horizontal scaling for workers, triggers

### Configuration

```toml
# config.toml
[database]
    type = "postgres"  # or "sqlite"
    connection = "${DATABASE_URL}"

[api]
    addr = ":8080"

[frontend]
    addr = ":3000"

[queue]
    addr = ":8081"

[log_service]
    addr = ":8084"
    # Log Service has its own storage config
    storage_backend = "filesystem"  # or "s3"
    storage_path = "/var/lib/vectis/logs"

# Heartbeat Service (Phase 2+)
[heartbeat_service]
    addr = ":8085"
    # Heartbeat Service has its own database connection
    database_url = "${HEARTBEAT_DATABASE_URL}"
    orphan_grace_period = "2m"
    heartbeat_interval = "30s"

[triggers.webhook]
    addr = ":8083"
    queue_addr = "${QUEUE_ADDR}"

[triggers.cron]
    queue_addr = "${QUEUE_ADDR}"

[triggers.polling]
    queue_addr = "${QUEUE_ADDR}"
    interval = "60s"

[worker]
    queue_addr = "${QUEUE_ADDR}"
    log_service_addr = "${LOG_SERVICE_ADDR}"
    heartbeat_service_addr = "${HEARTBEAT_SERVICE_ADDR}"
    executor = "hybrid"  # or "container", "process"
    # Which queues this worker subscribes to (in priority order)
    subscribed_queues = ["critical", "default"]
    # Capabilities (auto-detected if enabled)
    auto_detect_capabilities = true
    # Or manual:
    # capabilities = ["docker", "golang", "nodejs"]

[queues]
    [queues.critical]
        drain_order = 1
    [queues.default]
        drain_order = 2
    [queues.batch]
        drain_order = 3

[drain_weight]
    critical = 5
```

---

## Upgrade & Rollback Strategy

**Phase 1:** Simple stop/start, recreate everything

- Frequent teardown expected
- No backward compatibility requirements

**Production (Phase 5+):**

**Version Compatibility:**

- Single version for entire Vectis deployment (all components same version)
- Semantic versioning (MAJOR.MINOR.PATCH)
- Support N and N-1 major versions

**Rolling Upgrade:**

1. Backup database (recommended)
2. Apply database migrations
3. Stop old version, start new version
4. Verify health checks pass

**Rollback:**

- Restore database from backup if migration fails
- Redeploy previous version

**Breaking Changes:**

- Deprecation warnings in N
- Breaking changes in N+2
- Migration guides published with major releases

---

## 15. Technology Recommendations


| Component   | Language         | Notes                               |
| ----------- | ---------------- | ----------------------------------- |
| API Server  | Go               | REST + internal gRPC                |
| Frontend    | TypeScript/React | SPA                                 |
| Queue       | Go               | gRPC-first                          |
| Worker      | Go               | VCS may be different language later |
| Triggers    | Go               |                                     |
| Log Service | Go               |                                     |


**Initial Language: Go for all components**

- Single language simplifies development
- Easy to embed SQLite
- Excellent gRPC tooling
- Can split to other languages later when needed

---

## Configuration

**Configuration file:** `config.toml` (or via `-config` flag)

- Use config files for complex deployments (multiple queues, sites, fine-tuned settings)
- Use environment variables alone for simple/containerized deployments

**Environment variables:**

- All settings overridable via `VECTIS_`* prefix
- Format: `VECTIS_<SECTION>_<KEY>` (e.g., `VECTIS_DATABASE_URL`, `VECTIS_WORKER_TOKEN`)
- For simple deployments, env vars alone are sufficient (no config file needed)

### Sensible Defaults

To minimize configuration friction:

**Queues:**

- Single "default" queue auto-created if no queues defined
- Queues auto-created when first referenced (no explicit setup needed)
- No multi-queue config required for simple deployments

**Workers:**

- `subscribed_queues = []` subscribes to all queues
- Capabilities auto-detected if not explicitly configured

**Pipelines:**

- Default pipeline path: `.vectis.yml` at repo root
- Step defaults: 10m timeout, no explicit config needed
- Inline pipeline via API for ad-hoc jobs (no project/repository required)

**Ad-hoc Jobs:**

```bash
# Run a script without setting up a project
curl -X POST http://localhost:8080/api/v1/jobs \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "Quick test",
    "steps": [
      {"name": "Run", "type": "shell", "command": "echo hello"}
    ]
  }'
```

**Service Discovery:**

- Single-node: Use environment variables (no catalog needed)
- Multi-node: Use cluster catalog for dynamic discovery
- Docker Compose: Add services to `docker-compose.yml`, env vars propagate automatically

```yaml
# docker-compose.yml example
services:
  vectis-queue:
    image: vectis/queue
    environment:
      - VECTIS_DATABASE_URL=postgres://...
  vectis-worker:
    image: vectis/worker
    environment:
      - VECTIS_QUEUE_ADDR=queue:8081
      - VECTIS_HEARTBEAT_SERVICE_ADDR=heartbeat:8085
```

```toml
[server]
  api_addr = ":8080"
  queue_addr = ":8081"

[database]
  type = "sqlite"  # or "postgres"
  connection = "${VECTIS_DATABASE_URL:vectis.db}"

[worker]
  token = "${VECTIS_WORKER_TOKEN}"  # Required - fails if missing
  shutdown_timeout = "5m"
```

**Validation:**

- Configuration validates at startup
- Fails fast with clear error messages if required fields missing or invalid
- Unknown fields logged as warning

**Environment variable example:**

```bash
# Equivalent to config above
VECTIS_SERVER_API_ADDR=":8080" \
VECTIS_DATABASE_CONNECTION="/path/to/db" \
./vectis run queue
```

---

## 16. Architectural Strengths

- **Pull-based workers**: Queue is simple, workers autonomous
- **Multi-queue with weighted drain**: Prevents starvation, supports priority tiers
- **Pluggable storage**: Dev (simple) vs Prod (scalable)
- **Separate triggers**: Can scale independently, language-agnostic
- **Control channel**: Fast cancellation without polling
- **Long polling**: Reduces latency without persistent connections
- **Pipeline-as-code**: Job definitions in repo with override support
- **Multi-site federation**: Central config, local execution

---

## 17. Open Questions - RESOLVED


| Question             | Decision                                                                              |
| -------------------- | ------------------------------------------------------------------------------------- |
| Artifact storage     | Pluggable: filesystem (dev), S3 (prod); worker uploads directly                       |
| Secret management    | Pluggable: built-in encrypted (dev), external Vault (prod); worker fetches at runtime |
| Config format        | TOML                                                                                  |
| Initial language     | Go for all components                                                                 |
| Web UI               | Separate SPA with API Server behind it                                                |
| Logs                 | Real-time streaming, pluggable backends, plain text                                   |
| Log retention        | Per-project age-based                                                                 |
| Workspace            | Ephemeral per-job, opt-in caching                                                     |
| VCS                  | Worker-controlled, pluggable (Git default, null for no-repo)                          |
| Job recovery         | Configurable, default manual retry                                                    |
| Job cancellation     | API Server → Worker (direct gRPC), best-effort in Phase 1                             |
| Service discovery    | Static (dev), K8s (prod), registry (large scale)                                      |
| Queue backend        | In-memory (Phase 1), append-only log (Phase 2+), pluggable                            |
| Metrics              | Pluggable, push and pull                                                              |
| Rate limiting        | Per-project API rate limits                                                           |
| Pipeline definitions | In-repo (`.vectis.yml`), configurable path                                            |
| Override mechanism   | Colocated in override branch (`vectis/overrides`), tied to commit                     |
| Trigger config       | In pipeline file (webhook/poll/cron)                                                  |
| Concurrency modes    | single (default), concurrent, queue (pipeline-level only)                             |
| API boundaries       | Public (API Server) vs Internal (Queue)                                               |
| RBAC                 | viewer/trigger/operator/admin                                                         |
| Worker auth          | Static token with role claims + mTLS                                                  |
| Trigger auth         | Static token with role claims + mTLS                                                  |
| Multi-site           | Central DB with read replicas, local execution per site                               |
| Testing              | Vectis tests Vectis (dogfooding)                                                      |
| Config env prefix    | `VECTIS_`*                                                                            |
| Config validation    | Fail-fast at startup                                                                  |
| Pipeline validation  | Strict by default, reject invalid                                                     |
| Secrets scanning     | External tools responsibility                                                         |
| Protocol mapping     | REST at edge (8080), gRPC internally                                                  |
| Worker registration  | None - token-based auth only                                                          |
| Worker tracking      | None - Queue doesn't track workers                                                    |
| Heartbeat            | Separate Heartbeat Service component (Phase 2+)                                       |
| Log streaming        | Worker → Log Service (gRPC) → Frontend (WebSocket)                                    |
| Notifications        | Sent by worker on completion/failure                                                  |
| Pipeline fetch       | Worker fetches during checkout                                                        |
| VCS credentials      | Worker fetches from secrets                                                           |
| Orphan recovery      | Manual admin intervention only                                                        |
| Worker parallelism   | One job per worker process                                                            |
| Step failure         | Worker-driven: worker reports and decides                                             |
| API Gateway          | Part of API Server component                                                          |
| Cancellation         | API Server → Worker (direct gRPC, Port 8086), best-effort in Phase 1                  |
| Orphan update        | Background job marks stale jobs orphaned (Phase 2+)                                   |
| WebSocket owner      | Log Service serves WebSocket                                                          |
| Worker ID validation | No validation in Phase 1; JWT in Phase 2                                              |
| Artifact tracking    | Artifact entity in data model                                                         |
| Pipeline validation  | After claim, before executing steps                                                   |
| Auto-retry           | Worker handles internally (re-executes job)                                           |
| Override access      | Same VCS credentials as main checkout                                                 |
| Multi-site heartbeat | Per-site Heartbeat Service (Phase 2+)                                                 |
| Webhook→Project      | Secret-based mapping (webhook secret maps to project)                                 |
| Notifications        | Worker reads from pipeline, sends directly                                            |
| Step results         | StepResult entity in data model                                                       |
| Capability filtering | Queue-side filtering (Phase 1)                                                        |
| Queue subscription   | Worker config specifies subscribed queues                                             |
| Heartbeat DB config  | Own database config section (Phase 2+)                                                |


---

## 18. Performance & Scaling

### 18.1 Job Start Latency

Target: Sub-second to 1-second job start time

**Job Fetching:**
Workers long-poll `FetchJob()` with configurable timeout. When jobs are available:

- Queue responds immediately with matching job
- Worker claims job via `ClaimJob()`
- Long poll naturally provides low-latency job delivery

### 18.2 Scale Thresholds


| Scale        | Configuration            | Notes                                           |
| ------------ | ------------------------ | ----------------------------------------------- |
| Hundreds/day | Single Queue instance    | Default configuration, validated via benchmarks |
| Higher scale | Multiple Queue instances | Shard queues across instances as needed         |


**Note:** Scale targets are goals to validate through benchmarking. Initial release targets hundreds of builds/day.

**Queue sharding:**

- Different Queue instances handle different queues (e.g., Queue-A handles `critical`, Queue-B handles `default`)
- Workers discover Queue instances via service discovery
- Increases operational complexity - add only when needed

### 18.3 Known Bottlenecks


| Bottleneck                        | Mitigation                                                               |
| --------------------------------- | ------------------------------------------------------------------------ |
| Database contention on job claims | Atomic claims via DB transactions; horizontal scaling via queue sharding |
| External secrets fetch latency    | Visible via distributed tracing; use fast secret manager or cache        |
| External VCS latency              | Visible via distributed tracing; use local VCS mirrors                   |
| Log WebSocket connections         | Low concurrency in practice (typically <5 simultaneous viewers per job)  |


### 18.4 Performance Targets


| Metric                 | Target                  | Architecture Support               |
| ---------------------- | ----------------------- | ---------------------------------- |
| Job start latency      | <1 second               | Control Channel push notifications |
| Job claim throughput   | ~10k TPS                | PostgreSQL with tuning             |
| Concurrent workers     | ~10k per Queue instance | gRPC handles concurrent streams    |
| Concurrent job viewers | <10 per job             | Log WebSocket multiplexing         |


---

## 19. Next Steps

1. Set up repository structure
2. Define detailed API contracts (OpenAPI + gRPC)
3. Implement Phase 1: Queue + basic worker communication
4. Build log service with gRPC input + WebSocket output
5. Implement triggers
6. Build API server and frontend SPA

