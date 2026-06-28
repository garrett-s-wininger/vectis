# Glossary

This page defines the words Vectis uses across the docs, API, and CLI. For the system picture, start with [Architecture](./architecture.md). For job examples, see [Your First Job](../using/your-first-job.md).

## A

**Ack** — Short for acknowledge. After a worker accepts responsibility for a queued delivery, it tells the queue that the delivery is complete from the queue's point of view. Acking prevents the same queue delivery from being handed out again.

**Action** — One executable unit inside a job node. Examples include process actions like `builtins/script`, `builtins/test`, `builtins/checkout`, `builtins/upload-artifact`, and extension actions such as `gerrit/review@v1`; control-flow actions like `builtins/sequence`, `builtins/parallel`, and `builtins/retry`; and the `builtins/result` summary action. A node chooses its action with `uses` and passes inputs with `with`.

**API (`vectis-api`)** — The HTTP service clients talk to. It stores job definitions, creates runs, reports status, streams logs through SSE, and exposes health and metrics.

## C

**Claim** — A worker's database-backed attempt to take exclusive ownership of a run before executing it. Claims help prevent two workers from executing the same persisted run at the same time.

**Client** — Anything that talks to the HTTP API, including `vectis-cli`, scripts, dashboards, and custom integrations.

**Cron (`vectis-cron`)** — The scheduler process. It reads schedules from the database and enqueues runs when they are due.

**SCM poller (`vectis-scm-poller`)** — The source-control polling process. It claims due SCM poll trigger specs and records deduplicated provider events by stable event key.

## D

**Delivery** — One handoff of queued work from `vectis-queue` to a worker. A run can have more than one delivery over time if queue handoff or worker execution needs recovery.

**Dispatch** — The act of submitting recorded work to the queue. The API, cron, reconciler, cell ingress, and worker task continuations can all dispatch work.

## E

**Enqueue** — Add work to `vectis-queue` so a worker can pick it up.

**Ephemeral run** — A run started from an inline job definition, usually with `vectis-cli jobs run <file>` or `POST /api/v1/jobs/run`. The definition is stored enough for recovery, but it is not a reusable source-backed job that users trigger later by job ID.

## I

**Idempotency key** — A client-provided key used to retry a job submission safely. If a network error leaves the client unsure whether a run was created, retrying with the same key lets Vectis return the original response instead of creating a duplicate run. See [Idempotency And Retries](../using/idempotency-and-retries.md).

## J

**Job** — A definition of work Vectis can execute. A job is a tree of nodes, starting at `root`.

**Job definition** — The source-controlled or submitted JSON shape that describes a job. Reusable jobs keep a definition under a stable job ID in a registered repository; ephemeral runs submit the definition inline.

**Job ID** — The stable name of a reusable job definition. This is different from a node ID inside the job tree.

## L

**Lease** — A time-bounded ownership record a worker keeps while executing a run. Leases help Vectis detect work that may have been abandoned by a dead or unreachable worker.

**Log service (`vectis-log`)** — The service that receives log chunks from workers and stores run logs. Clients normally read logs through the API or CLI, not by calling the log service directly.

## N

**Namespace** — A scope for jobs and permissions. Namespaces let operators organize jobs and grant roles such as viewer, trigger, operator, or admin within a tree.

**Node** — One step-like vertex in a job tree. A node has an `id`, a `uses` action, optional `with` inputs, and optional child nodes attached through typed `ports`. The legacy `steps` field is shorthand for a node's primary port.

**Node ID** — The identifier for one node inside a job. Node IDs must be unique within a job. They are not the same thing as the top-level job ID.

## P

**Pinned address** — A service address, such as queue, orchestrator, log, artifact, secrets, or registry, set explicitly in configuration. Pinned addresses let a component avoid dynamic lookup or environment-specific defaults for that dependency.

**Producer** — A component that submits work to the queue. Current producers are `vectis-api`, `vectis-cell-ingress`, `vectis-cron`, `vectis-reconciler`, and `vectis-worker` for task continuations. `vectis-scm-poller` records SCM trigger events before the dispatch step.

## Q

**Queue (`vectis-queue`)** — The internal FIFO handoff service between producers and workers. The database records what should run; the queue helps workers receive the next piece of work.

**Queued run** — A run recorded in the database as waiting for execution. A queued run may or may not currently have a matching item in the queue, which is why the reconciler exists.

## R

**Reconciler (`vectis-reconciler`)** — The repair process that looks for queued runs that need another queue handoff. It is what closes the gap when a run is recorded in the database but was not successfully submitted to the queue.

**Registry (`vectis-registry`)** — Internal service discovery. Queue, log, and artifact can register their addresses, and other components can resolve those addresses instead of using pinned configuration.

**Run** — One execution of a job. A reusable job can have many runs over time; each run has its own run ID, status, timestamps, logs, and dispatch history.

**Run ID** — The unique ID for one run. Use it to inspect status, cancel a run, or stream logs.

**Run index** — A per-job sequence number used to order runs for the same reusable job.

## S

**Schedule** — A database-backed rule that tells `vectis-cron` when to create runs.

## W

**Worker (`vectis-worker`)** — The execution process. A worker dequeues work, claims a run, executes the job tree, streams logs, and writes final run status.

## Future Terms

**Federation / multi-site** — A target design for coordinating more than one Vectis site. It is not part of the shipped runtime today. See [Planning](../developing/roadmap/planning.md#federation-direction).

**`.vectis.yml`** — A planned pipeline-as-code format. Today, jobs are submitted as JSON-shaped job definitions.
