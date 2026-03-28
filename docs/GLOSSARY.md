# Glossary

Shared vocabulary for Vectis docs and APIs. Canonical job structure: [`api/proto/common.proto`](../api/proto/common.proto). For architecture and configuration, see [ARCHITECTURE.md](ARCHITECTURE.md) and [CONFIGURATION.md](CONFIGURATION.md).

## A

**Ack (acknowledge)** — After a worker has taken responsibility for a queued **delivery**, it tells the **queue** to finalize that delivery so the message is not redelivered. If ack fails, the run may be marked failed or retried depending on context.

**Action** — A unit of work invoked by the executor (e.g. built-ins **shell**, **checkout**, **sequence**). Wired in `internal/action/`; referenced from a job **node** via `uses` / `with` / nested `steps`.

## C

**Claim** — A **worker** uses the database to take exclusive responsibility for a **run** (`TryClaim`), moving it from **queued** toward **running** so another worker does not execute the same run.

**Client** — Anything that drives Vectis over **HTTP** (browser, automation, **`vectis-cli`**) or internal services that call the API. Not to be confused with gRPC *clients* inside the codebase.

**Cron (`vectis-cron`)** — Process that reads **schedules** from the database and **enqueues** work when a schedule is due. Separate from the HTTP API.

## D

**Delivery (queue)** — A single handing-off of a **job** message from the **queue** to a **worker**, identified by a **delivery id**. Subject to timeouts and **ack**; may be retried if not acked in time.

**Dispatch** — (1) Submitting a **run** to the **queue** after it exists in the database (API/cron/reconciler). (2) Database field **`last_dispatched_at`** tracks when a run was last submitted to the queue; the **reconciler** uses it with **queued** status to find runs that need another enqueue attempt.

## E

**Enqueue** — Add a **job** payload (with **`run_id`** set) to the **queue** so a **worker** can **dequeue** it. Producers: API (often asynchronously after HTTP **202**), **cron**, **reconciler**.

**Ephemeral run** — A run started from **`POST /api/v1/jobs/run`** with an inline body: the definition is stored in **`job_definitions`** for recovery without a normal **stored job** row. See [PLANNING.md](PLANNING.md) §2.5.

## J

**Job** — The unit of execution: protobuf **`Job`** with **`id`**, **`run_id`**, **`root`** **node** tree, and optional queue **`delivery_id`**. Over HTTP, usually represented as JSON matching that shape.

**Job definition** — The **`root`** tree and metadata needed to execute a **run**; persisted for **stored jobs** and for **ephemeral** paths as above.

## L

**Lease** — Time-bounded lock a **worker** holds on a **run** while executing; renewed until completion or failure. Prevents another worker from treating the run as free while work is in progress.

**Log service (`vectis-log`)** — Ingests **log** streams from workers over gRPC and serves them to consumers over HTTP. Does not own authoritative **run** status (the database does).

## N

**Node** — One vertex in the job graph: **`id`**, **`uses`** (action name), **`with`** (parameters), **`steps`** (children). The **`root`** node is the entry point.

## P

**Pinned address** — A configured **queue**, **log**, or **registry** address set explicitly in config/env instead of learning it only through the **registry**. See [CONFIGURATION.md](CONFIGURATION.md).

**Producer** — Component that **enqueues** work: **API**, **cron**, **reconciler**.

## Q

**Queue (`vectis-queue`)** — gRPC service holding pending **job** messages for **workers**; supports **enqueue**, **dequeue**, **try dequeue**, and **ack**. May persist backlog to disk (configurable). See [FAILURE_DOMAINS.md](FAILURE_DOMAINS.md#vectis-queue).

## R

**Reconciler (`vectis-reconciler`)** — Background process that finds **runs** stuck in **queued** long enough and **enqueues** them again (e.g. after a failed async enqueue or queue loss). Complements the API’s retry behavior.

**Registry (`vectis-registry`)** — gRPC **service discovery**: **queue** and **log** register their listen addresses; consumers resolve them when addresses are not **pinned**.

**Run** — One execution of a **job** for a given **`job_id`**: identified by **`run_id`**, ordered among siblings by **`run_index`**, with **status** (e.g. **queued**, **running**, success/failure) in the database.

## S

**Schedule** — Cron expression and associated **job** stored in the database; **`vectis-cron`** fires due schedules by **enqueueing** a new **run**.

**Stored job** — A job definition kept under a stable **`job_id`** (CRUD under **`/api/v1/jobs/...`**), as opposed to an **ephemeral run** only.

## W

**Worker (`vectis-worker`)** — Process that **dequeues** **jobs**, **claims** **runs**, executes **actions**, streams logs to the **log service**, and updates **run** status in the database. One **run** at a time per worker process unless you run multiple processes.

## Related terms (elsewhere)

**Federation / multi-site** — Target design only; see [FEDERATION.md](FEDERATION.md).

**`.vectis.yml`** — Target pipeline file format; not loaded by the shipped runtime ([PLANNING.md](PLANNING.md) §1).
