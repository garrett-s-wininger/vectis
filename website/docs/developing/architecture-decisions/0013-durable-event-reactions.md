# ADR 0013: Durable event reactions

## Status

Proposed

## Context

Vectis needs a durable way to react to run and definition lifecycle events such
as queued, started, succeeded, failed, cancelled, abandoned, dispatch repair, and
validation failure. The first product use is notifications, but the same shape is
also the foundation for future reactions such as "trigger another job when this
one completes".

Reaction execution must not depend on the job action tree executing. This
matters because job-defined finalizers have the same failure shape as Jenkinsfile
post blocks: if the definition is invalid, if graph materialization fails, if the
worker never reaches the finalizer, or if the finalizer action is itself
malformed, the reaction can be lost exactly when the operator needs it most.
Vectis already has `builtins/finally` for cleanup inside a valid local execution
tree; event reactions need a separate durability boundary.

Existing pieces influence the design:

- The SQL database is the durable source of truth for runs, task executions,
  dispatch events, trigger invocations, namespaces, RBAC, and audit records.
- Run creation can fail before a valid task DAG or run ID exists, especially for
  ephemeral runs and future pipeline-as-code compilation.
- Action descriptors already provide a useful contract for typed inputs,
  versioning, policy, custom runtimes, and frozen implementation identity.
- Operators should be able to manually send a notice through the same routing,
  retry, audit, and local-test path used by automatic lifecycle notifications.
- Multi-cell execution means a local cell can own execution while global
  operators still need coherent status and reaction visibility.
- Tests need a local notification target that can be asserted without sending
  email, Slack, webhooks, or other external side effects.

## Decision

Introduce event reactions as a control-plane subsystem driven by durable
lifecycle events, not by job DAG nodes.

The reaction pipeline has five separate concepts:

| Concept | Responsibility |
| --- | --- |
| Reaction event | A durable fact that something happened, such as `run.failed`, `run.succeeded`, or `definition.validation_failed`. |
| Reaction subscription | Operator or namespace-owned policy deciding which events should invoke which targets. |
| Reaction target | A configured out-of-DAG action reference plus target configuration, such as local notification, webhook, email, chat, or future job trigger. |
| Reaction invocation | The retryable record of running one reaction action for one event and one target. |
| Manual notice | An operator- or system-created event that uses the same routing and invocation path as automatic lifecycle reactions. |

Run, trigger, validation, dispatch, repair, and task finalization paths enqueue
reaction events after their owning state mutation commits. A reaction event is
intentionally smaller and more stable than the full run payload. It should
include identifiers, timestamps, namespace/job/run metadata when known, event
type, terminal status when known, failure code and reason when safe to expose,
owning cell, trigger metadata, and links or IDs needed to query logs and
artifacts. Validation events may have no run ID because the definition can fail
before Vectis creates a run row.

Reaction events are evaluated by a reaction worker loop. The first
implementation can live inside `vectis-api` or a small `vectis-reactions`
binary, but the storage contract should not require in-process execution. The
loop claims pending reaction invocations through the database, runs the selected
reaction action, records success or retryable failure, and leaves enough
operator-visible state to debug stuck reactions.

Reaction definitions are not part of `api.Job.root` and are not represented as
child nodes. Future job or project authoring may allow reaction preferences, but
those preferences compile to subscriptions outside the run DAG. Invalid job
definitions can still emit `definition.validation_failed` because the reaction
path only depends on the API boundary and durable event table, not a valid task
graph.

Define reaction execution as an action invocation that runs outside the normal
job DAG. Reaction targets store a `uses` reference resolved through the action
registry, plus target configuration such as a mailbox, channel, endpoint, or
downstream job ID. When a reaction invocation is created, Vectis freezes the
resolved descriptor or digest on the invocation record. Retries use that frozen
reaction action identity; target edits affect future invocations, not already
created attempts.

Reaction actions receive a structured event payload and target configuration.
They run with a reaction execution state, not a job execution state: no user job
workspace, no task claim, no task reduction, no action-tree node path, and no
ability to decide the source run result. They may use Vectis-managed target
secrets by reference, subject to reaction target policy, but they do not inherit
arbitrary worker environment or job-scoped secret material.

Automatic lifecycle reactions and manual notices share the same middle path:

1. Create a `reaction_events` row, either from a lifecycle emitter or a manual
   notice request.
2. Resolve matching subscriptions or explicit manual targets.
3. Create durable `reaction_invocations` rows with frozen reaction action
   descriptors.
4. Claim and run those invocations through the reaction worker loop.
5. Record local messages, external send success, downstream run creation, or
   retryable failure.

Manual notices should not bypass this path. A CLI or API request to send a
manual failure notice creates an event such as `manual.notice` with actor,
reason, severity, message, and optional run/job context, then relies on the same
subscription, invocation, retry, and audit machinery.

The first required reaction action is `builtins/notify-local`. It records event
payloads durably in the same SQL database and performs no external I/O. This
gives tests and local development an assertable sink:

- API and validation tests can assert that invalid definitions create local
  `definition.validation_failed` messages.
- Run lifecycle tests can assert success, failure, cancellation, and repair
  messages without depending on external services.
- Operators can inspect local messages while bringing up the subsystem.

Job chaining is a future reaction target, not part of the first implementation
slice. A later `builtins/trigger-job` reaction action can consume the same event
payload, use the same invocation/retry/audit path, and create a new run without
being part of the completed job's DAG.

Reaction invocation uses at-least-once semantics. Reaction actions receive a
stable event ID and reaction invocation ID so external sinks and downstream run
creation can deduplicate where possible. Vectis records invocation state, but it
does not promise exactly once side effects outside its database.

The minimum durable tables are:

| Table | Purpose |
| --- | --- |
| `reaction_events` | Append-only lifecycle facts with event ID, source, type, nullable run/job/namespace context, actor when applicable, payload JSON, creation time, and source cell. |
| `reaction_subscriptions` | Filter and target binding, scoped globally or to a namespace/project/job once those scopes exist. |
| `reaction_targets` | Out-of-DAG action reference, target kind, configuration, and secret references. |
| `reaction_invocations` | Per-event, per-target action invocation state, frozen descriptor/digest, attempts, next retry time, last error, and completed time. |
| `reaction_local_messages` | Local notification payload copies for tests, local development, and operator inspection. |

For v1, filters should stay deliberately small: event type, namespace, job ID,
trigger type, terminal run status, and owning cell. More expressive filtering
can come later after the basic invocation and observability contract is proven.

## Consequences

- Reactions can fire for invalid job definitions, failed validation, missed
  dispatch, cancelled runs, and other states that may never reach a worker.
- `builtins/finally` remains useful for job cleanup, but it is not the system
  reaction primitive.
- Reaction actions reuse the action registry without becoming job nodes.
- The local notification action gives a deterministic integration-test surface
  before any external transport is implemented.
- Manual notices, operator re-sends, automatic lifecycle notices, and future job
  chaining exercise the same invocation code path.
- Reaction invocation has its own retry and repair model instead of borrowing
  task execution retries.
- Frozen reaction action descriptors make invocation retries explainable even
  when target configuration or action registry state changes later.
- The database gains another append/retry workload; retention and capacity
  policies must include reaction events and invocations.
- Operators need visibility into stuck invocations, retry pressure, target
  failures, and downstream run creation failures.
- Exactly-once external effects remain out of scope. Consumers must tolerate
  duplicate sends or duplicate downstream trigger attempts.

## Open Questions

- Should v1 run the reaction worker loop inside `vectis-api`, or should it
  introduce a separate `vectis-reactions` binary immediately?
- Which scopes exist before projects ship: global, namespace, job, trigger, or
  only global and namespace?
- Should reaction subscriptions be user-managed through the public API at
  launch, or operator-managed through configuration until RBAC expectations are
  settled?
- How should multi-cell deployments aggregate execution-local events: emit in
  each owning cell, in the global catalog applier, or both with a deduplication
  key?
- Which external notification action should follow `builtins/notify-local`:
  webhook, SMTP, or chat?
- What should the reaction action interface look like: a small sibling of
  `action.Node`, or a constrained profile of existing action descriptors with
  reaction-specific execution state?
- Should manual notices always route through subscriptions, or may authorized
  callers name explicit targets while still creating durable events?
- What retention defaults should apply to events, successful invocations, failed
  invocations, and local messages?

## References

- [Architecture](../../concepts/architecture.md)
- [Failure Domains](../../concepts/failure-domains.md)
- [Job Definition Validation](../../using/job-validation.md)
- [ADR 0003: Database claims and queue deliveries](./0003-database-claims-and-queue-deliveries.md)
- [ADR 0006: Global coordination and cell-local execution](./0006-global-coordination-cell-local-execution.md)
- [ADR 0010: Versioned action registry](./0010-versioned-action-registry.md)
- `internal/job/validation/validation.go` - current API-side job validation boundary
- `internal/dal/` - durable run, trigger, dispatch, and catalog state
- `internal/action/actionregistry/` - descriptor and lifecycle model that future reaction actions can reuse
