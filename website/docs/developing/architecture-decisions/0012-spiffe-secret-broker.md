# ADR 0009: SPIFFE-authenticated secret broker

## Status

Accepted

## Context

Vectis jobs need a first-class way to consume credentials without storing
plaintext values in job definitions, queue payloads, execution payloads, audit
metadata, or service logs. Operators also need a path that can start small for
self-hosted deployments and later integrate with external secret systems such as
Vault or Knox.

Recent worker identity work gives Vectis a per-execution SPIFFE identity and a
bounded SPIRE SVID check before action code runs. Task-based execution also means
workers claim a specific execution attempt before running user code. Those two
facts give the secrets design a natural authorization boundary: the caller must
be a known workload identity and must prove it currently owns the execution that
is asking for secrets.

Options considered:

- **Worker-local mounted secrets only** — simple for operators, but every job on
  that worker can read the same material and Vectis cannot validate, audit, or
  scope secret use.
- **Put secret values in job or queue payloads** — easy to execute, but it
  spreads plaintext through durable stores and debugging surfaces.
- **Direct worker integration with every secret manager** — avoids a new Vectis
  service, but couples workers to provider-specific auth, request, retry, and
  audit behavior.
- **Cell-local broker with provider interface** — adds one service boundary, but
  keeps worker behavior stable while provider implementations evolve.

## Decision

Introduce a cell-local `vectis-secrets` service. Workers resolve declared job
secret references through this service after they claim an execution and before
they mark the execution started.

The first provider is an encrypted filesystem provider. Secret payload files are
encrypted at rest with operator-supplied key material kept outside the payload
directory. The provider owns the on-disk envelope format so later key rotation
and metadata changes do not affect job definitions.

Secret values are delivered to actions as files under the run workspace, rooted
at `.vectis/secrets`. File delivery is the default and only required v1 delivery
mode. The worker may expose `VECTIS_SECRETS_DIR`, but it must not place secret
values themselves in environment variables by default.

`vectis-secrets` authorizes each resolution request with both:

- SPIFFE-authenticated mTLS identity for the calling execution workload, which
  must exactly match the SPIFFE ID the broker derives from the active execution
  record and configured identity template;
- the execution ID and execution claim token proving the worker currently owns
  that task attempt in the cell database;
- an operator-owned secret access policy rule matching the execution namespace,
  job, task, and requested secret reference.

Job definitions store only secret references and delivery metadata. They never
store plaintext values. Execution envelopes, queue payloads, dispatch records,
and execution payload ledgers preserve the same rule.

The broker exposes a provider-backed contract so future Vault, Knox, Kubernetes,
or cloud-secret-manager providers can implement the same resolution behavior
without changing the worker/action materialization path.

## Consequences

- Vectis gets a testable end-to-end secret flow before depending on external
  secret managers.
- Secret access becomes task/execution scoped instead of ambient to a worker
  process.
- Workers depend on a cell-local secrets service when a job declares secrets.
  Jobs with no secret references remain unchanged.
- Operators must run and back up the encrypted filesystem store when they enable
  the built-in provider.
- The first implementation still cannot prevent a trusted job from printing or
  exfiltrating a secret it legitimately receives. It reduces accidental exposure
  and unauthorized resolution, not arbitrary code execution risk.
- A later public secret CRUD API can layer on top of the provider contract. The
  initial path may use operator provisioning tools for the encrypted filesystem
  store.

## References

- [Internal Service Trust](../../concepts/internal-service-trust.md)
- [Secrets And Redaction](../../operating/deployment/secrets-and-redaction.md)
- [Multi-Cell Operation](../../operating/multi-cell.md)
- `internal/workloadidentity/identity.go` — per-execution SPIFFE ID construction
- `internal/spire/workloadapi.go` — current SPIRE Workload API adapter
- `cmd/worker/main.go` — execution claim and pre-action SVID acquisition
