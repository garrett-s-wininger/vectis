# Production Readiness Evidence

Use this record for every release candidate that will be described as
production-ready. Keep the completed copy with the release notes, CI artifacts,
package/container artifacts, and any waiver approvals.

This page is a template, not a promise that every deployment looks the same.
Fill it out for the supported production-v1 path in the release. Mark
experimental features as out of scope unless the release notes deliberately add
them to the supported path.

## Release Identity

| Field | Record |
| --- | --- |
| Release version |  |
| Git commit |  |
| Candidate tag or artifact prefix |  |
| Release owner |  |
| Evidence date |  |
| Production-v1 scope | Single-cell, multi-cell advanced, or other documented shape. |
| Out-of-scope features | Experimental features not covered by this evidence. |

## Artifact Set

Record one immutable version for every artifact needed by the production path.

| Artifact | Version, digest, or path |
| --- | --- |
| Source commit |  |
| Linux service packages |  |
| `vectis-services` meta package |  |
| `vectis-cli` package |  |
| `vectis-local` package, if shipped |  |
| Container images |  |
| Generated systemd artifacts |  |
| Docs site artifact |  |
| SBOMs or attestations, if produced |  |

Minimum command evidence:

```sh
git status --short
git rev-parse HEAD
make release-readiness-report
make package-linux
vectis-cli --version
```

If a package, image, or binary is intentionally not part of the release, record
the waiver in [Known Risks And Waivers](#known-risks-and-waivers).

## Test And Smoke Results

| Check | Result | Evidence location |
| --- | --- | --- |
| `make release-readiness-report` |  |  |
| Docs dependency audit |  |  |
| `make release-local-validate` |  |  |
| `make test-quick` |  |  |
| Postgres integration tests, when required |  |  |
| Linux artifact/package tests |  |  |
| VM deploy smoke, when required |  |  |
| VM package smoke, when required |  |  |
| SQLite/local upgrade smoke |  |  |
| Postgres/reference upgrade smoke |  |  |
| Operator smoke run reaches terminal status |  |  |
| Logs stream or replay for the smoke run |  |  |
| Artifact download or verification, when in scope |  |  |
| Secret resolution smoke, when in scope |  |  |

Use the VM lanes when Linux install, package layout, worker isolation, VM
provider, or systemd behavior changed. If a VM lane cannot run, name the exact
reason and the compensating check.

## Migration And Rollback

| Field | Record |
| --- | --- |
| Schema changed? |  |
| Migration command and result |  |
| Old binary compatibility |  |
| New binary compatibility |  |
| Rollback choice | Previous artifacts, database restore, roll-forward repair, or down migration. |
| Backup required before upgrade? |  |
| Restore point tested? |  |

Attach the migration note required by [Database Migrations](./migrations.md)
for every schema change.

## Config, Secrets, And Security

| Area | Evidence |
| --- | --- |
| New or changed config |  |
| Required secret-manager entries |  |
| API auth and RBAC changes |  |
| Allowed Hosts and trusted proxy CIDRs |  |
| TLS or mTLS posture |  |
| SPIFFE authority and Workload API posture |  |
| Secret broker posture |  |
| Worker isolation posture |  |
| Public API surface changes |  |
| Redaction or audit changes |  |

Confirm that production settings remain consistent with the
[Production Config And Secrets Contract](../operating/deployment/production-config-contract.md)
and [Security](../concepts/security.md).

## Operations Evidence

| Area | Result | Evidence location |
| --- | --- | --- |
| `vectis-cli health check --strict` before upgrade |  |  |
| `vectis-cli health check --json` before upgrade |  |  |
| `vectis-cli health check --strict` after upgrade |  |  |
| `vectis-cli health check --json` after upgrade |  |  |
| API `/health/live` and `/health/ready` |  |  |
| Reconciler running and repairing visibility |  |  |
| Queue, log, and artifact shard storage checks |  |  |
| Backup/restore drill result |  |  |
| Retention schedule or explicit runbook owner |  |  |
| Monitoring and alert rule review |  |  |
| Dashboard or metric snapshot |  |  |
| Service logs checked for startup errors |  |  |

For the backup/restore row, link the completed
[Production v1 drill](../operating/reliability/backup-restore.md#production-v1-drill)
or state why a fresh drill was not required for this release.

Store the JSON health output as an artifact, not only pasted text. The JSON
record preserves check IDs, status, severity, evidence, suggested action, and
documentation links for later comparison.

## Capacity Evidence

| Field | Record |
| --- | --- |
| Capacity-sensitive change? |  |
| Performance check artifact |  |
| Deployed-stack evidence |  |
| Observed limiting component |  |
| Published capacity envelope changed? |  |
| Follow-up issue, if any |  |

When another thread owns performance measurements, link its artifact or summary
here before claiming the release is production-ready.

## Known Risks And Waivers

| Item | Risk | Owner | Expiration or follow-up |
| --- | --- | --- | --- |
|  |  |  |  |

Waivers should be specific. Prefer "VM package smoke skipped because the runner
had no prepared RPM guest; compensated by DEB smoke plus systemd artifact
verification" over broad statements like "not tested."

## Signoff

| Role | Name | Date | Notes |
| --- | --- | --- | --- |
| Release owner |  |  |  |
| Operations reviewer |  |  |  |
| Security reviewer, when needed |  |  |  |
| Capacity reviewer, when needed |  |  |  |

Do not mark the release production-ready until every required row has evidence
or a named waiver.
