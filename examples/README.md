# Examples

Run these from the repository root after `./bin/vectis-local` is up and first-admin setup has finished. `jobs run` submits the file once and streams logs with `--follow`. Storing a job for later triggers is the `jobs create` flow in the repository [README](../README.md).

```bash
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

Start with `sequenced.json`. Each later file adds one control-flow idea. The default local stack can run the jobs in the first table with no extra setup.

| File | What it shows | What you should see |
| --- | --- | --- |
| `sequenced.json` | `builtins/sequence` runs three `builtins/script` steps in order. | Three log lines, then a succeeded run. |
| `parallel.json` | A local sequence with a distributed `builtins/parallel` branch for unit tests and lint. | `Preparing workspace`, both check lines, then `All checks complete`. |
| `conditional.json` | `builtins/if` runs `then` when `deploy.changed` exists and `else` otherwise. | `No deploy changes detected`. A fresh workspace has no `deploy.changed` file, so the else branch runs. |
| `fallback.json` | `builtins/fallback` tries choices in order and keeps the first success. | `running portable fallback build`. `./preferred-build.sh` is absent, so the first choice fails and the second runs. |
| `dataflow.json` | A script writes `outputs.json`, and the next `builtins/test` step reads that `command` output. | A succeeded run. The test step executes the command produced by the first step. |
| `outputs.json` | A script publishes a JSON output file through `with.outputs`. | A succeeded run whose task output includes `image` and `ok`. |

## Custom action

`custom-greet.json` calls `examples/greet@v1` from `actions/examples/greet/action.json`. The API, worker, and CLI load that manifest only when the local action root is set. Export it before starting `vectis-local`, and restart the stack if it is already running:

```bash
export VECTIS_ACTION_REGISTRY_LOCAL_ROOTS=examples/actions
./bin/vectis-local
```

In another terminal, from the repository root:

```bash
export VECTIS_ACTION_REGISTRY_LOCAL_ROOTS=examples/actions
./bin/vectis-cli jobs run examples/custom-greet.json --follow
```

The run logs `Hello, Vectis`. `actions/examples/flaky-once/` is the custom action used by the smoke jobs below. It fails once, then succeeds, so the harness can check registry retry behavior.

## Jobs that need more setup

| File | Setup |
| --- | --- |
| `secrets.json` | Follow [Local SPIFFE Secrets Smoke Test](../website/docs/operating/deployment/local-spiffe-secrets-smoke-test.md). The job checks the checksum of the task-scoped secret file. |
| `resilient.json` | Shows `builtins/retry`, `builtins/timeout`, and `builtins/finally`. The body runs `mage build` and the cleanup step runs `mage clean`, so the job succeeds only in a workspace where those commands work. |
| `sleep.json` | Sleeps for 10000 seconds. Start it when you want a run that stays active, then cancel it with `./bin/vectis-cli runs cancel <run-id>`. |

## Smoke harness jobs

These files are inputs for automated smoke tests. Their scripts, secrets, and cluster layout come from the harness that submits them.

| File | Harness |
| --- | --- |
| `e2e-canonical.json` | Local end-to-end smoke. It needs the encryptedfs smoke secret and `VECTIS_ACTION_REGISTRY_LOCAL_ROOTS=examples/actions`. |
| `e2e-kubernetes.json` | Kubernetes workload smoke. |
| `e2e-kubernetes-worker-core.json` | Kubernetes worker-core provider smoke. |
| `e2e-kubernetes-cancel.json` | Worker-control cancel smoke. |
| `e2e-kubernetes-scale.json` | Worker scaling smoke. |
| `e2e-kubernetes-orphan.json` | Pod-loss orphan smoke. |
| `e2e-kubernetes-repair.json` | Explicit orphan repair smoke. |
| `e2e-kubernetes-knox.json` | Knox secrets-provider smoke. |
| `e2e-gerrit-change.json` | Gerrit checkout and review smoke. See [deploy/gerrit/README.md](../deploy/gerrit/README.md). |

Kubernetes harness details are in [deploy/kubernetes/README.md](../deploy/kubernetes/README.md).

## Worker core sample

`worker-core-external/` is a small Go program that serves a worker core over a Unix socket. The walkthrough is in [Worker Core SDK](../website/docs/developing/worker-core-sdk.md). The Kubernetes-backed core lives under `extensions/worker-core/`.
