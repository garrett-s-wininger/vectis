# Examples

Run these from the repository root after `./bin/vectis-local` is up and first-admin setup has finished. `jobs run` submits the file once and streams logs with `--follow`. Storing a job for later triggers is the `jobs create` flow in the repository [README](../README.md).

```bash
./bin/vectis-cli jobs run examples/sequenced.json --follow
```

Start with `sequenced.json`. Each later file adds one control-flow idea. The default local stack can run the jobs in the first table with no extra setup.

| File | What it shows | What you should see |
| --- | --- | --- |
| `sequenced.json` | `builtins/sequence` runs three `builtins/script` steps in order. | Three log lines, then a succeeded run. |
| `log-viewer.json` | Distributed tasks with stdout, stderr, split writes, and an intentional failure. | A failed run by design. Inspect each task in the UI; `not-reached` has no execution. |
| `run-controls.json` | A distributed sequence with 30 seconds of heartbeat output. | Cancel it from run details, then use Run again to create a new run; letting it finish succeeds. |
| `parallel.json` | A local sequence with a distributed `builtins/parallel` branch for unit tests and lint. | `Preparing workspace`, both check lines, then `All checks complete`. |
| `conditional.json` | `builtins/if` runs `then` when `deploy.changed` exists and `else` otherwise. | `No deploy changes detected`. A fresh workspace has no `deploy.changed` file, so the else branch runs. |
| `fallback.json` | `builtins/fallback` tries choices in order and keeps the first success. | `running portable fallback build`. `./preferred-build.sh` is absent, so the first choice fails and the second runs. |
| `dataflow.json` | A script writes `outputs.json`, and the next `builtins/test` step reads that `command` output. | A succeeded run. The test step executes the command produced by the first step. |
| `outputs.json` | A script publishes a JSON output file through `with.outputs`. | A succeeded run whose task output includes `image` and `ok`. |

## Try the run controls

In the UI, open **Runs → Run Once**, load `examples/run-controls.json`, and submit it. The heartbeat task logs once per second for 30 seconds, giving you time to choose **Cancel run → Confirm cancel**. The UI shows **Cancel requested** until the worker confirms **Cancelled**; the remaining task should not execute.

Choose **Run again** on the cancelled run. This opens a new run using the original saved definition, with **Re-run of** shown alongside its identifiers in the run summary. Leave it running to see it succeed after about 30 seconds, or cancel it again. The original run and its logs remain available. This is a new run, rather than an additional attempt on the original execution.

The run-once breadcrumb identifies the job by its UUID suffix before the run number. The selected task stays selected when it finishes, and known graph nodes remain visible while a task snapshot is incomplete. Logs scroll inside a fixed-height viewport. If an established stream closes without a completion marker, the UI briefly waits for run status; a confirmed terminal run shows a neutral closure notice, while a stream failure during active work still reports an error.

You can also submit the same example from the repository root:

```bash
./bin/vectis-cli jobs run examples/run-controls.json
```

The scripts use `sh`, `printf`, and `sleep`, so they run directly on Linux, macOS, or WSL without services or downloads.

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

## Exercise The Log Viewer

After restarting the updated local stack, open **Runs → Run Once → Load JSON file**, select `examples/log-viewer.json`, review the definition, and click **Submit run**.

You can also submit it from the repository root:

```sh
./bin/vectis-cli jobs run examples/log-viewer.json --follow
```

Open **Runs**, select `log-viewer-demo`, and select tasks in the graph. The run deliberately fails with exit code 7 after both parallel tasks finish; `not-reached` remains without an execution attempt.

- **stdout-demo:** each message occupies its own line. `a line written in two parts` must remain one line despite two writes separated by a one-second pause.
- **stderr-demo:** warning lines appear in the stderr color, and the task's stdout remains readable.
- **intentional-failure:** its failure messages belong only to that task's selected output.
- **not-reached:** shows the no-execution empty state.
- **All run output:** includes messages from every executed task. Parallel task order can vary.

This demo needs only `sh` and `sleep`; it does not use network services, secrets, or extra action manifests. Its first dispatch creates one attempt per executed task. To exercise the attempt selector, wait for the deliberate failure, copy its run ID from the UI, and requeue that **same run**:

```sh
./bin/vectis-cli runs repair mark-queued RUN_ID --reason "Exercise log viewer attempt history"
```

The reconciler dispatches it again. Once it finishes, refresh the same run page and select `stdout-demo`, `stderr-demo`, or `intentional-failure`. **Attempt** now offers attempts 1 and 2, plus **All attempts**; switching attempts changes the execution ID and limits output to that execution. The second dispatch deliberately fails too. Submitting the JSON again creates a different run and does not add attempts to the original.

This uses the operator repair path to exercise redispatch, not an automatic workflow retry policy. A `builtins/retry` loop inside an action remains one execution attempt. Compaction retains attempt identities, outcomes, and timing until the run is deleted; log availability separately follows the log retention window.
