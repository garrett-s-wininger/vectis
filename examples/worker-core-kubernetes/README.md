# Kubernetes Worker Core Example

This example implements the Vectis worker-core contract by running a claimed
leaf executable task as a Kubernetes `Job`.

It is intentionally small:

- `ExecuteTask` renders and applies a `batch/v1 Job` with one `task` container.
- `builtins/shell` runs `sh -c <with.command>`.
- Frozen custom `runtime=process` actions run `sh -c <runtime_config.command>`
  or `sh -c <runtime_config.entrypoint>`.
- Custom process inputs from `with` are exposed as `VECTIS_INPUT_*` variables,
  along with `VECTIS_ACTION_NAME`, `VECTIS_ACTION_VERSION`, and
  `VECTIS_ACTION_DIGEST`.
- Pod logs are forwarded through the worker shell log callback.
- Kubernetes `Complete` maps to `workercore.Success()`.
- Kubernetes `Failed` maps to `workercore.FailureWithReason("kubernetes.job_failed", ...)`.
- Lost or non-terminal state maps to `workercore.UnknownWithReason("kubernetes.job_unknown", ...)`.
- `CancelTask` is idempotent and deletes the derived Kubernetes Job.

Unsupported task shapes fail explicitly with `kubernetes.unsupported_task`.
This example does not translate full Vectis action graphs, dataflow-bound
inputs, artifacts, secret delivery, local-only actions, action source files, or
non-process custom runtimes into Kubernetes-native objects.

## Run It

Start the core:

```sh
go run ./examples/worker-core-kubernetes \
  --socket /tmp/vectis-worker-core-kubernetes.sock \
  --namespace vectis-work \
  --image busybox:1.36
```

Point a paired worker at that socket:

```sh
VECTIS_WORKER_CORE_SOCKET=/tmp/vectis-worker-core-kubernetes.sock \
vectis-worker
```

Useful flags:

| Flag | Default | Meaning |
| --- | --- | --- |
| `--socket` | `/tmp/vectis-worker-core-kubernetes.sock` | Worker-core Unix socket. |
| `--namespace` | `$KUBERNETES_NAMESPACE` or `default` | Namespace for task Jobs. |
| `--image` | `$VECTIS_KUBERNETES_WORKER_CORE_IMAGE` or `busybox:1.36` | Shell task image. |
| `--kubectl` | `$KUBECTL` or `kubectl` | `kubectl` binary. |
| `--wait-timeout` | `30m` | Maximum wait for a task Job to become terminal. |
| `--poll-interval` | `1s` | Job status poll interval. |
| `--delete-after` | `false` | Delete terminal task Jobs after completion. |

## RBAC

The process running this core needs Kubernetes credentials for the target
namespace with permission to:

- create, patch, get, list, watch, and delete `jobs.batch`;
- get, list, and watch `pods`;
- get pod logs.

When running as a sidecar next to `vectis-worker`, mount a kubeconfig or give the
pod a service account with the narrow namespace-scoped permissions above.

## Contract Notes

The Kubernetes Job name is derived from `run_id`, `task_key`, and `session_id`.
That makes `CancelTask` deterministic and allows a repeated execute request for
the same session to apply the same external object. A force-requeue produces a
new execution session and therefore a distinct Kubernetes Job.
