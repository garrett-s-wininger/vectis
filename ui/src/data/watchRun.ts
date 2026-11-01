import type { RunListItem } from "../components";

const refreshInterval = 2000;
const maxRetryInterval = 30000;

export function watchRun(
  load: () => Promise<RunListItem>,
  onRun: (run: RunListItem) => void,
  onError: (error: unknown) => boolean
) {
  let active = true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let retryInterval = refreshInterval;

  async function refresh() {
    try {
      const run = await load();
      if (!active) return;

      onRun(run);
      retryInterval = refreshInterval;

      const terminal = ["succeeded", "failed", "cancelled", "aborted", "abandoned"].includes(run.status);
      const tasksSettled = run.tasks?.every(
        (task) =>
          !["pending", "accepted", "running"].includes(task.status) &&
          task.attempts.every((attempt) => !["pending", "accepted", "running"].includes(attempt.status))
      );

      if (terminal && tasksSettled) return;
    } catch (error) {
      if (!active || !onError(error)) return;
      retryInterval = Math.min(retryInterval * 2, maxRetryInterval);
    }

    if (active) timer = setTimeout(() => void refresh(), retryInterval);
  }

  void refresh();

  return () => {
    active = false;
    clearTimeout(timer);
  };
}
