import type { RunListItem } from "../components";
import { isTerminalRunStatus, runRootAttemptNumber } from "../components/data/RunPresentation";

const refreshInterval = 2000;
const maxRetryInterval = 30000;

export function watchRun(
  load: () => Promise<RunListItem>,
  onRun: (run: RunListItem) => void,
  onError: (error: unknown) => boolean,
  initialRun?: RunListItem
) {
  let active = true;
  let timer: ReturnType<typeof setTimeout> | undefined;
  let retryInterval = refreshInterval;
  let latestRun = initialRun;

  async function refresh() {
    try {
      const snapshot = await load();
      if (!active) return;

      const run = retainRunTasks(latestRun, snapshot);
      latestRun = run;

      onRun(run);
      retryInterval = refreshInterval;

      const tasksSettled = run.tasks?.every(
        (task) =>
          !["pending", "accepted", "running"].includes(task.status) &&
          task.attempts.every((attempt) => !["pending", "accepted", "running"].includes(attempt.status))
      );

      if (isTerminalRunStatus(run.status) && tasksSettled && !run.tasksUpdating && run.tasks?.length) return;
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

export function retainRunTasks(previous: RunListItem | undefined, incoming: RunListItem): RunListItem {
  if (!previous || previous.id !== incoming.id || !previous.tasks?.length) return incoming;

  const previousAttempt = runRootAttemptNumber(previous);
  const incomingAttempt = runRootAttemptNumber(incoming);

  if (incomingAttempt > previousAttempt) return incoming;
  if (incomingAttempt < previousAttempt && incomingAttempt > 0) {
    return { ...incoming, tasks: previous.tasks, tasksUpdating: true };
  }

  const updates = new Map(incoming.tasks?.map((task) => [task.taskID, task]));
  if (previous.tasks.every((task) => updates.has(task.taskID))) return incoming;

  const knownIDs = new Set(previous.tasks.map((task) => task.taskID));
  return {
    ...incoming,
    tasks: [
      ...previous.tasks.map((task) => updates.get(task.taskID) ?? task),
      ...(incoming.tasks ?? []).filter((task) => !knownIDs.has(task.taskID))
    ],
    tasksUpdating: true
  };
}
