import type { RunListItem } from "../components";

export type RunAttemptSelection = "latest" | number;

export function runAttemptNumbers(run: RunListItem) {
  const root = run.tasks?.find((task) => task.taskKey === "root");
  const attempts = root?.attempts.length ? root.attempts : (run.tasks?.flatMap((task) => task.attempts) ?? []);
  return [
    ...new Set(
      attempts
        .filter((attempt) => Number.isSafeInteger(attempt.attempt) && attempt.attempt > 0)
        .map((attempt) => attempt.attempt)
    )
  ].sort((a, b) => b - a);
}

export function runForAttempt(run: RunListItem, attempt: number): RunListItem {
  const latest = runAttemptNumbers(run)[0] ?? 1;
  const tasks = run.tasks?.map((task) => {
    const matching = task.attempts.filter((item) => item.attempt === attempt);
    const attempts = matching.filter((item) => (item.executionStatus ?? item.status) !== "planned");
    return {
      ...task,
      attempts,
      status:
        attempt === latest && (matching.length > 0 || task.attempts.length === 0)
          ? task.status
          : (attempts[0]?.status ?? "planned")
    };
  });

  return { ...run, tasks };
}
