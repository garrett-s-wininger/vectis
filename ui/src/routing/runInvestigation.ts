import type { RunAttemptSelection } from "../pages/RunAttemptPresentation";

export type RunInvestigationContext = {
  /** Task key within the run; older links may contain a full task ID. */
  task?: string;
  attempt: RunAttemptSelection;
};

export function runInvestigationFromSearch(search: string): RunInvestigationContext {
  const params = new URLSearchParams(search);
  const value = params.get("attempt") ?? "";
  const attempt = /^[1-9]\d*$/.test(value) ? Number(value) : NaN;
  return {
    task: params.get("task") || undefined,
    attempt: Number.isSafeInteger(attempt) ? attempt : "latest"
  };
}

export function runInvestigationPath(pathname: string, search: string, hash: string, context: RunInvestigationContext) {
  const params = new URLSearchParams(search);
  params.delete("task");
  params.delete("attempt");

  if (context.task) params.set("task", context.task);
  if (typeof context.attempt === "number" && Number.isSafeInteger(context.attempt) && context.attempt > 0) {
    params.set("attempt", String(context.attempt));
  }

  const query = params.toString();
  return `${pathname}${query ? `?${query}` : ""}${hash}`;
}
