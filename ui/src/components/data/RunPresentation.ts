import type { RunStatus } from "../status/StatusBadge";

export type RunTrigger = "api" | "manual" | "poll" | "schedule" | "ui" | "webhook";

type RunPresentationInput = {
  submittedBy?: string;
  trigger?: RunTrigger;
};

export function runActorLabel(submittedBy: string) {
  const normalized = submittedBy.toLowerCase();

  if (normalized === "cron" || normalized === "system") {
    return "SYSTEM";
  }

  if (normalized === "api" || normalized === "anonymous") {
    return "Anonymous";
  }

  return submittedBy;
}

export function runCountLabel(count: number) {
  return count === 1 ? "1 Run" : `${count} Runs`;
}

export function runDurationLabel(status: RunStatus) {
  if (status === "queued") {
    return "Waiting";
  }

  if (status === "running") {
    return "Elapsed";
  }

  return "Duration";
}

export function runStatusClass(status: RunStatus) {
  return `status${status[0].toUpperCase()}${status.slice(1)}` as const;
}

export function runTriggerLabel(run: RunPresentationInput) {
  const trigger = run.trigger ?? inferredRunTrigger(run);

  switch (trigger) {
    case "api":
      return "API";
    case "poll":
      return "Poll";
    case "schedule":
      return "Schedule";
    case "ui":
      return "UI";
    case "webhook":
      return "Webhook";
    case "manual":
      return "Manual";
  }
}

function inferredRunTrigger(run: RunPresentationInput): RunTrigger {
  const submittedBy = run.submittedBy?.toLowerCase();

  if (submittedBy === "api") {
    return "api";
  }

  if (submittedBy === "cron" || submittedBy === "system") {
    return "schedule";
  }

  return "manual";
}
