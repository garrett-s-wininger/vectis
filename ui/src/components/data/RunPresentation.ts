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

export function runDisplayName(run: { id?: string; jobName: string; source?: "stored" | "ephemeral" }) {
  if (run.source === "ephemeral" && isGeneratedID(run.jobName)) {
    return run.id ? shortRunID(run.id) : "Inline";
  }

  return run.jobName;
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

function isGeneratedID(value: string) {
  return /^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/i.test(value);
}

function shortRunID(runID: string) {
  const uuid = runID.match(/[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-([89ab][0-9a-f]{3})-([0-9a-f]{12})/i);
  if (uuid) {
    return `${uuid[1]}-${uuid[2]}`;
  }

  return runID.slice(-17);
}
