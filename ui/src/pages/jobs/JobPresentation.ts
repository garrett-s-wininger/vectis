import type { RunListItem } from "../../components";
import type { Job } from "../../domain/console";

export const jobConfigurationActionLabel = "Edit";

export const triggerKindLabel: Record<Job["triggers"][number]["kind"], string> = {
  manual: "Manual",
  poll: "Poll",
  schedule: "Schedule",
  webhook: "Webhook"
};

export function getJobDescription(job: Job) {
  if (job.description) {
    return job.description;
  }

  return job.sourceKind === "db"
    ? "Reusable job definition stored in Vectis."
    : "Reusable job definition read from source control.";
}

export function getJobSourceDetail(job: Job) {
  return job.sourceKind === "db" ? "database" : `${job.repository} (${job.branch})`;
}

export function getJobPathDetail(job: Job) {
  return job.sourceKind === "db" ? "Stored in Vectis" : job.sourceDetail;
}

export function canManuallyRunJob(job: Job) {
  return job.triggers.some((trigger) => trigger.kind === "manual");
}

export function canTriggerRun(job: Job) {
  return canManuallyRunJob(job) && job.status === "enabled";
}

function getJobSourceFact(job: Job) {
  return job.sourceKind === "db"
    ? { label: "Definition", value: "Stored in Vectis" }
    : { label: "Path", value: job.sourceDetail };
}

export function getJobActionFacts(job: Job) {
  const facts = [getJobSourceFact(job)];
  const scheduledTrigger = job.triggers.find((trigger) => trigger.kind === "schedule");

  if (scheduledTrigger) {
    facts.push({ label: "Schedule", value: scheduledTrigger.detail });
    facts.push({ label: "Next run", value: job.nextRun });
  }

  return facts;
}

export function getLatestRunForJob(job: Job, runs: RunListItem[]) {
  return getRunsForJob(job, runs)[0];
}

export function getRunsForJob(job: Job, runs: RunListItem[]) {
  return runs
    .filter((run) => run.jobName === job.name && run.namespacePath === job.namespacePath)
    .sort((left, right) => right.runNumber - left.runNumber);
}

export function getRunHealthSummary(runs: RunListItem[]) {
  const recentRuns = runs.slice(0, 5);
  const failedRuns = recentRuns.filter((run) => run.status === "failed").length;
  const activeRuns = recentRuns.filter((run) => run.status === "queued" || run.status === "running").length;
  const succeededRuns = recentRuns.filter((run) => run.status === "succeeded").length;

  if (recentRuns.length === 0) {
    return {
      detail: "No run history yet.",
      label: "No runs",
      tone: "empty" as const
    };
  }

  if (failedRuns === recentRuns.length) {
    return {
      detail: failedRuns === 1 ? "The latest run failed." : `The last ${failedRuns} runs failed.`,
      label: "All failing",
      tone: "failed" as const
    };
  }

  if (failedRuns > 0) {
    return {
      detail:
        activeRuns > 0
          ? `Recent runs are mixed, with ${failedRuns} failed and ${activeRuns} still active.`
          : `Recent runs are mixed, with ${failedRuns} failed and ${succeededRuns} successful.`,
      label: "Mixed",
      tone: "queued" as const
    };
  }

  if (activeRuns > 0) {
    return {
      detail:
        succeededRuns > 0
          ? `${activeRuns} active now; recent completed runs passed.`
          : `${activeRuns} ${activeRuns === 1 ? "run is" : "runs are"} active now.`,
      label: "In progress",
      tone: "running" as const
    };
  }

  return {
    detail:
      recentRuns.length === 1
        ? "The latest run completed successfully."
        : `The last ${recentRuns.length} runs completed successfully.`,
    label: "Passing",
    tone: "succeeded" as const
  };
}

export function getManualTriggerDetail(job: Job) {
  if (!canManuallyRunJob(job)) {
    return "Manual runs are not enabled for this job.";
  }

  return job.status === "enabled" ? "Ready to run on demand." : "Enable the job before starting a manual run.";
}

export function getScheduleDetail(job: Job) {
  const scheduledTrigger = job.triggers.find((trigger) => trigger.kind === "schedule");

  if (!scheduledTrigger) {
    return "No scheduled trigger is configured.";
  }

  return `${scheduledTrigger.detail}; next trigger ${job.nextRun}.`;
}
