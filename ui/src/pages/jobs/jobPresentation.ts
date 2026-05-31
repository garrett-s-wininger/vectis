import type { RunListItem } from "../../components";
import type { Job } from "../../domain/console";

export const jobConfigurationActionLabel = "Config";

export const triggerKindLabel: Record<Job["triggers"][number]["kind"], string> = {
  manual: "Manual",
  poll: "Poll",
  schedule: "Schedule",
  webhook: "Webhook"
};

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
  return runs
    .filter((run) => run.jobName === job.name && run.namespacePath === job.namespacePath)
    .sort((left, right) => right.runNumber - left.runNumber)[0];
}
