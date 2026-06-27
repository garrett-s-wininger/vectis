import type { Job, JobStatus, UpdateJob } from "../../domain/console";
import { defaultJobDefinition } from "../../domain/consoleOptions";
import { cronSpecFromSchedule, scheduleMode } from "./JobSchedule";

export type JobEditorMode = { kind: "create" } | { kind: "edit"; jobID: string };

export type JobFormValues = {
  branch: string;
  cronSpec: string;
  description: string;
  definition: string;
  manualEnabled: boolean;
  name: string;
  repository: string;
  schedule: string;
  status: JobStatus;
};

export const emptyJobForm: JobFormValues = {
  branch: "main",
  cronSpec: "",
  description: "",
  definition: defaultJobDefinition,
  manualEnabled: true,
  name: "",
  repository: "",
  schedule: "None",
  status: "enabled"
};

export function valuesFromJob(job: Job): JobFormValues {
  return {
    branch: job.branch,
    description: job.description ?? "",
    definition: job.definition ?? defaultJobDefinition,
    cronSpec: cronSpecFromSchedule(job.schedule),
    manualEnabled: job.triggers.some((trigger) => trigger.kind === "manual"),
    name: job.name,
    repository: job.repository,
    schedule: job.schedule === "Manual" ? "None" : scheduleMode(job.schedule),
    status: job.status
  };
}

export function jobInputFromValues(values: JobFormValues): UpdateJob {
  const schedule = values.schedule === "Custom" ? `Cron: ${values.cronSpec}` : values.schedule;

  return {
    branch: values.branch,
    description: values.description.trim() || undefined,
    definition: values.definition,
    manualEnabled: values.manualEnabled,
    name: values.name,
    repository: values.repository,
    schedule: schedule === "None" && values.manualEnabled ? "Manual" : schedule,
    status: values.status
  };
}
