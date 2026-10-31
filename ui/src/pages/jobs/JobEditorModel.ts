import type { Job, UpdateJob } from "../../domain/console";
import { defaultJobDefinition } from "../../domain/consoleOptions";

export type JobEditorMode = { kind: "create" } | { kind: "edit"; jobID: string };
export type JobFormValues = { branch: string; definition: string; name: string; repository: string };

export const emptyJobForm: JobFormValues = { branch: "", definition: defaultJobDefinition, name: "", repository: "" };

export function valuesFromJob(job: Job): JobFormValues {
  return { branch: job.branch, definition: job.definition ?? "", name: job.name, repository: job.repository };
}

export function jobInputFromValues(values: JobFormValues): UpdateJob {
  return { branch: values.branch, definition: values.definition, name: values.name, repository: values.repository };
}
