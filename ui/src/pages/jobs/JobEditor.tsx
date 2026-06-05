import type { FormEvent } from "react";
import { useEffect, useState } from "react";
import { Button, FormError, FormField, TextAreaField, ToggleField } from "../../components";
import type { Job, JobStatus, NewJob, UpdateJob } from "../../domain/console";
import { defaultJobDefinition } from "../../domain/consoleOptions";
import { cronSpec, jsonObject, required } from "../../validation/FormValidation";
import { ResourceTitle } from "../shared";
import { JobSourceOptions } from "./JobSourceOptions";
import { cronSpecFromSchedule, scheduleMode, schedulePresetSpec, JobTriggerControls } from "./JobTriggerControls";
import styles from "./JobEditor.module.css";

export type JobEditorMode = { kind: "create" } | { kind: "edit"; jobID: string };

export type JobFormValues = {
  branch: string;
  cronSpec: string;
  definition: string;
  manualEnabled: boolean;
  name: string;
  repository: string;
  schedule: string;
  status: JobStatus;
};

type JobEditorProps = {
  error: string;
  mode: JobEditorMode;
  onCancel: () => void;
  onCreateJob: (input: NewJob) => void;
  onError: (message: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => void;
  namespacePath: string;
  values: JobFormValues;
  onValuesChange: (values: JobFormValues) => void;
};

type JobEditorFieldErrors = Partial<Record<"cronSpec" | "definition" | "name", string>>;

export const emptyJobForm: JobFormValues = {
  branch: "main",
  cronSpec: "",
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
    definition: job.definition ?? defaultJobDefinition,
    cronSpec: cronSpecFromSchedule(job.schedule),
    manualEnabled: job.triggers.some((trigger) => trigger.kind === "manual"),
    name: job.name,
    repository: job.repository,
    schedule: job.schedule === "Manual" ? "None" : scheduleMode(job.schedule),
    status: job.status
  };
}

export function jobInputFromValues(values: JobFormValues) {
  const schedule = values.schedule === "Custom" ? `Cron: ${values.cronSpec}` : values.schedule;
  return {
    branch: values.branch,
    definition: values.definition,
    manualEnabled: values.manualEnabled,
    name: values.name,
    repository: values.repository,
    schedule: schedule === "None" && values.manualEnabled ? "Manual" : schedule,
    status: values.status
  };
}

export function JobEditor({
  error,
  mode,
  namespacePath,
  onCancel,
  onCreateJob,
  onError,
  onUpdateJob,
  onValuesChange,
  values
}: JobEditorProps) {
  const [fieldErrors, setFieldErrors] = useState<JobEditorFieldErrors>({});
  const [cronSpecEdited, setCronSpecEdited] = useState(false);

  function setValues(nextValues: JobFormValues) {
    onValuesChange(nextValues);
  }

  function clearFieldError(field: keyof JobEditorFieldErrors) {
    setFieldErrors((currentErrors) => {
      const nextErrors = { ...currentErrors };
      delete nextErrors[field];
      return nextErrors;
    });
  }

  useEffect(() => {
    if (!cronSpecEdited || values.schedule !== "Custom") {
      return;
    }

    const validationTimer = window.setTimeout(() => {
      const result = cronSpec(values.cronSpec);
      setFieldErrors((currentErrors) => {
        const nextErrors = { ...currentErrors };
        if (result.valid) {
          delete nextErrors.cronSpec;
        } else {
          nextErrors.cronSpec = result.message;
        }
        return nextErrors;
      });
    }, 350);

    return () => window.clearTimeout(validationTimer);
  }, [cronSpecEdited, values.cronSpec, values.schedule]);

  function submitJob(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onError("");
    const nextFieldErrors = validateJobForm(values);

    if (Object.keys(nextFieldErrors).length > 0) {
      setFieldErrors(nextFieldErrors);
      return;
    }

    setFieldErrors({});

    if (mode.kind === "edit") {
      onUpdateJob(mode.jobID, jobInputFromValues(values));
    } else {
      onCreateJob({
        ...jobInputFromValues(values),
        namespacePath
      });
    }

    onCancel();
  }

  return (
    <section className={`${styles.editorPanel} resource-editor-panel`} aria-labelledby="job-editor-title">
      <div className={styles.editorHeader}>
        <ResourceTitle id="job-editor-title" title={mode.kind === "create" ? "Create" : "Configure"} />
        <Button onClick={onCancel}>Cancel</Button>
      </div>
      <form className={`${styles.editorForm} resource-editor-form`} onSubmit={submitJob}>
        <div className={styles.editorLayout}>
          <section className={styles.editorSection} aria-labelledby="job-basics-title">
            <div className={styles.sectionIntro}>
              <h3 id="job-basics-title">Identity</h3>
            </div>
            <div className={styles.fieldGrid}>
              <FormField
                disabled={mode.kind === "edit"}
                error={fieldErrors.name}
                label="Name"
                name="jobName"
                onChange={(event) => {
                  clearFieldError("name");
                  setValues({ ...values, name: event.target.value });
                }}
                placeholder="worker-image"
                required
                value={values.name}
                wide
              />
              <ToggleField
                checked={values.status === "enabled"}
                label="State"
                name="jobEnabled"
                offText="Paused"
                onChange={(checked) =>
                  setValues({
                    ...values,
                    status: checked ? "enabled" : "paused"
                  })
                }
                onText="Enabled"
              />
            </div>
          </section>
          <section className={styles.editorSection} aria-labelledby="job-source-title">
            <div className={styles.sectionIntro}>
              <h3 id="job-source-title">Source</h3>
            </div>
            <JobSourceOptions />
          </section>
          <section className={styles.editorSection} aria-labelledby="job-triggers-title">
            <div className={styles.sectionIntro}>
              <h3 id="job-triggers-title">Triggers</h3>
            </div>
            <JobTriggerControls
              cronSpec={values.cronSpec}
              cronSpecError={fieldErrors.cronSpec}
              manualEnabled={values.manualEnabled}
              onCronSpecChange={(nextCronSpec) => {
                setCronSpecEdited(true);
                clearFieldError("cronSpec");
                setValues({ ...values, cronSpec: nextCronSpec });
              }}
              onManualChange={(manualEnabled) => setValues({ ...values, manualEnabled })}
              onScheduleChange={(schedule) => {
                setCronSpecEdited(false);
                clearFieldError("cronSpec");
                setValues({
                  ...values,
                  cronSpec: schedulePresetSpec(schedule) ?? values.cronSpec,
                  schedule
                });
              }}
              schedule={values.schedule}
            />
          </section>
          <section className={styles.editorSection} aria-labelledby="job-definition-title">
            <div className={styles.sectionIntro}>
              <h3 id="job-definition-title">Definition</h3>
            </div>
            <TextAreaField
              code
              error={fieldErrors.definition}
              label="JSON"
              name="jobDefinition"
              onChange={(event) => {
                clearFieldError("definition");
                setValues({ ...values, definition: event.target.value });
              }}
              required
              rows={10}
              value={values.definition}
              wide
            />
          </section>
        </div>
        <FormError message={error} />
        <div className="resource-editor-form__actions">
          <Button type="submit">{mode.kind === "create" ? "Create" : "Save"}</Button>
          <Button onClick={onCancel}>Cancel</Button>
        </div>
      </form>
    </section>
  );
}

function validateJobForm(values: JobFormValues) {
  const errors: JobEditorFieldErrors = {};
  const nameResult = required(values.name, "Enter a job name.");
  const definitionResult = jsonObject(values.definition);
  const cronResult = values.schedule === "Custom" ? cronSpec(values.cronSpec) : { valid: true as const };

  if (!nameResult.valid) {
    errors.name = nameResult.message;
  }

  if (!definitionResult.valid) {
    errors.definition = definitionResult.message;
  }

  if (!cronResult.valid) {
    errors.cronSpec = cronResult.message;
  }

  return errors;
}
