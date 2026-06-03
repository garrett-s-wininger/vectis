import type { FormEvent } from "react";
import { Button, FormError, FormField, TextAreaField, ToggleField } from "../../components";
import type { Job, JobStatus, NewJob, UpdateJob } from "../../domain/console";
import { defaultJobDefinition } from "../../domain/consoleOptions";
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
  function setValues(nextValues: JobFormValues) {
    onValuesChange(nextValues);
  }

  function submitJob(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onError("");

    try {
      JSON.parse(values.definition);
    } catch {
      onError("Definition must be valid JSON.");
      return;
    }

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
                label="Name"
                name="jobName"
                onChange={(event) => setValues({ ...values, name: event.target.value })}
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
              manualEnabled={values.manualEnabled}
              onCronSpecChange={(cronSpec) => setValues({ ...values, cronSpec })}
              onManualChange={(manualEnabled) => setValues({ ...values, manualEnabled })}
              onScheduleChange={(schedule) =>
                setValues({
                  ...values,
                  cronSpec: schedulePresetSpec(schedule) ?? values.cronSpec,
                  schedule
                })
              }
              schedule={values.schedule}
            />
          </section>
          <section className={styles.editorSection} aria-labelledby="job-definition-title">
            <div className={styles.sectionIntro}>
              <h3 id="job-definition-title">Definition</h3>
            </div>
            <TextAreaField
              code
              label="JSON"
              name="jobDefinition"
              onChange={(event) => setValues({ ...values, definition: event.target.value })}
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
