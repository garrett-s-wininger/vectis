import type { FormEvent, ReactNode } from "react";
import { useEffect, useState } from "react";
import { Button, FormError, FormField, TextAreaField, ToggleField } from "../../components";
import type { NewJob, UpdateJob } from "../../domain/console";
import { cronSpec, jsonObject, required } from "../../validation/FormValidation";
import { ResourceTitle } from "../shared";
import { jobInputFromValues, type JobEditorMode, type JobFormValues } from "./JobEditorModel";
import { JobSourceOptions } from "./JobSourceOptions";
import { schedulePresetSpec } from "./JobSchedule";
import { JobTriggerControls } from "./JobTriggerControls";
import styles from "./JobEditor.module.css";

type JobEditorProps = {
  error: string;
  mode: JobEditorMode;
  onCancel: () => void;
  onCreateJob: (input: NewJob) => Promise<void> | void;
  onError: (message: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => Promise<void> | void;
  namespacePath: string;
  values: JobFormValues;
  onValuesChange: (values: JobFormValues) => void;
};

type JobEditorFieldErrors = Partial<Record<"cronSpec" | "definition" | "name", string>>;

type EditorSectionProps = {
  children: ReactNode;
  description: string;
  title: string;
  titleID: string;
};

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
  const isConfigureMode = mode.kind === "edit";
  const editorTitle = "Definition";
  const editorSubtitle = "Basics, source, triggers, and JSON payload.";

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

  async function submitJob(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    onError("");
    const nextFieldErrors = validateJobForm(values);

    if (Object.keys(nextFieldErrors).length > 0) {
      setFieldErrors(nextFieldErrors);
      return;
    }

    setFieldErrors({});

    try {
      if (mode.kind === "edit") {
        await onUpdateJob(mode.jobID, jobInputFromValues(values));
      } else {
        await onCreateJob({
          ...jobInputFromValues(values),
          namespacePath
        });
      }
    } catch (error) {
      onError(error instanceof Error ? error.message : "Unable to save job.");
      return;
    }

    onCancel();
  }

  return (
    <section
      className={`${styles.editorPanel} polished-panel polished-panel--accent-top polished-panel--overlay-safe`}
      aria-labelledby="job-editor-title"
    >
      <div className={styles.editorHeader}>
        <ResourceTitle
          className={styles.editorTitle}
          id="job-editor-title"
          subtitle={editorSubtitle}
          title={editorTitle}
        />
        <Button className={styles.editorHeaderAction} onClick={onCancel} variant="quiet">
          Cancel
        </Button>
      </div>
      <form className={styles.editorForm} onSubmit={submitJob}>
        <div className={styles.editorLayout}>
          <EditorSection
            description="Display details and whether new runs can be submitted."
            title="Basics"
            titleID="job-basics-title"
          >
            <div className={styles.fieldGrid}>
              <FormField
                disabled={isConfigureMode}
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
              <FormField
                label="Description"
                name="jobDescription"
                onChange={(event) => setValues({ ...values, description: event.target.value })}
                placeholder="Coordinates a repeatable workflow"
                value={values.description}
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
          </EditorSection>
          <EditorSection
            description="Choose where Vectis treats this as authoritative."
            title="Source"
            titleID="job-source-title"
          >
            <JobSourceOptions />
          </EditorSection>
          <EditorSection
            description="Allow manual starts, scheduled starts, or both."
            title="Triggers"
            titleID="job-triggers-title"
          >
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
          </EditorSection>
          <EditorSection
            description="Payload Vectis stores and submits when this job starts."
            title="JSON"
            titleID="job-definition-title"
          >
            <TextAreaField
              code
              error={fieldErrors.definition}
              label="Payload"
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
          </EditorSection>
        </div>
        <FormError message={error} />
        <div className={styles.editorActions}>
          <Button type="submit">{mode.kind === "create" ? "Create" : "Save"}</Button>
          <Button onClick={onCancel} variant="quiet">
            Cancel
          </Button>
        </div>
      </form>
    </section>
  );
}

function EditorSection({ children, description, title, titleID }: EditorSectionProps) {
  return (
    <section className={styles.editorSection} aria-labelledby={titleID}>
      <div className={styles.sectionIntro}>
        <h3 id={titleID}>{title}</h3>
        <p>{description}</p>
      </div>
      {children}
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
