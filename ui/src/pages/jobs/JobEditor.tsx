import type { SourceRepository } from "../../domain/console";
import type { FormEvent, ReactNode } from "react";
import { useState } from "react";
import { Button, FormError, FormField, SelectField, TextAreaField } from "../../components";
import type { NewJob, UpdateJob } from "../../domain/console";
import { jsonObject, required } from "../../validation/FormValidation";
import { ResourceTitle } from "../shared";
import { jobInputFromValues, type JobEditorMode, type JobFormValues } from "./JobEditorModel";
import styles from "./JobEditor.module.css";

type JobEditorProps = {
  repositories?: SourceRepository[];
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

type JobEditorFieldErrors = Partial<Record<"definition" | "name" | "repository", string>>;

type EditorSectionProps = {
  children: ReactNode;
  description: string;
  title: string;
  titleID: string;
};

export function JobEditor({
  repositories = [],
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
  const [saving, setSaving] = useState(false);
  const [fieldErrors, setFieldErrors] = useState<JobEditorFieldErrors>({});
  const isConfigureMode = mode.kind === "edit";
  const editorTitle = "Definition";
  const editorSubtitle = "Repository and JSON definition.";

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

  async function submitJob(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (saving) return;
    onError("");
    const nextFieldErrors = validateJobForm(values);
    if (
      mode.kind === "create" &&
      values.repository &&
      !repositories.some((repository) => repository.id === values.repository && repository.canAuthor)
    )
      nextFieldErrors.repository = "Choose a registered repository with authoring enabled.";

    if (Object.keys(nextFieldErrors).length > 0) {
      setFieldErrors(nextFieldErrors);
      return;
    }

    setFieldErrors({});
    setSaving(true);

    try {
      if (mode.kind === "edit") {
        await onUpdateJob(mode.jobID, jobInputFromValues(values));
      } else {
        await onCreateJob({
          ...jobInputFromValues(values),
          namespacePath:
            repositories.find((repository) => repository.id === values.repository)?.namespacePath ?? namespacePath
        });
      }
    } catch (error) {
      onError(error instanceof Error ? error.message : "Unable to save job.");
      return;
    } finally {
      setSaving(false);
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
          <EditorSection description="Name the reusable source definition." title="Basics" titleID="job-basics-title">
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
            </div>
          </EditorSection>
          <EditorSection
            description="Save definitions as local commits in a registered repository with authoring enabled. Push commits separately to share them."
            title="Source"
            titleID="job-source-title"
          >
            <SelectField
              disabled={isConfigureMode}
              options={[
                { value: "", label: "Select a repository" },
                ...repositories.map((repository) => ({
                  value: repository.id,
                  label: `${repository.id} (${repository.namespacePath})${repository.canAuthor ? "" : " — authoring unavailable"}`,
                  disabled: !repository.canAuthor
                })),
                ...(isConfigureMode && !repositories.some((repository) => repository.id === values.repository)
                  ? [{ value: values.repository, label: values.repository }]
                  : [])
              ]}
              error={fieldErrors.repository}
              label="Repository ID"
              name="jobRepository"
              onChange={(event) => {
                clearFieldError("repository");
                setValues({ ...values, repository: event.target.value });
              }}
              required
              value={values.repository}
            />
            {mode.kind === "create" && !repositories.some((repository) => repository.canAuthor) ? (
              <p>
                No registered repositories support authoring. Configure a managed repository with local_commit
                authoring.
              </p>
            ) : null}
          </EditorSection>
          <EditorSection
            description="Definition committed to the repository and loaded when this job starts."
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
          <Button disabled={saving} type="submit">
            {mode.kind === "create" ? "Create" : "Save"}
          </Button>
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
  const repositoryResult = required(values.repository, "Enter a registered source repository ID.");

  if (!repositoryResult.valid) {
    errors.repository = repositoryResult.message;
  }

  if (!nameResult.valid) {
    errors.name = nameResult.message;
  }

  if (!definitionResult.valid) {
    errors.definition = definitionResult.message;
  }

  return errors;
}
