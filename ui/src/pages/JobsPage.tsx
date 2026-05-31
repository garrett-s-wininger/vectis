import type { FormEvent } from "react";
import { useState } from "react";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { FormError } from "../components";
import { FormField } from "../components";
import { NamespacePicker } from "../components";
import { PageHeader } from "../components";
import { SelectField } from "../components";
import { StatusBadge } from "../components";
import { TextAreaField } from "../components";
import type { Job, JobStatus, Namespace, NewJob, UpdateJob } from "../domain/console";
import { defaultJobDefinition, jobScheduleOptions, jobStatusOptions } from "../domain/consoleOptions";
import { ResourceStatus, ResourceTitle, TableActions } from "./shared";

type JobEditorMode = { kind: "create" } | { kind: "edit"; jobID: string } | null;

type JobFormValues = {
  branch: string;
  definition: string;
  name: string;
  repository: string;
  schedule: string;
  status: JobStatus;
};

type JobsPageProps = {
  jobs: Job[];
  namespaces: Namespace[];
  namespacePath: string;
  onCreateJob: (input: NewJob) => void;
  onDeleteJob: (jobID: string) => void;
  onSelectNamespace: (namespacePath: string) => void;
  onTriggerRun: (jobID: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => void;
};

const emptyJobForm: JobFormValues = {
  branch: "main",
  definition: defaultJobDefinition,
  name: "",
  repository: "",
  schedule: "Manual",
  status: "enabled"
};

export function JobsPage({
  jobs,
  namespaces,
  namespacePath,
  onCreateJob,
  onDeleteJob,
  onSelectNamespace,
  onTriggerRun,
  onUpdateJob
}: JobsPageProps) {
  const [editorMode, setEditorMode] = useState<JobEditorMode>(null);
  const [values, setValues] = useState<JobFormValues>(emptyJobForm);
  const [formError, setFormError] = useState("");

  function startCreateJob() {
    setEditorMode({ kind: "create" });
    setValues(emptyJobForm);
    setFormError("");
  }

  function startEditJob(job: Job) {
    setEditorMode({ kind: "edit", jobID: job.id });
    setValues({
      branch: job.branch,
      definition: job.definition ?? defaultJobDefinition,
      name: job.name,
      repository: job.repository,
      schedule: job.schedule,
      status: job.status
    });
    setFormError("");
  }

  function closeEditor() {
    setEditorMode(null);
    setFormError("");
  }

  function submitJob(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setFormError("");

    try {
      JSON.parse(values.definition);
    } catch {
      setFormError("Definition must be valid JSON.");
      return;
    }

    if (editorMode?.kind === "edit") {
      onUpdateJob(editorMode.jobID, values);
    } else {
      onCreateJob({ ...values, namespacePath });
    }

    closeEditor();
  }

  const columns: DataTableColumn<Job>[] = [
    {
      header: "Job",
      cell: (job) => <ResourceTitle subtitle={job.repository} title={job.name} />
    },
    {
      header: "Namespace",
      cell: (job) => job.namespacePath
    },
    {
      header: "Branch",
      cell: (job) => job.branch
    },
    {
      header: "Schedule",
      cell: (job) => <ResourceTitle subtitle={job.nextRun} title={job.schedule} />
    },
    {
      header: "Last run",
      cell: (job) => <StatusBadge status={job.lastRunStatus} />
    },
    {
      align: "end",
      header: "State",
      cell: (job) => (
        <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (job) => (
        <TableActions>
          <Button
            aria-label={`Trigger ${job.name}`}
            disabled={job.status === "paused"}
            onClick={() => onTriggerRun(job.id)}
          >
            Trigger
          </Button>
          <Button aria-label={`Edit ${job.name}`} onClick={() => startEditJob(job)}>
            Edit
          </Button>
          <Button aria-label={`Delete ${job.name}`} onClick={() => onDeleteJob(job.id)}>
            Delete
          </Button>
        </TableActions>
      )
    }
  ];

  return (
    <>
      <PageHeader
        description={`Configured job definitions under ${namespacePath}.`}
        eyebrow="Jobs"
        actions={
          <>
            <NamespacePicker compact namespaces={namespaces} onChange={onSelectNamespace} value={namespacePath} />
            <Button aria-expanded={editorMode?.kind === "create"} onClick={startCreateJob}>
              New job
            </Button>
          </>
        }
        title="Jobs"
      />
      {editorMode ? (
        <section className="resource-editor-panel" aria-labelledby="job-editor-title">
          <ResourceTitle
            id="job-editor-title"
            subtitle={`Namespace ${namespacePath}`}
            title={editorMode.kind === "create" ? "New job" : "Edit job"}
          />
          <form className="resource-editor-form" onSubmit={submitJob}>
            <div className="resource-editor-form__grid">
              <FormField
                label="Name"
                name="jobName"
                onChange={(event) => setValues({ ...values, name: event.target.value })}
                required
                value={values.name}
              />
              <FormField
                label="Repository"
                name="jobRepository"
                onChange={(event) => setValues({ ...values, repository: event.target.value })}
                required
                value={values.repository}
              />
              <FormField
                label="Branch"
                name="jobBranch"
                onChange={(event) => setValues({ ...values, branch: event.target.value })}
                required
                value={values.branch}
              />
              <SelectField
                label="Schedule"
                name="jobSchedule"
                onChange={(event) => setValues({ ...values, schedule: event.target.value })}
                options={jobScheduleOptions}
                value={values.schedule}
              />
              <SelectField
                label="State"
                name="jobState"
                onChange={(event) =>
                  setValues({
                    ...values,
                    status: event.target.value as JobStatus
                  })
                }
                options={jobStatusOptions}
                value={values.status}
              />
            </div>
            <TextAreaField
              label="Definition JSON"
              name="jobDefinition"
              onChange={(event) => setValues({ ...values, definition: event.target.value })}
              required
              rows={10}
              value={values.definition}
              wide
            />
            <FormError message={formError} />
            <div className="resource-editor-form__actions">
              <Button type="submit">{editorMode.kind === "create" ? "Create job" : "Save job"}</Button>
              <Button onClick={closeEditor}>Cancel</Button>
            </div>
          </form>
        </section>
      ) : null}
      <DataTable columns={columns} emptyMessage="No jobs loaded." getRowKey={(job) => job.id} rows={jobs} />
    </>
  );
}
