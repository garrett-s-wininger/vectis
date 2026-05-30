import type { FormEvent } from "react";
import { useState } from "react";
import { Button } from "../components/Button";
import { DataTable, type DataTableColumn } from "../components/DataTable";
import { FormError } from "../components/FormError";
import { FormField } from "../components/FormField";
import { PageHeader } from "../components/PageHeader";
import { SelectField } from "../components/SelectField";
import { StatusBadge } from "../components/StatusBadge";
import fieldStyles from "../components/Field.module.css";
import type {
  MockJob,
  MockJobStatus,
  NewMockJob,
  UpdateMockJob
} from "../mocks/consoleData";

type JobEditorMode =
  | { kind: "create" }
  | { kind: "edit"; jobID: string }
  | null;

type JobFormValues = {
  branch: string;
  definition: string;
  name: string;
  repository: string;
  schedule: string;
  status: MockJobStatus;
};

type JobsPageProps = {
  jobs: MockJob[];
  namespacePath: string;
  onCreateJob: (input: NewMockJob) => void;
  onDeleteJob: (jobID: string) => void;
  onTriggerRun: (jobID: string) => void;
  onUpdateJob: (jobID: string, input: UpdateMockJob) => void;
};

const statusOptions = [
  { label: "Enabled", value: "enabled" },
  { label: "Paused", value: "paused" }
];

const scheduleOptions = [
  { label: "Manual", value: "Manual" },
  { label: "On push", value: "On push" },
  { label: "Hourly", value: "Hourly" },
  { label: "Nightly", value: "Nightly" }
];

const defaultJobDefinition = JSON.stringify(
  {
    id: "stored-job",
    root: {
      id: "root",
      uses: "builtins/shell",
      with: {
        command: "echo 'Hello from Vectis'"
      }
    }
  },
  null,
  2
);

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
  namespacePath,
  onCreateJob,
  onDeleteJob,
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

  function startEditJob(job: MockJob) {
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

  const columns: DataTableColumn<MockJob>[] = [
    {
      header: "Job",
      cell: (job) => (
        <div className="resource-title">
          <strong>{job.name}</strong>
          <small>{job.repository}</small>
        </div>
      )
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
      cell: (job) => (
        <div className="resource-title">
          <strong>{job.schedule}</strong>
          <small>{job.nextRun}</small>
        </div>
      )
    },
    {
      header: "Last run",
      cell: (job) => <StatusBadge status={job.lastRunStatus} />
    },
    {
      align: "end",
      header: "State",
      cell: (job) => (
        <span className={`resource-status resource-status--${job.status}`}>
          {job.status === "enabled" ? "Enabled" : "Paused"}
        </span>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (job) => (
        <div className="table-actions">
          <Button
            aria-label={`Trigger ${job.name}`}
            disabled={job.status === "paused"}
            onClick={() => onTriggerRun(job.id)}
          >
            Trigger
          </Button>
          <Button
            aria-label={`Edit ${job.name}`}
            onClick={() => startEditJob(job)}
          >
            Edit
          </Button>
          <Button
            aria-label={`Delete ${job.name}`}
            onClick={() => onDeleteJob(job.id)}
          >
            Delete
          </Button>
        </div>
      )
    }
  ];

  return (
    <>
      <PageHeader
        description={`Configured job definitions under ${namespacePath}.`}
        eyebrow="Jobs"
        actions={
          <Button
            aria-expanded={editorMode?.kind === "create"}
            onClick={startCreateJob}
          >
            New job
          </Button>
        }
        title="Jobs"
      />
      {editorMode ? (
        <section
          className="resource-editor-panel"
          aria-labelledby="job-editor-title"
        >
          <div className="resource-title">
            <strong id="job-editor-title">
              {editorMode.kind === "create" ? "New job" : "Edit job"}
            </strong>
            <small>Namespace {namespacePath}</small>
          </div>
          <form className="resource-editor-form" onSubmit={submitJob}>
            <div className="resource-editor-form__grid">
              <FormField
                label="Name"
                name="jobName"
                onChange={(event) =>
                  setValues({ ...values, name: event.target.value })
                }
                required
                value={values.name}
              />
              <FormField
                label="Repository"
                name="jobRepository"
                onChange={(event) =>
                  setValues({ ...values, repository: event.target.value })
                }
                required
                value={values.repository}
              />
              <FormField
                label="Branch"
                name="jobBranch"
                onChange={(event) =>
                  setValues({ ...values, branch: event.target.value })
                }
                required
                value={values.branch}
              />
              <SelectField
                label="Schedule"
                name="jobSchedule"
                onChange={(event) =>
                  setValues({ ...values, schedule: event.target.value })
                }
                options={scheduleOptions}
                value={values.schedule}
              />
              <SelectField
                label="State"
                name="jobState"
                onChange={(event) =>
                  setValues({
                    ...values,
                    status: event.target.value as MockJobStatus
                  })
                }
                options={statusOptions}
                value={values.status}
              />
            </div>
            <label className={`${fieldStyles.root} ${fieldStyles.wide}`}>
              <span>Definition JSON</span>
              <textarea
                name="jobDefinition"
                onChange={(event) =>
                  setValues({ ...values, definition: event.target.value })
                }
                required
                rows={10}
                value={values.definition}
              />
            </label>
            <FormError message={formError} />
            <div className="resource-editor-form__actions">
              <Button type="submit">
                {editorMode.kind === "create" ? "Create job" : "Save job"}
              </Button>
              <Button onClick={closeEditor}>Cancel</Button>
            </div>
          </form>
        </section>
      ) : null}
      <DataTable
        columns={columns}
        emptyMessage="No jobs loaded."
        getRowKey={(job) => job.id}
        rows={jobs}
      />
    </>
  );
}
