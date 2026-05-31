import type { FormEvent } from "react";
import { useState } from "react";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { FormError } from "../components";
import { FormField } from "../components";
import { NamespacePicker } from "../components";
import { PageHeader } from "../components";
import type { RunListItem } from "../components";
import { SelectField } from "../components";
import { StatusBadge } from "../components";
import { TextAreaField } from "../components";
import type { Job, JobStatus, Namespace, NewJob, UpdateJob } from "../domain/console";
import { defaultJobDefinition, jobScheduleOptions, jobStatusOptions } from "../domain/consoleOptions";
import { ResourceStatus, ResourceTitle } from "./shared";
import { JobActionPanel } from "./jobs/JobActionPanel";
import { JobDetailsDrawer } from "./jobs/JobDetailsDrawer";
import { JobIdentity } from "./jobs/JobIdentity";
import styles from "./jobs/JobsPage.module.css";
import { JobTriggers } from "./jobs/JobTriggers";
import { getLatestRunForJob } from "./jobs/jobPresentation";

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
  onSelectRun: (runID: string) => void;
  onSelectNamespace: (namespacePath: string) => void;
  onTriggerRun: (jobID: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => void;
  runs: RunListItem[];
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
  onSelectRun,
  onSelectNamespace,
  onTriggerRun,
  onUpdateJob,
  runs
}: JobsPageProps) {
  const [editorMode, setEditorMode] = useState<JobEditorMode>(null);
  const [selectedJobID, setSelectedJobID] = useState("");
  const [values, setValues] = useState<JobFormValues>(emptyJobForm);
  const [formError, setFormError] = useState("");
  const selectedJob = jobs.find((job) => job.id === selectedJobID);
  const selectedJobLastRun = selectedJob ? getLatestRunForJob(selectedJob, runs) : undefined;

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

  function toggleSelectedJob(jobID: string) {
    setSelectedJobID((currentJobID) => (currentJobID === jobID ? "" : jobID));
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
      width: "34%",
      cell: (job) => (
        <JobIdentity job={job} onSelect={() => toggleSelectedJob(job.id)} selected={selectedJob?.id === job.id} />
      )
    },
    {
      header: "Triggers",
      hideOnMobile: true,
      width: "34%",
      cell: (job) => <JobTriggers job={job} />
    },
    {
      header: "Details",
      mobileOnly: true,
      cell: (job) =>
        selectedJob?.id === job.id ? (
          <JobDetailsDrawer
            job={job}
            lastRun={getLatestRunForJob(job, runs)}
            onEdit={() => startEditJob(job)}
            onOpenLastRun={() => {
              const lastRun = getLatestRunForJob(job, runs);
              if (lastRun) {
                onSelectRun(lastRun.id);
              }
            }}
            onTrigger={() => onTriggerRun(job.id)}
          />
        ) : null
    },
    {
      align: "end",
      header: "State",
      hideOnMobile: true,
      width: "96px",
      cell: (job) => (
        <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>
      )
    },
    {
      align: "end",
      header: "Latest run",
      hideOnMobile: true,
      width: "96px",
      cell: (job) => <StatusBadge status={job.lastRunStatus} />
    }
  ];

  return (
    <>
      <PageHeader
        description="Definitions, source of truth, and triggers."
        eyebrow="Jobs"
        actions={
          <>
            <NamespacePicker compact namespaces={namespaces} onChange={onSelectNamespace} value={namespacePath} />
            <Button aria-expanded={editorMode?.kind === "create"} onClick={startCreateJob}>
              New
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
      <div className={selectedJob ? `${styles.workspace} ${styles.workspaceWithPanel}` : styles.workspace}>
        <DataTable
          columns={columns}
          emptyMessage="No jobs loaded."
          getRowKey={(job) => job.id}
          isRowSelected={(job) => selectedJob?.id === job.id}
          rows={jobs}
        />
        {selectedJob ? (
          <JobActionPanel
            job={selectedJob}
            lastRun={selectedJobLastRun}
            onEdit={() => startEditJob(selectedJob)}
            onOpenLastRun={() => {
              if (selectedJobLastRun) {
                onSelectRun(selectedJobLastRun.id);
              }
            }}
            onTrigger={() => onTriggerRun(selectedJob.id)}
          />
        ) : null}
      </div>
    </>
  );
}
