import { useState } from "react";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { NamespacePicker } from "../components";
import { PageHeader } from "../components";
import type { RunListItem } from "../components";
import { StatusBadge } from "../components";
import type { Job, Namespace, NewJob, UpdateJob } from "../domain/console";
import { ResourceStatus } from "./shared";
import { JobActionPanel } from "./jobs/JobActionPanel";
import { JobDetailsDrawer } from "./jobs/JobDetailsDrawer";
import { emptyJobForm, JobEditor, type JobEditorMode, type JobFormValues, valuesFromJob } from "./jobs/JobEditor";
import { JobIdentity } from "./jobs/JobIdentity";
import styles from "./jobs/JobsPage.module.css";
import { JobTriggers } from "./jobs/JobTriggers";
import { getLatestRunForJob } from "./jobs/jobPresentation";

type ActiveJobEditorMode = JobEditorMode | null;

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
  const [editorMode, setEditorMode] = useState<ActiveJobEditorMode>(null);
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
    setValues(valuesFromJob(job));
    setFormError("");
  }

  function toggleSelectedJob(jobID: string) {
    setSelectedJobID((currentJobID) => (currentJobID === jobID ? "" : jobID));
  }

  function closeEditor() {
    setEditorMode(null);
    setFormError("");
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
      cell: (job) => {
        const lastRun = getLatestRunForJob(job, runs);

        return lastRun ? <StatusBadge status={lastRun.status} /> : <StatusBadge status="empty" />;
      }
    }
  ];

  return (
    <>
      <PageHeader
        description={`Stored definitions and triggers for ${namespacePath === "/" ? "/ root" : namespacePath}.`}
        eyebrow="Jobs"
        actions={
          !editorMode && jobs.length > 0 ? (
            <>
              <NamespacePicker compact namespaces={namespaces} onChange={onSelectNamespace} value={namespacePath} />
              <Button aria-expanded={false} onClick={startCreateJob}>
                Create
              </Button>
            </>
          ) : null
        }
        title="Jobs"
      />
      {editorMode ? (
        <JobEditor
          error={formError}
          mode={editorMode}
          namespacePath={namespacePath}
          onCancel={closeEditor}
          onCreateJob={onCreateJob}
          onError={setFormError}
          onUpdateJob={onUpdateJob}
          onValuesChange={setValues}
          values={values}
        />
      ) : null}
      {jobs.length === 0 && !editorMode ? (
        <section className={styles.emptyState} aria-labelledby="jobs-empty-title">
          <div>
            <p className="eyebrow">No stored jobs</p>
            <h2 id="jobs-empty-title">Create One Today</h2>
            <p>
              Stored jobs are reusable definitions you can trigger manually now and connect to richer sources later.
            </p>
          </div>
          <Button onClick={startCreateJob}>Create</Button>
        </section>
      ) : null}
      {jobs.length > 0 ? (
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
      ) : null}
    </>
  );
}
