import { useState } from "react";
import { AppState } from "../components";
import { Button } from "../components";
import { BreadcrumbTrail } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { NamespacePicker } from "../components";
import { PageHeader } from "../components";
import type { RunListItem } from "../components";
import { StatusBadge } from "../components";
import type { Job, Namespace, NewJob, UpdateJob } from "../domain/console";
import { ResourceStatus } from "./shared";
import { JobActionPanel } from "./jobs/JobActionPanel";
import { JobDetailPage } from "./jobs/JobDetailPage";
import { JobDetailsDrawer } from "./jobs/JobDetailsDrawer";
import { emptyJobForm, JobEditor, type JobEditorMode, type JobFormValues, valuesFromJob } from "./jobs/JobEditor";
import { JobIdentity } from "./jobs/JobIdentity";
import styles from "./jobs/JobsPage.module.css";
import { JobTriggers } from "./jobs/JobTriggers";
import { jobEditorBreadcrumbItems, jobsIndexBreadcrumbItems } from "./jobs/JobBreadcrumbs";
import { getLatestRunForJob, getRunsForJob } from "./jobs/JobPresentation";

type ActiveJobEditorMode = JobEditorMode | null;

type JobsPageProps = {
  allJobs?: Job[];
  detailJobID?: string;
  editorMode?: ActiveJobEditorMode;
  jobs: Job[];
  namespaces: Namespace[];
  namespacePath: string;
  onCloseEditor: () => void;
  onCreateJob: (input: NewJob) => void;
  onOpenCreate: () => void;
  onOpenEditor: (jobID: string) => void;
  onOpenJob: (jobID: string) => void;
  onOpenJobRuns?: (jobName: string) => void;
  onSelectRun: (runID: string) => void;
  onSelectNamespace: (namespacePath: string) => void;
  onTriggerRun: (jobID: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => void;
  runs: RunListItem[];
};

export function JobsPage({
  allJobs,
  detailJobID,
  editorMode = null,
  jobs,
  namespaces,
  namespacePath,
  onCloseEditor,
  onCreateJob,
  onOpenCreate,
  onOpenEditor,
  onOpenJob,
  onOpenJobRuns,
  onSelectRun,
  onSelectNamespace,
  onTriggerRun,
  onUpdateJob,
  runs
}: JobsPageProps) {
  const routableJobs = allJobs ?? jobs;
  const [selectedJobID, setSelectedJobID] = useState("");
  const selectedJob = jobs.find((job) => job.id === selectedJobID);
  const detailJob = detailJobID ? routableJobs.find((job) => job.id === detailJobID) : null;
  const selectedJobLastRun = selectedJob ? getLatestRunForJob(selectedJob, runs) : undefined;
  const editorJob =
    editorMode?.kind === "edit" ? (routableJobs.find((candidate) => candidate.id === editorMode.jobID) ?? null) : null;

  const editorInitialValues = editorJob ? valuesFromJob(editorJob) : emptyJobForm;
  const editorKey = editorMode?.kind === "edit" ? `edit:${editorMode.jobID}` : (editorMode?.kind ?? "");
  const editorNamespacePath = editorJob?.namespacePath ?? namespacePath;
  const editorJobName = editorJob?.name ?? "Create";
  const pageTitle = editorMode ? (editorMode.kind === "edit" ? "Configure" : "Create") : "Jobs";
  const pageDescription = editorMode
    ? editorMode.kind === "edit"
      ? "Review and adjust the stored definition, state, and triggers."
      : "Define a reusable stored job and trigger policy."
    : "Stored definitions and triggers.";

  function startEditJob(job: Job) {
    onOpenEditor(job.id);
  }

  function toggleSelectedJob(jobID: string) {
    setSelectedJobID((currentJobID) => (currentJobID === jobID ? "" : jobID));
  }

  if (detailJobID && !detailJob) {
    return <AppState description={`Job ${detailJobID} was not found in ${namespacePath}.`} title="Job not found" />;
  }

  if (detailJob) {
    const latestRun = getLatestRunForJob(detailJob, runs);
    const jobRuns = getRunsForJob(detailJob, runs);

    return (
      <JobDetailPage
        job={detailJob}
        lastRun={latestRun}
        runs={jobRuns}
        onBack={() => onOpenJob("")}
        onConfigure={() => startEditJob(detailJob)}
        onOpenLastRun={() => {
          if (latestRun) {
            onSelectRun(latestRun.id);
          }
        }}
        onOpenRuns={() => onOpenJobRuns?.(detailJob.name)}
        onTrigger={() => onTriggerRun(detailJob.id)}
      />
    );
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
            onOpen={() => onOpenJob(job.id)}
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
        description={pageDescription}
        navigation={
          editorMode ? (
            <BreadcrumbTrail
              items={jobEditorBreadcrumbItems({
                editorJobName,
                mode: editorMode,
                namespacePath: editorNamespacePath,
                onJob: () => {
                  if (editorMode.kind === "edit") {
                    onOpenJob(editorMode.jobID);
                    return;
                  }

                  onCloseEditor();
                },
                onJobs: onCloseEditor
              })}
              label="Job editor location"
            />
          ) : (
            <BreadcrumbTrail items={jobsIndexBreadcrumbItems(namespacePath)} label="Jobs location" />
          )
        }
        actions={
          !editorMode && jobs.length > 0 ? (
            <>
              <NamespacePicker compact namespaces={namespaces} onChange={onSelectNamespace} value={namespacePath} />
              <Button aria-expanded={false} onClick={onOpenCreate}>
                Create
              </Button>
            </>
          ) : null
        }
        title={pageTitle}
      />
      {editorMode ? (
        <RoutedJobEditor
          key={editorKey}
          initialValues={editorInitialValues}
          mode={editorMode}
          namespacePath={namespacePath}
          onCancel={onCloseEditor}
          onCreateJob={onCreateJob}
          onUpdateJob={onUpdateJob}
        />
      ) : null}
      {!editorMode && jobs.length === 0 ? (
        <section
          className={`${styles.emptyState} polished-panel polished-panel--accent-top`}
          aria-labelledby="jobs-empty-title"
        >
          <div>
            <p className="eyebrow">No stored jobs</p>
            <h2 id="jobs-empty-title">Create One Today</h2>
            <p>
              Stored jobs are reusable definitions you can trigger manually now and connect to richer sources later.
            </p>
          </div>
          <Button onClick={onOpenCreate}>Create</Button>
        </section>
      ) : null}
      {!editorMode && jobs.length > 0 ? (
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
              onOpen={() => onOpenJob(selectedJob.id)}
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

function RoutedJobEditor({
  initialValues,
  mode,
  namespacePath,
  onCancel,
  onCreateJob,
  onUpdateJob
}: {
  initialValues: JobFormValues;
  mode: JobEditorMode;
  namespacePath: string;
  onCancel: () => void;
  onCreateJob: (input: NewJob) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => void;
}) {
  const [values, setValues] = useState<JobFormValues>(initialValues);
  const [formError, setFormError] = useState("");

  return (
    <JobEditor
      error={formError}
      mode={mode}
      namespacePath={namespacePath}
      onCancel={onCancel}
      onCreateJob={onCreateJob}
      onError={setFormError}
      onUpdateJob={onUpdateJob}
      onValuesChange={setValues}
      values={values}
    />
  );
}
