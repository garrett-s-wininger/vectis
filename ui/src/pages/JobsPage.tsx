import { useConsoleAccess } from "../domain/access";
import type { SourceRepository } from "../domain/console";
import { Button } from "../components";
import { BreadcrumbTrail } from "../components";
import { EmptyStatePanel } from "../components";
import { NamespacePicker } from "../components";
import { PageHeader } from "../components";
import type { RunListItem } from "../components";
import type { Job, Namespace, NewJob, UpdateJob } from "../domain/console";
import { JobDetailPage } from "./jobs/JobDetailPage";
import { emptyJobForm, type JobEditorMode, valuesFromJob } from "./jobs/JobEditorModel";
import { JobList } from "./jobs/JobList";
import { jobEditorBreadcrumbItems, jobsIndexBreadcrumbItems } from "./jobs/JobBreadcrumbs";
import { getLatestRunForJob, getRunsForJob } from "./jobs/JobPresentation";
import { EmptyStateRail, PageMissingState } from "./shared";
import { RoutedJobEditor } from "./jobs/RoutedJobEditor";

type ActiveJobEditorMode = JobEditorMode | null;

type JobsPageProps = {
  repositories?: SourceRepository[];
  allJobs?: Job[];
  detailJobID?: string;
  editorMode?: ActiveJobEditorMode;
  jobs: Job[];
  namespaces: Namespace[];
  namespacePath: string;
  onCloseEditor: () => void;
  onCreateJob: (input: NewJob) => Promise<void> | void;
  onOpenCreate: () => void;
  onOpenEditor: (jobID: string) => void;
  onOpenJob: (jobID: string) => void;
  onOpenJobRuns?: (jobName: string) => void;
  onSelectRun: (runID: string) => void;
  onSelectNamespace: (namespacePath: string) => void;
  onTriggerRun: (jobID: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => Promise<void> | void;
  runs: RunListItem[];
};

export function JobsPage({
  repositories = [],
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
  const { can } = useConsoleAccess();
  const canWrite = can("job:write", namespacePath);
  const routableJobs = allJobs ?? jobs;
  const detailJob = detailJobID ? routableJobs.find((job) => job.id === detailJobID) : null;
  const editorJob =
    editorMode?.kind === "edit" ? (routableJobs.find((candidate) => candidate.id === editorMode.jobID) ?? null) : null;

  const editorInitialValues = editorJob ? valuesFromJob(editorJob) : emptyJobForm;
  const editorKey = editorMode?.kind === "edit" ? `edit:${editorMode.jobID}` : (editorMode?.kind ?? "");
  const editorNamespacePath = editorJob?.namespacePath ?? namespacePath;
  const editorJobName = editorJob?.name ?? (editorMode?.kind === "edit" ? editorMode.jobID : "Create");
  const pageTitle = editorMode ? (editorMode.kind === "edit" ? "Configure Job" : "Create Job") : "Jobs";
  const pageDescription = editorMode
    ? editorMode.kind === "edit"
      ? "Update editable settings, trigger policy, and definition JSON."
      : "Create a saved definition and choose how it can be triggered."
    : "Stored definitions and triggers.";

  function startEditJob(job: Job) {
    onOpenEditor(job.id);
  }

  if (detailJobID && !detailJob) {
    return (
      <PageMissingState
        actionLabel="View Jobs"
        breadcrumbs={jobMissingBreadcrumbItems(namespacePath)}
        description="This job is no longer available, or the route points to an ID that does not exist."
        label="Job location"
        onAction={() => onOpenJob("")}
        panelDescription="Return to the jobs index to choose an active definition."
        panelEyebrow="Missing Job"
        panelTitle="No Job Found"
        title="Job Not Found"
      />
    );
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
          !editorMode ? (
            <>
              <NamespacePicker compact namespaces={namespaces} onChange={onSelectNamespace} value={namespacePath} />
              {canWrite && jobs.length > 0 ? (
                <Button aria-expanded={false} onClick={onOpenCreate}>
                  Create
                </Button>
              ) : null}
            </>
          ) : null
        }
        title={pageTitle}
      />
      {editorMode ? (
        <RoutedJobEditor
          key={editorKey}
          initialValues={editorInitialValues}
          repositories={repositories}
          mode={editorMode}
          namespacePath={namespacePath}
          onCancel={onCloseEditor}
          onCreateJob={onCreateJob}
          onUpdateJob={onUpdateJob}
        />
      ) : null}
      {!editorMode && jobs.length === 0 ? (
        <EmptyStateRail>
          <EmptyStatePanel
            actions={canWrite ? <Button onClick={onOpenCreate}>Create</Button> : undefined}
            description={
              canWrite
                ? "Create a source-backed job to make a reusable definition available here."
                : "No jobs are available in this namespace. An administrator can add a source-backed job; operators can submit a Run Once from Runs."
            }
            eyebrow="No Stored Jobs"
            title={canWrite ? "Create One Today" : "No Jobs in This Namespace"}
            titleID="jobs-empty-title"
          />
        </EmptyStateRail>
      ) : null}
      {!editorMode && jobs.length > 0 ? <JobList jobs={jobs} onOpen={onOpenJob} runs={runs} /> : null}
    </>
  );
}

function jobMissingBreadcrumbItems(namespacePath: string) {
  return [
    ...jobsIndexBreadcrumbItems(namespacePath).map((item) => ({ ...item, current: false })),
    { current: true, label: "Missing" }
  ];
}
