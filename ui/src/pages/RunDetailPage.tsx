import { useConsoleAccess } from "../domain/access";
import { useState } from "react";
import {
  ArrowUpRight,
  ChevronDown,
  Clock,
  Code2,
  FileText,
  Fingerprint,
  Hash,
  History,
  Server,
  User,
  Zap
} from "lucide-react";
import { BreadcrumbTrail, CopyValue, OperationalFact, PageHeader, StatusBadge, type RunListItem } from "../components";
import { SelectControl } from "../components/primitives/SelectControl";
import type { RunInvestigationContext } from "../routing/runInvestigation";
import { runAttemptNumbers, runForAttempt } from "./RunAttemptPresentation";
import {
  runActorLabel,
  runDisplayName,
  runDurationLabel,
  runFamilyIdentifier,
  runTriggerLabel
} from "../components/data/RunPresentation";
import { RunLogs } from "./RunLogs";
import { RunArtifacts } from "./RunArtifacts";
import type { LoadRunArtifacts } from "../data/runArtifacts";
import { RunControls, type RunControlActions } from "./RunControls";
import { formatNamespaceCrumb } from "./jobs/JobBreadcrumbs";
import {
  formatRunDefinition,
  referenceLabel,
  runDefinitionDescription,
  runDefinitionTitle,
  runDetailDescription,
  runTasksForDisplay,
  runTimelineEvents,
  sourceLabel
} from "./RunDetailPresentation";
import { RunTaskGraph } from "./RunTaskGraph";
import { PageMissingState } from "./shared";
import styles from "./RunDetailPage.module.css";

type RunDetailPageProps = RunControlActions & {
  sharedWorkspace?: boolean;
  onBack: () => void;
  onOpenJob?: (jobName: string) => void;
  onOpenRun?: (runID: string) => Promise<void>;
  run?: RunListItem;
  runID: string;
  originalRun?: RunListItem;
  loadArtifacts?: LoadRunArtifacts;
  investigation?: RunInvestigationContext;
  onInvestigationChange?: (context: RunInvestigationContext) => void;
};

export function RunDetailPage({
  onBack,
  sharedWorkspace = false,
  onOpenJob,
  onOpenRun,
  onCancel,
  onReplay,
  run,
  runID,
  originalRun,
  loadArtifacts,
  investigation,
  onInvestigationChange
}: RunDetailPageProps) {
  if (!run) {
    return (
      <PageMissingState
        actionLabel="View Runs"
        breadcrumbs={[
          { label: "Runs", onClick: onBack },
          { current: true, label: "Missing" }
        ]}
        description={`Run ${runID} is no longer available, or the route points to an ID that does not exist.`}
        label="Run location"
        onAction={onBack}
        panelDescription="Return to the runs index to choose an active execution record."
        panelEyebrow="Missing Run"
        panelTitle="No Run Found"
        title="Run Not Found"
      />
    );
  }

  return (
    <RunDetailContent
      key={run.id}
      onBack={onBack}
      sharedWorkspace={sharedWorkspace}
      onOpenJob={onOpenJob}
      onOpenRun={onOpenRun}
      onCancel={onCancel}
      onReplay={onReplay}
      run={run}
      originalRun={originalRun}
      loadArtifacts={loadArtifacts}
      investigation={investigation}
      onInvestigationChange={onInvestigationChange}
    />
  );
}

function RunDetailContent({
  onBack,
  sharedWorkspace = false,
  onOpenJob,
  onOpenRun,
  onCancel,
  onReplay,
  originalRun,
  loadArtifacts,
  investigation,
  onInvestigationChange,
  run
}: RunControlActions & {
  onBack: () => void;
  onOpenJob?: (jobName: string) => void;
  onOpenRun?: (runID: string) => Promise<void>;
  sharedWorkspace?: boolean;
  run: RunListItem;
  originalRun?: RunListItem;
  loadArtifacts?: LoadRunArtifacts;
  investigation?: RunInvestigationContext;
  onInvestigationChange?: (context: RunInvestigationContext) => void;
}) {
  const { can } = useConsoleAccess();
  const canOperate = can("run:operator", run.namespacePath ?? "/");
  const [localInvestigation, setLocalInvestigation] = useState<RunInvestigationContext>({ attempt: "latest" });
  const context = investigation ?? localInvestigation;
  const updateInvestigation = onInvestigationChange ?? setLocalInvestigation;
  const attemptSelection = context.attempt;
  const attemptNumbers = runAttemptNumbers(run);
  const latestAttempt = attemptNumbers[0] ?? 1;
  const selectedAttempt = attemptSelection === "latest" ? latestAttempt : attemptSelection;
  const displayRun = runForAttempt(run, selectedAttempt);
  const historical = selectedAttempt !== latestAttempt;
  const tasks = runTasksForDisplay(displayRun);
  const active = run.status === "queued" || run.status === "running";
  const awaitingTasks =
    !historical &&
    (run.tasksUpdating ||
      ((run.status === "queued" || run.status === "running") &&
        tasks.length === 1 &&
        tasks[0].taskKey === "root" &&
        tasks[0].status === "succeeded"));

  const [openingOriginal, setOpeningOriginal] = useState(false);
  const selectedTask =
    tasks.find((task) => task.taskKey === context.task) ?? tasks.find((task) => task.taskID === context.task);
  const selectTask = (taskID: string) => {
    const task = tasks.find((candidate) => candidate.taskID === taskID);
    updateInvestigation({ ...context, task: selectedTask?.taskID === taskID ? undefined : task?.taskKey });
  };
  const runOnceID = run.source === "ephemeral" && run.jobID ? run.jobID.split("-").at(-1) : runFamilyIdentifier(run);

  return (
    <>
      <PageHeader
        actions={
          canOperate && ((!historical && active && onCancel) || (!active && onReplay)) ? (
            <RunControls run={run} onCancel={historical ? undefined : onCancel} onReplay={onReplay} />
          ) : undefined
        }
        description={runDetailDescription()}
        navigation={
          <BreadcrumbTrail
            items={[
              ...(!sharedWorkspace ? [{ label: formatNamespaceCrumb(run.namespacePath ?? "/") }] : []),
              { label: "Runs", onClick: onBack },
              {
                label: runOnceID ?? runDisplayName(run),
                onClick: run.source === "stored" && onOpenJob ? () => onOpenJob(run.jobName) : undefined
              },
              { label: `#${run.runNumber}`, current: true }
            ]}
            label="Run location"
          />
        }
        title={`${runDisplayName(run)} (#${run.runNumber})`}
      />

      <section className={`${styles.hero} polished-panel polished-panel--accent-top`} aria-label="Run summary">
        <div className={styles.heroHeader}>
          <div>
            <h2>Run summary</h2>
          </div>
          <div className={styles.overallStatus}>
            <span>Overall run</span>
            <StatusBadge status={run.status} />
          </div>
        </div>
        <dl className={styles.facts}>
          <OperationalFact
            emphasis
            icon={Clock}
            label={runDurationLabel(displayRun.status)}
            value={displayRun.duration}
          />
          <OperationalFact icon={Server} label="Cell" value={displayRun.cellName ?? "Unassigned"} />
          <OperationalFact icon={Zap} label="Trigger" value={runTriggerLabel(run)} />
          <OperationalFact
            icon={User}
            label="Actor"
            value={run.submittedBy ? runActorLabel(run.submittedBy) : "Unknown"}
          />
          <OperationalFact icon={FileText} label="Source" value={sourceLabel(run.source ?? "stored")} />
          <OperationalFact icon={Code2} label="Reference" value={referenceLabel(run)} />
        </dl>
        <dl className={styles.metadata}>
          <OperationalFact
            layout="detail"
            icon={Fingerprint}
            label="Run ID"
            value={
              <CopyValue appearance="field" label="Run ID" value={run.id}>
                <code>{run.id}</code>
              </CopyValue>
            }
            wrap
          />
          {run.jobID ? (
            <OperationalFact
              layout="detail"
              icon={Hash}
              label="Job ID"
              value={
                <CopyValue appearance="field" label="Job ID" value={run.jobID}>
                  <code>{run.jobID}</code>
                </CopyValue>
              }
              wrap
            />
          ) : null}
          {run.replayOfRunID ? (
            <OperationalFact
              layout="detail"
              icon={History}
              label="Re-run of"
              wrap
              value={
                <a
                  className={styles.runReference}
                  href={`/runs/${encodeURIComponent(run.replayOfRunID)}`}
                  aria-busy={openingOriginal || undefined}
                  onClick={(event) => {
                    if (
                      !onOpenRun ||
                      !run.replayOfRunID ||
                      event.defaultPrevented ||
                      event.button !== 0 ||
                      event.altKey ||
                      event.ctrlKey ||
                      event.metaKey ||
                      event.shiftKey
                    )
                      return;

                    event.preventDefault();
                    if (openingOriginal) return;

                    setOpeningOriginal(true);
                    void onOpenRun(run.replayOfRunID).finally(() => setOpeningOriginal(false));
                  }}
                  aria-label={`View original run${originalRun ? ` ${runDisplayName(originalRun)} #${originalRun.runNumber}` : ""}`}
                >
                  <span>
                    {originalRun ? `${runDisplayName(originalRun)} #${originalRun.runNumber}` : "Original run"}
                  </span>
                  <ArrowUpRight aria-hidden="true" />
                </a>
              }
            />
          ) : null}
        </dl>
      </section>

      <section className={styles.layout} aria-label="Run investigation">
        <RunTaskGraph
          awaitingTasks={awaitingTasks}
          onSelectTask={selectTask}
          selectedTaskID={selectedTask?.taskID ?? context.task}
          onClearSelection={() => updateInvestigation({ ...context, task: undefined })}
          selectionNotice={
            context.task && run.tasks && !awaitingTasks && !selectedTask
              ? "The selected task is not available in the current graph. Showing all tasks."
              : undefined
          }
          tasks={tasks}
          controls={
            attemptNumbers.length > 1 || attemptSelection !== "latest" ? (
              <SelectControl
                summaryLabel="Attempt"
                aria-label="Attempt"
                leadingIcon={<History />}
                value={String(attemptSelection)}
                onChange={(event) =>
                  updateInvestigation({
                    ...context,
                    task: selectedTask?.taskKey ?? context.task,
                    attempt: event.target.value === "latest" ? "latest" : Number(event.target.value)
                  })
                }
                options={[
                  { value: "latest", label: attemptNumbers.length ? `Latest (attempt ${latestAttempt})` : "Latest" },
                  ...[
                    ...new Set([...attemptNumbers, ...(typeof attemptSelection === "number" ? [attemptSelection] : [])])
                  ]
                    .sort((a, b) => b - a)
                    .map((attempt) => ({ value: String(attempt), label: `Attempt ${attempt}` }))
                ]}
              />
            ) : null
          }
        />
        <RunLogs run={run} selectedTask={selectedTask} attempt={selectedAttempt} />
        {loadArtifacts ? (
          <RunArtifacts run={run} selectedTask={selectedTask} loadArtifacts={loadArtifacts} attempt={selectedAttempt} />
        ) : null}
        <RunTimeline run={run} />
        <RunDefinition run={run} />
      </section>
    </>
  );
}

function RunTimeline({ run }: { run: RunListItem }) {
  return (
    <section
      className={`${styles.panel} ${styles.timelinePanel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-timeline-title"
    >
      <div className={styles.panelHeader}>
        <div>
          <h2 id="run-timeline-title">Timeline</h2>
          <p>Overall run checkpoints and elapsed time.</p>
        </div>
      </div>
      <ol className={styles.timelineList}>
        {runTimelineEvents(run).map((event) => (
          <li key={event.label}>
            <span className={styles.timelineMarker}>
              <Clock aria-hidden="true" />
            </span>
            <div className={styles.timelineBody}>
              <div className={styles.timelineHeader}>
                <strong>{event.label}</strong>
                <span className={styles.eventTime}>
                  {event.time}
                  {event.delta ? <small>{event.delta}</small> : null}
                </span>
              </div>
              <p>{event.detail}</p>
            </div>
          </li>
        ))}
      </ol>
    </section>
  );
}

function RunDefinition({ run }: { run: RunListItem }) {
  return (
    <section
      className={`${styles.panel} ${styles.definitionPanel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-definition-title"
    >
      <details className={styles.definitionDetails}>
        <summary className={styles.definitionSummary}>
          <span className={styles.definitionIcon}>
            <FileText aria-hidden="true" />
          </span>
          <div>
            <h2 id="run-definition-title">{runDefinitionTitle(run)}</h2>
            <p>{runDefinitionDescription(run)}</p>
          </div>
          <span className={styles.definitionDisclosure} aria-hidden="true">
            <ChevronDown />
          </span>
        </summary>
        <pre className={`definition-preview ${styles.definitionPreview}`}>{formatRunDefinition(run)}</pre>
      </details>
    </section>
  );
}
