import { useState } from "react";
import { ChevronDown, Clock, Code2, FileText, Fingerprint, Hash, History, Server, User, Zap } from "lucide-react";
import { BreadcrumbTrail, OperationalFact, PageHeader, StatusBadge, type RunListItem } from "../components";
import {
  runActorLabel,
  runDisplayName,
  runDurationLabel,
  runFamilyIdentifier,
  runTriggerLabel
} from "../components/data/RunPresentation";
import { RunLogs } from "./RunLogs";
import { RunControls, type RunControlActions } from "./RunControls";
import { formatNamespaceCrumb } from "./jobs/JobBreadcrumbs";
import {
  formatRunDefinition,
  referenceLabel,
  runDefinitionDescription,
  runDefinitionTitle,
  runDetailDescription,
  runTasksForDisplay,
  preferredRunTaskID,
  runTimelineEvents,
  sourceLabel
} from "./RunDetailPresentation";
import { RunTaskGraph } from "./RunTaskGraph";
import { PageMissingState } from "./shared";
import styles from "./RunDetailPage.module.css";

type RunDetailPageProps = RunControlActions & {
  onBack: () => void;
  onOpenJob?: (jobName: string) => void;
  onOpenRun?: (runID: string) => Promise<void>;
  run?: RunListItem;
  runID: string;
  originalRun?: RunListItem;
};

export function RunDetailPage({
  onBack,
  onOpenJob,
  onOpenRun,
  onCancel,
  onReplay,
  run,
  runID,
  originalRun
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
      onOpenJob={onOpenJob}
      onOpenRun={onOpenRun}
      onCancel={onCancel}
      onReplay={onReplay}
      run={run}
      originalRun={originalRun}
    />
  );
}

function RunDetailContent({
  onBack,
  onOpenJob,
  onOpenRun,
  onCancel,
  onReplay,
  originalRun,
  run
}: RunControlActions & {
  onBack: () => void;
  onOpenJob?: (jobName: string) => void;
  onOpenRun?: (runID: string) => Promise<void>;
  run: RunListItem;
  originalRun?: RunListItem;
}) {
  const tasks = runTasksForDisplay(run);
  const awaitingTasks =
    run.tasksUpdating ||
    ((run.status === "queued" || run.status === "running") &&
      tasks.length === 1 &&
      tasks[0].taskKey === "root" &&
      tasks[0].status === "succeeded");
  const preferredTaskID = preferredRunTaskID(tasks);
  const [automaticTaskID, setAutomaticTaskID] = useState(preferredTaskID);
  const [selection, setSelection] = useState<string | null>(null);
  const [openingOriginal, setOpeningOriginal] = useState(false);
  const preferredTask = tasks.find((task) => task.taskID === preferredTaskID);

  if (
    !selection &&
    preferredTask &&
    ["running", "accepted", "failed", "aborted"].includes(preferredTask.status) &&
    automaticTaskID !== preferredTaskID
  ) {
    setAutomaticTaskID(preferredTaskID);
  }

  const selectedTaskID =
    selection && tasks.some((task) => task.taskID === selection)
      ? selection
      : tasks.some((task) => task.taskID === automaticTaskID)
        ? automaticTaskID
        : preferredTaskID;

  const selectedTask = tasks.find((task) => task.taskID === selectedTaskID) ?? tasks[0];
  const selectTask = (taskID: string) => setSelection(taskID);
  const runOnceID = run.source === "ephemeral" && run.jobID ? run.jobID.split("-").at(-1) : runFamilyIdentifier(run);

  return (
    <>
      <PageHeader
        actions={onCancel || onReplay ? <RunControls run={run} onCancel={onCancel} onReplay={onReplay} /> : undefined}
        description={runDetailDescription()}
        navigation={
          <BreadcrumbTrail
            items={[
              { label: formatNamespaceCrumb(run.namespacePath ?? "/") },
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
          <StatusBadge status={run.status} />
        </div>
        <dl className={styles.facts}>
          <OperationalFact emphasis icon={Clock} label={runDurationLabel(run.status)} value={run.duration} />
          <OperationalFact icon={Server} label="Cell" value={run.cellName ?? "Unassigned"} />
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
          <OperationalFact icon={Fingerprint} label="Run ID" value={<code>{run.id}</code>} wrap />
          {run.jobID ? <OperationalFact icon={Hash} label="Job ID" value={<code>{run.jobID}</code>} wrap /> : null}
          {run.replayOfRunID ? (
            <OperationalFact
              icon={History}
              label="Re-run of"
              wrap
              value={
                <a
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
                  {originalRun ? `${runDisplayName(originalRun)} #${originalRun.runNumber}` : "Original run"}
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
          selectedTaskID={selectedTask?.taskID}
          tasks={tasks}
        />
        <RunLogs run={run} selectedTask={selectedTask} />
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
          <p>System checkpoints and elapsed time.</p>
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
