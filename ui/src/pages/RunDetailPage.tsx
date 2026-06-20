import { useEffect, useState } from "react";
import { Clock, Code2, FileText, History, Server, User, Zap } from "lucide-react";
import {
  BreadcrumbTrail,
  ErrorAlert,
  OperationalFact,
  PageHeader,
  StatusBadge,
  type RunListItem
} from "../components";
import { runActorLabel, runDisplayName, runDurationLabel, runTriggerLabel } from "../components/data/RunPresentation";
import { streamRunLogs, type RunLogEntry } from "../data/runLogs";
import { formatNamespaceCrumb } from "./jobs/JobBreadcrumbs";
import {
  formatRunDefinition,
  referenceLabel,
  runDefinitionDescription,
  runDefinitionTitle,
  runDetailDescription,
  runLogLines,
  runTimelineEvents,
  sourceLabel
} from "./RunDetailPresentation";
import { PageMissingState } from "./shared";
import styles from "./RunDetailPage.module.css";

type RunDetailPageProps = {
  onBack: () => void;
  onOpenJob?: (jobName: string) => void;
  run?: RunListItem;
  runID: string;
};

export function RunDetailPage({ onBack, onOpenJob, run, runID }: RunDetailPageProps) {
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
    <>
      <PageHeader
        description={runDetailDescription()}
        navigation={
          <BreadcrumbTrail
            items={[
              { label: formatNamespaceCrumb(run.namespacePath ?? "/") },
              { label: "Runs", onClick: onBack },
              {
                label: runDisplayName(run),
                onClick: run.source === "stored" && onOpenJob ? () => onOpenJob(run.jobName) : undefined
              },
              { label: `#${run.runNumber}`, current: true }
            ]}
            label="Run location"
          />
        }
        title={`${runDisplayName(run)} (#${run.runNumber})`}
      />

      <section
        className={`${styles.hero} polished-panel polished-panel--accent-top`}
        aria-label="Run investigation summary"
      >
        <div className={styles.heroHeader}>
          <div>
            <h2>Summary</h2>
            <p>{run.id}</p>
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
      </section>

      <section className={styles.layout} aria-label="Run investigation">
        <RunGraph run={run} />
        <RunLogs run={run} />
        <RunDefinition run={run} />
        <RunTimeline run={run} />
      </section>
    </>
  );
}

function RunGraph({ run }: { run: RunListItem }) {
  return (
    <section
      className={`${styles.panel} ${styles.graphPanel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-graph-title"
    >
      <div className={styles.panelHeader}>
        <div>
          <h2 id="run-graph-title">Graph</h2>
          <p>Execution nodes and selected task context.</p>
        </div>
      </div>
      <div className={styles.graph} aria-label="Execution graph">
        <span className={`${styles.graphNode} ${styles[run.status]}`}>
          <span className={styles.graphIndicator} aria-hidden="true" />
          <span className={styles.graphLabel}>root</span>
        </span>
      </div>
    </section>
  );
}

function RunLogs({ run }: { run: RunListItem }) {
  const [logState, setLogState] = useState<{ entries: RunLogEntry[]; error: string; runID: string }>({
    entries: [],
    error: "",
    runID: run.id
  });

  useEffect(() => {
    return streamRunLogs(
      run.id,
      (entry) => {
        setLogState((state) => {
          const entries = state.runID === run.id ? state.entries : [];
          if (entries.some((candidate) => candidate.sequence === entry.sequence && entry.sequence >= 0)) {
            return state;
          }

          return {
            runID: run.id,
            error: "",
            entries: [...entries, entry].sort((a, b) => a.sequence - b.sequence)
          };
        });
      },
      (error) => setLogState({ runID: run.id, entries: [], error })
    );
  }, [run.id]);

  const visibleLogState = logState.runID === run.id ? logState : { entries: [], error: "", runID: run.id };
  const logLines =
    visibleLogState.entries.length > 0
      ? formatRunLogEntries(visibleLogState.entries)
      : runLogLines(run);

  return (
    <section
      className={`${styles.panel} ${styles.primaryPanel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-logs-title"
    >
      <div className={styles.panelHeader}>
        <div>
          <h2 id="run-logs-title">Logs</h2>
          <p>Worker output and dispatch messages for this run.</p>
        </div>
      </div>
      <ErrorAlert message={visibleLogState.error} title="Log Stream Unavailable" />
      <pre className={`code-block ${styles.logs}`}>{logLines.join("\n")}</pre>
    </section>
  );
}

function formatRunLogEntries(entries: RunLogEntry[]) {
  return entries.map((entry) => (entry.stream === "stderr" ? `[stderr] ${entry.data}` : entry.data));
}

function RunTimeline({ run }: { run: RunListItem }) {
  return (
    <section
      className={`${styles.panel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-timeline-title"
    >
      <div className={styles.panelHeader}>
        <div>
          <h2 id="run-timeline-title">Timeline</h2>
          <p>How this run moved through the system.</p>
        </div>
      </div>
      <ol className={styles.eventList}>
        {runTimelineEvents(run).map((event) => (
          <li key={event.label}>
            <History aria-hidden="true" />
            <div>
              <strong>
                {event.label}
                <span className={styles.eventTime}>
                  {event.time}
                  {event.delta ? <small>{event.delta}</small> : null}
                </span>
              </strong>
              <span>{event.detail}</span>
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
      className={`${styles.panel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-definition-title"
    >
      <div className={styles.panelHeader}>
        <div>
          <h2 id="run-definition-title">{runDefinitionTitle(run)}</h2>
          <p>{runDefinitionDescription(run)}</p>
        </div>
      </div>
      <pre className="definition-preview">{formatRunDefinition(run)}</pre>
    </section>
  );
}
