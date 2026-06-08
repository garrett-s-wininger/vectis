import { Clock, Code2, FileText, History, Server, User, Zap } from "lucide-react";
import {
  AppState,
  BreadcrumbTrail,
  Button,
  OperationalFact,
  PageHeader,
  StatusBadge,
  type RunListItem
} from "../components";
import { runActorLabel, runDurationLabel, runTriggerLabel } from "../components/data/RunPresentation";
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

type RunDetailPageProps = {
  onBack: () => void;
  onOpenJob?: (jobName: string) => void;
  run?: RunListItem;
  runID: string;
};

export function RunDetailPage({ onBack, onOpenJob, run, runID }: RunDetailPageProps) {
  if (!run) {
    return (
      <>
        <PageHeader
          actions={<Button onClick={onBack}>Back to Runs</Button>}
          description={`No run matched ${runID}.`}
          navigation={
            <BreadcrumbTrail
              items={[
                { label: "Runs", onClick: onBack },
                { label: runID, current: true }
              ]}
              label="Run location"
            />
          }
          title="Run not found"
        />
        <AppState
          actions={<Button onClick={onBack}>Back to Runs</Button>}
          description="The selected run is not present in the current console data."
          title="Run not found"
          tone="empty"
        />
      </>
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
              { label: run.jobName, onClick: onOpenJob ? () => onOpenJob(run.jobName) : undefined },
              { label: `#${run.runNumber}`, current: true }
            ]}
            label="Run location"
          />
        }
        title={`${run.jobName} (#${run.runNumber})`}
      />

      <section className="run-investigation-hero" aria-label="Run investigation summary">
        <div className="run-investigation-hero__header">
          <div>
            <h2>Summary</h2>
            <p>{run.id}</p>
          </div>
          <StatusBadge status={run.status} />
        </div>
        <dl className="run-investigation-facts">
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

      <section className="run-investigation-layout" aria-label="Run investigation">
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
    <section className="run-investigation-panel run-graph-panel" aria-labelledby="run-graph-title">
      <div className="run-investigation-panel__header">
        <div>
          <h2 id="run-graph-title">Graph</h2>
          <p>Execution nodes and selected task context.</p>
        </div>
      </div>
      <div className="run-node-graph" aria-label="Execution graph">
        <span className={`run-node-graph__node run-node-graph__node--${run.status}`}>
          <span className="run-node-graph__indicator" aria-hidden="true" />
          <span className="run-node-graph__label">root</span>
        </span>
      </div>
    </section>
  );
}

function RunLogs({ run }: { run: RunListItem }) {
  return (
    <section className="run-investigation-panel run-investigation-panel--primary" aria-labelledby="run-logs-title">
      <div className="run-investigation-panel__header">
        <div>
          <h2 id="run-logs-title">Logs</h2>
          <p>Worker output and dispatch messages for this run.</p>
        </div>
      </div>
      <pre className="code-block">{runLogLines(run).join("\n")}</pre>
    </section>
  );
}

function RunTimeline({ run }: { run: RunListItem }) {
  return (
    <section className="run-investigation-panel" aria-labelledby="run-timeline-title">
      <div className="run-investigation-panel__header">
        <div>
          <h2 id="run-timeline-title">Timeline</h2>
          <p>How this run moved through the system.</p>
        </div>
      </div>
      <ol className="run-event-list">
        {runTimelineEvents(run).map((event) => (
          <li key={event.label}>
            <History aria-hidden="true" />
            <div>
              <strong>
                {event.label}
                <span className="run-event-list__time">
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
    <section className="run-investigation-panel" aria-labelledby="run-definition-title">
      <div className="run-investigation-panel__header">
        <div>
          <h2 id="run-definition-title">{runDefinitionTitle(run)}</h2>
          <p>{runDefinitionDescription(run)}</p>
        </div>
      </div>
      <pre className="definition-preview">{formatRunDefinition(run)}</pre>
    </section>
  );
}
