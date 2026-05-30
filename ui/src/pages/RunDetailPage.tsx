import { useState } from "react";
import { AppState } from "../components";
import { Button } from "../components";
import { PageHeader } from "../components";
import type { RunListItem } from "../components";
import { StatusBadge } from "../components";

type RunDetailTab = "summary" | "definition" | "logs";

type RunDetailPageProps = {
  onBack: () => void;
  run?: RunListItem;
  runID: string;
};

const tabLabels: Record<RunDetailTab, string> = {
  summary: "Summary",
  definition: "Definition",
  logs: "Logs"
};

export function RunDetailPage({ onBack, run, runID }: RunDetailPageProps) {
  const [activeTab, setActiveTab] = useState<RunDetailTab>("summary");

  if (!run) {
    return (
      <>
        <PageHeader
          actions={<Button onClick={onBack}>Back to runs</Button>}
          description={`No run matched ${runID}.`}
          eyebrow="Run detail"
          title="Run not found"
        />
        <AppState
          actions={<Button onClick={onBack}>Back to runs</Button>}
          description="The selected run is not present in the current console data."
          title="Run not found"
          tone="empty"
        />
      </>
    );
  }

  const source = run.source ?? "stored";

  return (
    <>
      <PageHeader
        actions={<Button onClick={onBack}>Back to runs</Button>}
        description={`${run.namespacePath ?? "/"} · ${run.cellName ?? "unknown cell"} · ${sourceLabel(source)}`}
        eyebrow="Run detail"
        title={`${run.jobName} #${run.runNumber}`}
      />
      <div className="run-detail-tabs" role="tablist" aria-label="Run detail">
        {Object.entries(tabLabels).map(([tab, label]) => (
          <button
            aria-selected={activeTab === tab}
            className="run-detail-tabs__tab"
            key={tab}
            onClick={() => setActiveTab(tab as RunDetailTab)}
            role="tab"
            type="button"
          >
            {label}
          </button>
        ))}
      </div>
      {activeTab === "summary" ? <RunSummary run={run} /> : null}
      {activeTab === "definition" ? <RunDefinition run={run} /> : null}
      {activeTab === "logs" ? <RunLogs run={run} /> : null}
    </>
  );
}

function RunSummary({ run }: { run: RunListItem }) {
  const source = run.source ?? "stored";

  return (
    <section className="run-detail-layout" aria-label="Run summary">
      <div className="run-detail-panel">
        <div className="run-detail-panel__header">
          <h2>Summary</h2>
          <StatusBadge status={run.status} />
        </div>
        <dl className="run-detail-facts">
          <div>
            <dt>Run ID</dt>
            <dd>{run.id}</dd>
          </div>
          <div>
            <dt>Source</dt>
            <dd>
              <span className={`run-source run-source--${source}`}>{sourceLabel(source)}</span>
            </dd>
          </div>
          <div>
            <dt>Namespace</dt>
            <dd>{run.namespacePath ?? "/"}</dd>
          </div>
          <div>
            <dt>Cell</dt>
            <dd>{run.cellName ?? "unknown"}</dd>
          </div>
          <div>
            <dt>Submitted by</dt>
            <dd>{run.submittedBy ?? "unknown"}</dd>
          </div>
          <div>
            <dt>Reference</dt>
            <dd>{run.commit}</dd>
          </div>
          <div>
            <dt>Duration</dt>
            <dd>{run.duration}</dd>
          </div>
        </dl>
      </div>
      <div className="run-detail-panel">
        <div className="run-detail-panel__header">
          <h2>Events</h2>
        </div>
        <ol className="run-event-list">
          {eventsForRun(run).map((event) => (
            <li key={event.label}>
              <strong>{event.label}</strong>
              <span>{event.detail}</span>
            </li>
          ))}
        </ol>
      </div>
    </section>
  );
}

function RunDefinition({ run }: { run: RunListItem }) {
  return (
    <section className="run-detail-panel run-detail-panel--wide">
      <div className="run-detail-panel__header">
        <h2>{run.source === "ephemeral" ? "Submitted definition" : "Stored job definition"}</h2>
      </div>
      <pre className="code-block">{definitionForRun(run)}</pre>
    </section>
  );
}

function RunLogs({ run }: { run: RunListItem }) {
  return (
    <section className="run-detail-panel run-detail-panel--wide">
      <div className="run-detail-panel__header">
        <h2>Logs</h2>
        <StatusBadge status={run.status} />
      </div>
      <pre className="code-block">{logsForRun(run).join("\n")}</pre>
    </section>
  );
}

function sourceLabel(source: NonNullable<RunListItem["source"]>) {
  return source === "ephemeral" ? "Ephemeral" : "Stored";
}

function definitionForRun(run: RunListItem) {
  if (!run.definition) {
    return JSON.stringify(
      {
        id: run.jobName,
        source: run.source ?? "stored",
        run_id: run.id
      },
      null,
      2
    );
  }

  try {
    return JSON.stringify(JSON.parse(run.definition), null, 2);
  } catch {
    return run.definition;
  }
}

function eventsForRun(run: RunListItem) {
  const source = sourceLabel(run.source ?? "stored").toLowerCase();
  return [
    {
      label: "Accepted",
      detail: `${run.submittedBy ?? "unknown"} submitted ${source} work`
    },
    {
      label: "Persisted",
      detail: `${run.id} stored under ${run.namespacePath ?? "/"}`
    },
    {
      label: "Dispatch",
      detail: `${run.cellName ?? "unknown cell"} reports ${run.status}`
    }
  ];
}

function logsForRun(run: RunListItem) {
  const prefix = run.id;
  if (run.status === "queued") {
    return [`[${prefix}] accepted ${run.jobName}`, `[${prefix}] waiting for queue dispatch`];
  }

  if (run.status === "running") {
    return [
      `[${prefix}] accepted ${run.jobName}`,
      `[${prefix}] worker claimed run`,
      `[${prefix}] streaming output from ${run.cellName ?? "cell"}`
    ];
  }

  return [`[${prefix}] accepted ${run.jobName}`, `[${prefix}] completed with status ${run.status}`];
}
