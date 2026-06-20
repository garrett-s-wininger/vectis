import type { ReactNode } from "react";
import { Clock, Server, User, Zap } from "lucide-react";
import { OperationalFact } from "../primitives/OperationalFact";
import { StatusBadge, type RunStatus } from "../status/StatusBadge";
import {
  runActorLabel,
  runCountLabel,
  runDisplayName,
  runDurationLabel,
  runStatusClass,
  runTriggerLabel,
  type RunTrigger
} from "./RunPresentation";
import styles from "./RunList.module.css";

export type RunListItem = {
  id: string;
  jobName: string;
  runNumber: number;
  commit: string;
  createdAt?: string;
  definition?: string;
  definitionVersion?: number;
  duration: string;
  finishedAt?: string;
  cellName?: string;
  namespacePath?: string;
  source?: "stored" | "ephemeral";
  startedAt?: string;
  submittedBy?: string;
  trigger?: RunTrigger;
  status: RunStatus;
};

type RunListProps = {
  emptyMessage?: string;
  hideJobName?: boolean;
  onSelectRun?: (runID: string) => void;
  title: string;
  runs: RunListItem[];
};

export function RunList({ emptyMessage = "No runs match the current filters.", hideJobName, onSelectRun, title, runs }: RunListProps) {
  return (
    <section className={`${styles.root} polished-panel polished-panel--accent-top`} aria-labelledby="run-list-title">
      <div className={styles.header}>
        <div className={styles.headingGroup}>
          <h2 id="run-list-title">{title}</h2>
          <p>Latest execution records</p>
        </div>
        <span className={styles.count}>{runCountLabel(runs.length)}</span>
      </div>
      {runs.length > 0 ? (
        <ul className={styles.items}>
          {runs.map((run) => (
            <li className={`${styles.item} ${styles[runStatusClass(run.status)]}`} key={run.id}>
              <RunRowAction
                label={`Open run ${runDisplayName(run)} #${run.runNumber}`}
                onSelect={onSelectRun}
                runID={run.id}
              >
                <div className={styles.summary}>
                  <div className={styles.identity}>
                    <strong>{hideJobName ? `Run #${run.runNumber}` : runDisplayName(run)}</strong>
                    {!hideJobName ? <span>Run #{run.runNumber}</span> : null}
                  </div>
                  <dl className={styles.meta}>
                    <OperationalFact emphasis icon={Clock} label={runDurationLabel(run.status)} value={run.duration} />
                    <OperationalFact icon={Server} label="Cell" value={run.cellName ?? "Unassigned"} />
                    <OperationalFact icon={Zap} label="Trigger" value={runTriggerLabel(run)} />
                    {run.submittedBy ? (
                      <OperationalFact icon={User} label="Actor" value={runActorLabel(run.submittedBy)} />
                    ) : null}
                  </dl>
                </div>
              </RunRowAction>
              <div className={styles.actions}>
                <StatusBadge status={run.status} />
              </div>
            </li>
          ))}
        </ul>
      ) : (
        <p className={styles.empty}>{emptyMessage}</p>
      )}
    </section>
  );
}

function RunRowAction({
  children,
  label,
  onSelect,
  runID
}: {
  children: ReactNode;
  label: string;
  onSelect?: (runID: string) => void;
  runID: string;
}) {
  if (!onSelect) {
    return <div className={styles.rowAction}>{children}</div>;
  }

  return (
    <button aria-label={label} className={styles.rowAction} onClick={() => onSelect(runID)} type="button">
      {children}
    </button>
  );
}
