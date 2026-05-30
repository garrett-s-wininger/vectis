import { StatusBadge, type RunStatus } from "./StatusBadge";
import { Button } from "./Button";
import styles from "./RunList.module.css";

export type RunListItem = {
  id: string;
  jobName: string;
  runNumber: number;
  commit: string;
  definition?: string;
  duration: string;
  cellName?: string;
  namespacePath?: string;
  source?: "stored" | "ephemeral";
  submittedBy?: string;
  status: RunStatus;
};

type RunListProps = {
  onSelectRun?: (runID: string) => void;
  title: string;
  runs: RunListItem[];
};

export function RunList({ onSelectRun, title, runs }: RunListProps) {
  return (
    <section className={styles.root} aria-labelledby="run-list-title">
      <div className={styles.header}>
        <h2 id="run-list-title">{title}</h2>
      </div>
      {runs.length > 0 ? (
        <ul className={styles.items}>
          {runs.map((run) => (
            <li className={styles.item} key={run.id}>
              <div className={styles.summary}>
                <div className={styles.title}>
                  <strong>
                    {run.jobName} <span>#{run.runNumber}</span>
                  </strong>
                  <span
                    className={`${styles.source} ${
                      styles[run.source ?? "stored"]
                    }`}
                  >
                    {run.source === "ephemeral" ? "Ephemeral" : "Stored"}
                  </span>
                </div>
                <small>
                  {run.namespacePath ? `${run.namespacePath} · ` : null}
                  {run.cellName ? `${run.cellName} · ` : null}
                  {run.submittedBy ? `${run.submittedBy} · ` : null}
                  {run.commit} · {run.duration}
                </small>
              </div>
              <div className={styles.actions}>
                <StatusBadge status={run.status} />
                {onSelectRun ? (
                  <Button
                    aria-label={`Open run ${run.jobName} #${run.runNumber}`}
                    onClick={() => onSelectRun(run.id)}
                  >
                    Open
                  </Button>
                ) : null}
              </div>
            </li>
          ))}
        </ul>
      ) : (
        <p className={styles.empty}>No runs to show.</p>
      )}
    </section>
  );
}
