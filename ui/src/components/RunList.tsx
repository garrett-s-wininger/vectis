import { StatusBadge, type RunStatus } from "./StatusBadge";
import { Button } from "./Button";

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
    <section className="run-list" aria-labelledby="run-list-title">
      <div className="run-list__header">
        <h2 id="run-list-title">{title}</h2>
      </div>
      {runs.length > 0 ? (
        <ul className="run-list__items">
          {runs.map((run) => (
            <li className="run-list__item" key={run.id}>
              <div className="run-list__summary">
                <div className="run-list__title">
                  <strong>
                    {run.jobName} <span>#{run.runNumber}</span>
                  </strong>
                  <span
                    className={`run-source run-source--${
                      run.source ?? "stored"
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
              <div className="run-list__actions">
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
        <p className="run-list__empty">No runs to show.</p>
      )}
    </section>
  );
}
