import { StatusBadge, type RunStatus } from "./StatusBadge";

export type RunListItem = {
  id: string;
  jobName: string;
  runNumber: number;
  commit: string;
  duration: string;
  namespacePath?: string;
  status: RunStatus;
};

type RunListProps = {
  title: string;
  runs: RunListItem[];
};

export function RunList({ title, runs }: RunListProps) {
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
                <strong>
                  {run.jobName} <span>#{run.runNumber}</span>
                </strong>
                <small>
                  {run.namespacePath ? `${run.namespacePath} · ` : null}
                  {run.commit} · {run.duration}
                </small>
              </div>
              <StatusBadge status={run.status} />
            </li>
          ))}
        </ul>
      ) : (
        <p className="run-list__empty">No runs to show.</p>
      )}
    </section>
  );
}
