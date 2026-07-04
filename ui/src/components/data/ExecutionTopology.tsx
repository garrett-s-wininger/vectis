import { Activity, Gauge, Network, Server, type LucideIcon } from "lucide-react";
import { useId } from "react";
import { ResourceStatus, type ResourceStatusTone } from "../status/ResourceStatus";
import styles from "./ExecutionTopology.module.css";

export type ExecutionTopologyStatus = "degraded" | "healthy" | "offline";

export type ExecutionTopologyCell = {
  id: string;
  endpoint: string;
  name: string;
  queueDepth: number;
  status: ExecutionTopologyStatus;
  workloadLabel: string;
  workloadValue: string;
  workers: string;
};

type ExecutionTopologyProps = {
  cells: ExecutionTopologyCell[];
  countLabel: string;
  emptyMessage: string;
  onSelectCell?: (cellID: string) => void;
};

export function ExecutionTopology({ cells, countLabel, emptyMessage, onSelectCell }: ExecutionTopologyProps) {
  const titleID = useId();
  const mapClassName = cells.length === 1 ? `${styles.map} ${styles.singleMap}` : styles.map;
  const cellsClassName = cells.length === 1 ? `${styles.cells} ${styles.singleCell}` : styles.cells;

  return (
    <section className={`${styles.root} polished-panel polished-panel--accent-top`} aria-labelledby={titleID}>
      <div className={styles.header}>
        <div className={styles.headingGroup}>
          <h2 id={titleID}>Cells</h2>
          <p>Execution Topology</p>
        </div>
        <span className={styles.count}>{countLabel}</span>
      </div>
      {cells.length > 0 ? (
        <div className={mapClassName}>
          <div className={styles.gateway}>
            <span className={styles.gatewayIcon} aria-hidden="true">
              <Network />
            </span>
            <div>
              <strong>Gateway</strong>
              <span>Routes work into cells</span>
            </div>
          </div>
          <span className={styles.connector} aria-hidden="true" />
          <ul className={cellsClassName}>
            {cells.map((cell) => (
              <li className={styles.cellItem} key={cell.id}>
                <CellNode cell={cell} onSelect={onSelectCell} />
              </li>
            ))}
          </ul>
        </div>
      ) : (
        <p className={styles.empty}>{emptyMessage}</p>
      )}
    </section>
  );
}

function CellNode({ cell, onSelect }: { cell: ExecutionTopologyCell; onSelect?: (cellID: string) => void }) {
  const content = (
    <>
      <div className={styles.cellHeader}>
        <span className={`${styles.nodeIcon} ${styles[statusAccentClass(cell.status)]}`} aria-hidden="true">
          <Server />
        </span>
        <div className={styles.cellTitle}>
          <strong>{cell.name}</strong>
          <span>{cell.endpoint}</span>
        </div>
        <ResourceStatus tone={statusTone(cell.status)}>{statusLabel(cell.status)}</ResourceStatus>
      </div>
      <dl className={styles.meta}>
        <TopologyFact icon={Activity} label={cell.workloadLabel} value={cell.workloadValue} />
        <TopologyFact icon={Gauge} label="Queue" value={String(cell.queueDepth)} />
        <TopologyFact icon={Server} label="Workers" value={cell.workers} />
      </dl>
    </>
  );

  if (!onSelect) {
    return <div className={`${styles.cellNode} ${styles[statusBorderClass(cell.status)]}`}>{content}</div>;
  }

  return (
    <button
      aria-label={`Inspect ${cell.name}`}
      className={`${styles.cellNode} ${styles[statusBorderClass(cell.status)]}`}
      onClick={() => onSelect(cell.id)}
      type="button"
    >
      {content}
    </button>
  );
}

function TopologyFact({ icon: Icon, label, value }: { icon: LucideIcon; label: string; value: string }) {
  return (
    <div className={styles.fact}>
      <span className={styles.factIcon} aria-hidden="true">
        <Icon />
      </span>
      <dt>{label}</dt>
      <dd>{value}</dd>
    </div>
  );
}

function statusLabel(status: ExecutionTopologyStatus) {
  switch (status) {
    case "healthy":
      return "Healthy";
    case "degraded":
      return "Degraded";
    case "offline":
      return "Offline";
  }
}

function statusTone(status: ExecutionTopologyStatus): ResourceStatusTone {
  switch (status) {
    case "healthy":
      return "success";
    case "degraded":
      return "warning";
    case "offline":
      return "danger";
  }
}

function statusAccentClass(status: ExecutionTopologyStatus) {
  switch (status) {
    case "healthy":
      return "nodeIconSuccess";
    case "degraded":
      return "nodeIconWarning";
    case "offline":
      return "nodeIconDanger";
  }
}

function statusBorderClass(status: ExecutionTopologyStatus) {
  switch (status) {
    case "healthy":
      return "cellNodeSuccess";
    case "degraded":
      return "cellNodeWarning";
    case "offline":
      return "cellNodeDanger";
  }
}
