import { BreadcrumbTrail, ExecutionTopology, MetricCard, PageHeader } from "../components";
import type { Cell } from "../domain/console";
import { clusterHealthMetricsFor } from "../mocks/consoleData";
import styles from "./HealthPage.module.css";

type HealthPageProps = {
  cells: Cell[];
  loading?: boolean;
  onSelectCell: (cellID: string) => void;
};

export function HealthPage({ cells, loading = false, onSelectCell }: HealthPageProps) {
  const metrics = clusterHealthMetricsFor(cells);

  return (
    <>
      <PageHeader
        description="Execution cells and their status."
        navigation={
          <BreadcrumbTrail items={[{ label: "Cluster" }, { current: true, label: "Health" }]} label="Health location" />
        }
        title="Health"
      />
      <div className={styles.healthLayout} aria-busy={loading} aria-label="Cluster health" role="region">
        <CellStatusDistribution cells={cells} loading={loading} />
        <div className={styles.secondaryMetrics}>
          {metrics.map((metric) => (
            <MetricCard
              detail={loading ? "Awaiting cell status" : metric.detail}
              key={metric.id}
              label={metric.label}
              tone={loading ? "neutral" : metric.tone}
              value={loading ? "—" : metric.value}
            />
          ))}
        </div>
        <div className={styles.cellList}>
          <ExecutionTopology
            cells={topologyCellsFor(cells)}
            countLabel={loading ? "Loading…" : cellCountLabel(cells.length)}
            emptyMessage={loading ? "Loading cell status…" : "No cells registered."}
            onSelectCell={onSelectCell}
          />
        </div>
      </div>
    </>
  );
}

function CellStatusDistribution({ cells, loading }: { cells: Cell[]; loading: boolean }) {
  const counts = cellStatusCounts(cells);
  const total = cells.length;
  const summary = cellStatusSummary(counts);

  return (
    <article className={`${styles.statusMetric} polished-panel polished-panel--accent-top`}>
      <span className={styles.statusLabel}>Cell Status</span>
      <strong className={styles.statusValue}>{loading ? "—" : total}</strong>
      <div
        aria-label={loading ? "Cell status distribution loading" : `Cell status distribution: ${summary}`}
        className={styles.statusBar}
        role="img"
      >
        {statusSegments.map((segment) => {
          const count = counts[segment.status];
          const width = total > 0 ? `${(count / total) * 100}%` : "0%";

          return count > 0 ? (
            <span
              className={`${styles.statusSegment} ${styles[segment.className]}`}
              key={segment.status}
              style={{ width }}
            />
          ) : null;
        })}
      </div>
      <div className={styles.statusLegend}>
        {statusSegments.map((segment) => (
          <span className={styles.legendItem} key={segment.status}>
            <span className={`${styles.legendMarker} ${styles[segment.className]}`} />
            {loading ? "—" : counts[segment.status]} {segment.label}
          </span>
        ))}
      </div>
    </article>
  );
}

function cellCountLabel(count: number) {
  return count === 1 ? "1 cell" : `${count} cells`;
}

const statusSegments = [
  { className: "healthySegment", label: "healthy", status: "healthy" },
  { className: "degradedSegment", label: "degraded", status: "degraded" },
  { className: "offlineSegment", label: "offline", status: "offline" }
] as const;

function cellStatusCounts(cells: Cell[]) {
  return cells.reduce(
    (counts, cell) => ({
      ...counts,
      [cell.status]: counts[cell.status] + 1
    }),
    { degraded: 0, healthy: 0, offline: 0 } satisfies Record<Cell["status"], number>
  );
}

function cellStatusSummary(counts: Record<Cell["status"], number>) {
  return `${counts.healthy} healthy, ${counts.degraded} degraded, ${counts.offline} offline`;
}

function cellWorkloadLabel(cell: Cell) {
  return cell.stuckRuns === undefined ? "Runs" : "Stuck";
}

function cellWorkloadValue(cell: Cell) {
  return cell.stuckRuns ?? cell.activeRuns;
}

function workerCapacityLabel(cell: Cell) {
  return cell.workersTotal > 0 ? `${cell.workersOnline}/${cell.workersTotal}` : "N/A";
}

function topologyCellsFor(cells: Cell[]) {
  return cells.map((cell) => ({
    endpoint: cell.endpoint,
    id: cell.id,
    name: cell.name,
    queueDepth: cell.queueDepth,
    status: cell.status,
    workloadLabel: cellWorkloadLabel(cell),
    workloadValue: String(cellWorkloadValue(cell)),
    workers: workerCapacityLabel(cell)
  }));
}
