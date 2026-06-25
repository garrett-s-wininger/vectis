import { Activity, Gauge, Network, Server } from "lucide-react";
import {
  MetricCard,
  OperationalFact,
  PageHeader,
  RecordList,
  RecordListIdentity,
  RecordListMeta,
  RecordListSummary,
  type RecordListRailTone
} from "../components";
import type { Cell } from "../domain/console";
import { cellStatusLabel, cellStatusTone } from "../domain/consoleOptions";
import { clusterHealthMetricsFor } from "../mocks/consoleData";
import { ResourceStatus } from "./shared";
import styles from "./HealthPage.module.css";

type HealthPageProps = {
  cells: Cell[];
  onSelectCell: (cellID: string) => void;
};

export function HealthPage({ cells, onSelectCell }: HealthPageProps) {
  const metrics = clusterHealthMetricsFor(cells);

  return (
    <>
      <PageHeader
        description="Gateway-visible cells and their local control-plane health."
        eyebrow="Cluster"
        title="Health"
      />
      <div className={styles.metrics}>
        {metrics.map((metric) => (
          <MetricCard
            detail={metric.detail}
            key={metric.id}
            label={metric.label}
            tone={metric.tone}
            value={metric.value}
          />
        ))}
      </div>
      <RecordList
        countLabel={cellCountLabel(cells.length)}
        description="Gateway-visible execution topology"
        emptyMessage="No cells registered."
        items={cells.map((cell) => ({
          actions: <ResourceStatus tone={cellStatusTone(cell.status)}>{cellStatusLabel(cell.status)}</ResourceStatus>,
          ariaLabel: `Inspect ${cell.name}`,
          content: (
            <RecordListSummary>
              <RecordListIdentity subtitle={cell.region} title={cell.name} />
              <RecordListMeta featuredFirst>
                <OperationalFact emphasis icon={Network} label="Endpoint" value={cell.endpoint} />
                <OperationalFact icon={Activity} label="Runs" value={String(cell.activeRuns)} />
                <OperationalFact icon={Gauge} label="Queue" value={String(cell.queueDepth)} />
                <OperationalFact icon={Server} label="Workers" value={`${cell.workersOnline}/${cell.workersTotal}`} />
              </RecordListMeta>
            </RecordListSummary>
          ),
          key: cell.id,
          onSelect: () => onSelectCell(cell.id),
          railTone: cellRailTone(cell.status)
        }))}
        title="Cells"
      />
    </>
  );
}

function cellCountLabel(count: number) {
  return count === 1 ? "1 cell" : `${count} cells`;
}

function cellRailTone(status: Cell["status"]): RecordListRailTone {
  switch (status) {
    case "healthy":
      return "success";
    case "degraded":
      return "warning";
    case "offline":
      return "danger";
  }
}
