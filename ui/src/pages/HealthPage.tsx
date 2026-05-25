import { Button } from "../components/Button";
import { DataTable, type DataTableColumn } from "../components/DataTable";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import {
  clusterHealthMetricsFor,
  type MockCell,
  type MockCellStatus
} from "../mocks/consoleData";
import { DashboardPage } from "./DashboardPage";

type HealthPageProps = {
  cells: MockCell[];
  onSelectCell: (cellID: string) => void;
  selectedCellID?: string;
};

export function HealthPage({
  cells,
  onSelectCell,
  selectedCellID
}: HealthPageProps) {
  const selectedCell = cells.find((cell) => cell.id === selectedCellID);
  const metrics = clusterHealthMetricsFor(cells);
  const columns: DataTableColumn<MockCell>[] = [
    {
      header: "Cell",
      cell: (cell) => (
        <div className="resource-title">
          <strong>{cell.name}</strong>
          <small>{cell.region}</small>
        </div>
      )
    },
    {
      header: "Endpoint",
      cell: (cell) => cell.endpoint
    },
    {
      align: "end",
      header: "Runs",
      cell: (cell) => cell.activeRuns
    },
    {
      align: "end",
      header: "Queue",
      cell: (cell) => cell.queueDepth
    },
    {
      align: "end",
      header: "Workers",
      cell: (cell) => `${cell.workersOnline}/${cell.workersTotal}`
    },
    {
      align: "end",
      header: "Status",
      cell: (cell) => (
        <span className={`resource-status resource-status--${statusTone(cell.status)}`}>
          {statusLabel(cell.status)}
        </span>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (cell) => (
        <Button
          aria-label={`Inspect ${cell.name}`}
          onClick={() => onSelectCell(cell.id)}
        >
          Inspect
        </Button>
      )
    }
  ];

  return (
    <>
      <PageHeader
        description="Gateway-visible cells and their local control-plane health."
        eyebrow="Cluster"
        title="Health"
      />
      <div className="metric-card-grid">
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
      <DataTable
        columns={columns}
        emptyMessage="No cells loaded."
        getRowKey={(cell) => cell.id}
        rows={cells}
      />
      {selectedCell ? <DashboardPage cell={selectedCell} /> : null}
    </>
  );
}

function statusLabel(status: MockCellStatus) {
  switch (status) {
    case "healthy":
      return "Healthy";
    case "degraded":
      return "Degraded";
    case "offline":
      return "Offline";
  }
}

function statusTone(status: MockCellStatus) {
  switch (status) {
    case "healthy":
      return "active";
    case "degraded":
      return "paused";
    case "offline":
      return "disabled";
  }
}
