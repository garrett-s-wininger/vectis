import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { MetricCard } from "../components";
import { PageHeader } from "../components";
import type { Cell, CellStatus } from "../domain/console";
import { clusterHealthMetricsFor } from "../mocks/consoleData";
import { DashboardPage } from "./DashboardPage";
import { ResourceStatus, ResourceTitle } from "./shared";

type HealthPageProps = {
  cells: Cell[];
  onSelectCell: (cellID: string) => void;
  selectedCellID?: string;
};

export function HealthPage({ cells, onSelectCell, selectedCellID }: HealthPageProps) {
  const selectedCell = cells.find((cell) => cell.id === selectedCellID);
  const metrics = clusterHealthMetricsFor(cells);
  const columns: DataTableColumn<Cell>[] = [
    {
      header: "Cell",
      cell: (cell) => <ResourceTitle subtitle={cell.region} title={cell.name} />
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
      cell: (cell) => <ResourceStatus tone={statusTone(cell.status)}>{statusLabel(cell.status)}</ResourceStatus>
    },
    {
      align: "end",
      header: "Actions",
      cell: (cell) => (
        <Button aria-label={`Inspect ${cell.name}`} onClick={() => onSelectCell(cell.id)}>
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
      <DataTable columns={columns} emptyMessage="No cells loaded." getRowKey={(cell) => cell.id} rows={cells} />
      {selectedCell ? <DashboardPage cell={selectedCell} /> : null}
    </>
  );
}

function statusLabel(status: CellStatus) {
  switch (status) {
    case "healthy":
      return "Healthy";
    case "degraded":
      return "Degraded";
    case "offline":
      return "Offline";
  }
}

function statusTone(status: CellStatus) {
  switch (status) {
    case "healthy":
      return "active";
    case "degraded":
      return "paused";
    case "offline":
      return "disabled";
  }
}
