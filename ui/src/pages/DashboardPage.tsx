import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import { ProgressMeter } from "../components/ProgressMeter";
import { SectionPanel } from "../components/SectionPanel";
import { SignalList } from "../components/SignalList";
import type { MockCell } from "../mocks/consoleData";
import type { DashboardMetric } from "../mocks/fixtures";

type DashboardPageProps = {
  cell: MockCell;
};

export function DashboardPage({ cell }: DashboardPageProps) {
  return (
    <>
      <PageHeader
        description={`Cell-wide control-plane health for ${cell.endpoint}.`}
        eyebrow={cell.region}
        title={`${cell.name} dashboard`}
      />
      <div className="metric-card-grid">
        {cellDashboardMetrics(cell).map((metric) => (
          <MetricCard
            detail={metric.detail}
            key={metric.id}
            label={metric.label}
            tone={metric.tone}
            value={metric.value}
          />
        ))}
      </div>
      <div className="dashboard-layout">
        <SectionPanel description={cell.detail} title="Components">
          <SignalList signals={cell.components} />
        </SectionPanel>
        <div className="dashboard-side-stack">
          <SectionPanel description={`${cell.activeRuns} active runs, ${cell.queueDepth} queued.`} title="Workload">
            <div className="progress-meter-stack">
              {cell.progress.map((progress) => (
                <ProgressMeter
                  detail={progress.detail}
                  key={progress.id}
                  label={progress.label}
                  tone={progress.tone}
                  value={progress.value}
                />
              ))}
            </div>
          </SectionPanel>
        </div>
      </div>
    </>
  );
}

function cellDashboardMetrics(cell: MockCell): DashboardMetric[] {
  return [
    {
      id: "status",
      label: "Status",
      value: statusLabel(cell.status),
      detail: cell.detail,
      tone: cell.status === "healthy" ? "success" : "attention"
    },
    {
      id: "active-runs",
      label: "Active runs",
      value: String(cell.activeRuns),
      detail: "Cell-local execution",
      tone: "neutral"
    },
    {
      id: "queue-depth",
      label: "Queued",
      value: String(cell.queueDepth),
      detail: "Waiting in this cell",
      tone: cell.queueDepth > 5 ? "attention" : "neutral"
    },
    {
      id: "workers",
      label: "Workers",
      value: `${cell.workersOnline}/${cell.workersTotal}`,
      detail: "Online workers",
      tone: cell.workersOnline < cell.workersTotal ? "attention" : "success"
    }
  ];
}

function statusLabel(status: MockCell["status"]) {
  switch (status) {
    case "healthy":
      return "Healthy";
    case "degraded":
      return "Degraded";
    case "offline":
      return "Offline";
  }
}
