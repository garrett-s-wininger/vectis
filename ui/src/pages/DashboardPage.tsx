import { dashboardMetricsFor, type MockConsoleData } from "../mocks/consoleData";
import { MetricCard } from "../components/MetricCard";
import { PageHeader } from "../components/PageHeader";
import { ProgressMeter } from "../components/ProgressMeter";
import { RunList } from "../components/RunList";
import { SectionPanel } from "../components/SectionPanel";
import { SignalList } from "../components/SignalList";

type DashboardPageProps = {
  data: MockConsoleData;
  namespacePath: string;
};

export function DashboardPage({ data, namespacePath }: DashboardPageProps) {
  const metrics = dashboardMetricsFor(data);

  return (
    <>
      <PageHeader
        description={`Live activity and instance health for ${namespacePath}.`}
        eyebrow="Console"
        title="Dashboard"
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
      <div className="dashboard-layout">
        <RunList title="Active runs" runs={data.runs.slice(0, 3)} />
        <div className="dashboard-side-stack">
          <SectionPanel title="Instance signals">
            <SignalList signals={data.signals} />
          </SectionPanel>
          <SectionPanel title="Workload">
            <div className="progress-meter-stack">
              {data.progress.map((progress) => (
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
