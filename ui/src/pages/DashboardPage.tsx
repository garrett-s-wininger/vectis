import { Activity, Globe2, Server } from "lucide-react";
import { BreadcrumbTrail } from "../components";
import { MetricCard } from "../components";
import { OperationalFact } from "../components";
import { PageHeader } from "../components";
import { ProgressMeter } from "../components";
import { SectionPanel } from "../components";
import { SignalList } from "../components";
import type { Cell } from "../domain/console";
import { cellStatusLabel, cellStatusTone } from "../domain/consoleOptions";
import type { DashboardMetric } from "../mocks/fixtures";
import { ResourceStatus } from "./shared";
import styles from "./DashboardPage.module.css";

type DashboardPageProps = {
  cell: Cell;
  onOpenHealth?: () => void;
};

export function DashboardPage({ cell, onOpenHealth }: DashboardPageProps) {
  return (
    <>
      <PageHeader
        description={cell.detail}
        navigation={
          onOpenHealth ? (
            <BreadcrumbTrail
              items={[{ label: "Health", onClick: onOpenHealth }, { label: cell.name, current: true }]}
              label="Health breadcrumbs"
            />
          ) : undefined
        }
        title={cell.name}
      />
      <section className={`${styles.summary} polished-panel polished-panel--accent-top`} aria-labelledby="cell-summary-title">
        <div className={styles.summaryHeader}>
          <div>
            <h2 id="cell-summary-title">Cell Summary</h2>
            <p>Gateway-visible execution cell</p>
          </div>
          <ResourceStatus tone={cellStatusTone(cell.status)}>{cellStatusLabel(cell.status)}</ResourceStatus>
        </div>
        <dl className={styles.facts}>
          <OperationalFact emphasis icon={Globe2} label="Endpoint" value={cell.endpoint} />
          <OperationalFact icon={Server} label="Region" value={cell.region} />
          <OperationalFact icon={Activity} label="Capacity" value={`${cell.workersOnline}/${cell.workersTotal} workers online`} />
        </dl>
        <div className={styles.metrics}>
          {cellDashboardMetrics(cell).map((metric) => (
            <MetricCard
              detail={metric.detail}
              key={metric.id}
              label={metric.label}
              tone={metric.tone}
              value={metric.value}
              variant="plain"
            />
          ))}
        </div>
      </section>
      <div className={styles.layout}>
        <SectionPanel description={cell.detail} title="Components">
          <SignalList signals={cell.components} variant="stretch" />
        </SectionPanel>
        <SectionPanel description={`${cell.activeRuns} active runs, ${cell.queueDepth} queued.`} title="Workload">
          <div className={styles.progressStack}>
            {cell.progress.map((progress) => (
              <ProgressMeter
                detail={progress.detail}
                key={progress.id}
                label={progress.label}
                tone={progress.tone}
                value={progress.value}
                variant="card"
              />
            ))}
          </div>
        </SectionPanel>
      </div>
    </>
  );
}

function cellDashboardMetrics(cell: Cell): DashboardMetric[] {
  return [
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
