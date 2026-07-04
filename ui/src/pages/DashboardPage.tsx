import { Activity, Globe2, Server } from "lucide-react";
import { BreadcrumbTrail } from "../components";
import { MetricCard } from "../components";
import { OperationalFact } from "../components";
import { PageHeader } from "../components";
import { ProgressMeter } from "../components";
import { ResourceStatus } from "../components";
import { SectionPanel } from "../components";
import { SignalList } from "../components";
import type { Cell } from "../domain/console";
import { cellStatusLabel, cellStatusTone } from "../domain/consoleOptions";
import type { DashboardMetric } from "../mocks/fixtures";
import styles from "./DashboardPage.module.css";

type DashboardPageProps = {
  cell: Cell;
  onOpenHealth?: () => void;
};

export function DashboardPage({ cell, onOpenHealth }: DashboardPageProps) {
  return (
    <>
      <PageHeader
        description="How this cell is reached and whether it can accept work."
        navigation={
          onOpenHealth ? (
            <BreadcrumbTrail
              items={[
                { label: "Cluster" },
                { label: "Health", onClick: onOpenHealth },
                { label: cell.name, current: true }
              ]}
              label="Cell location"
            />
          ) : undefined
        }
        title={cell.name}
      />
      <section
        className={`${styles.summary} polished-panel polished-panel--accent-top`}
        aria-labelledby="cell-summary-title"
      >
        <div className={styles.summaryHeader}>
          <div>
            <h2 id="cell-summary-title">Cell Summary</h2>
            <p>Reachability, identity, and capacity.</p>
          </div>
          <ResourceStatus tone={cellStatusTone(cell.status)}>{cellStatusLabel(cell.status)}</ResourceStatus>
        </div>
        <dl className={styles.facts}>
          <OperationalFact emphasis icon={Globe2} label={cellEndpointLabel(cell)} value={cell.endpoint} />
          <OperationalFact icon={Server} label={cellIdentityLabel(cell)} value={cellIdentityValue(cell)} />
          <OperationalFact icon={Activity} label="Capacity" value={workerCapacityLabel(cell)} />
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
        <SectionPanel description={workloadDescription(cell)} title="Workload">
          {cell.progress.length > 0 ? (
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
          ) : (
            <p className={styles.emptyState}>No workload telemetry reported.</p>
          )}
        </SectionPanel>
      </div>
    </>
  );
}

function cellDashboardMetrics(cell: Cell): DashboardMetric[] {
  const workloadValue = cell.stuckRuns ?? cell.activeRuns;
  const reportsStuckRuns = cell.stuckRuns !== undefined;

  const metrics: DashboardMetric[] = [
    {
      id: "active-runs",
      label: reportsStuckRuns ? "Stuck runs" : "Active runs",
      value: String(workloadValue),
      detail: reportsStuckRuns ? "Awaiting dispatch recovery" : "Cell-local execution",
      tone: workloadValue > 0 && reportsStuckRuns ? "attention" : "neutral"
    },
    {
      id: "queue-depth",
      label: "Queued",
      value: String(cell.queueDepth),
      detail: "Waiting in this cell",
      tone: cell.queueDepth > 5 ? "attention" : "neutral"
    }
  ];

  if (cell.workersTotal > 0) {
    metrics.push({
      id: "workers",
      label: "Workers",
      value: workerCapacityShortLabel(cell),
      detail: "Online workers",
      tone: cell.workersOnline < cell.workersTotal ? "attention" : "success"
    });
  }

  return metrics;
}

function workloadDescription(cell: Cell) {
  if (cell.stuckRuns !== undefined) {
    return `${cell.stuckRuns} stuck, ${cell.queueDepth} queued.`;
  }

  return `${cell.activeRuns} active runs, ${cell.queueDepth} queued.`;
}

function workerCapacityLabel(cell: Cell) {
  return cell.workersTotal > 0 ? `${cell.workersOnline}/${cell.workersTotal} workers online` : "N/A";
}

function workerCapacityShortLabel(cell: Cell) {
  return cell.workersTotal > 0 ? `${cell.workersOnline}/${cell.workersTotal}` : "N/A";
}

function cellEndpointLabel(cell: Cell) {
  return cell.stuckRuns === undefined ? "Endpoint" : "Ingress";
}

function cellIdentityLabel(cell: Cell) {
  return cell.stuckRuns === undefined ? "Region" : "Name";
}

function cellIdentityValue(cell: Cell) {
  return cell.stuckRuns === undefined ? cell.region : cell.name;
}
