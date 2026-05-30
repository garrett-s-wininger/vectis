import type { MetricCardProps } from "../components/MetricCard";
import type { ProgressMeterProps } from "../components/ProgressMeter";
import type { RunListItem } from "../components/RunList";
import type { SignalItem } from "../components/SignalList";

export type DashboardMetric = MetricCardProps & {
  id: string;
};

export type ProgressFixture = ProgressMeterProps & {
  id: string;
};

export const dashboardMetrics: DashboardMetric[] = [
  {
    id: "running",
    label: "Running",
    value: "12",
    detail: "3 queued behind",
    tone: "attention"
  },
  {
    id: "succeeded",
    label: "Succeeded today",
    value: "48",
    detail: "94% success rate",
    tone: "success"
  },
  {
    id: "failed",
    label: "Failed today",
    value: "3",
    detail: "2 need review"
  },
  {
    id: "workers",
    label: "Workers",
    value: "8",
    detail: "6 active, 2 idle"
  }
];

export const activeRuns: RunListItem[] = [
  {
    id: "run-1240",
    jobName: "api-test-suite",
    runNumber: 1240,
    cellName: "local",
    commit: "8f12c4a",
    definition: JSON.stringify(
      {
        id: "api-test-suite",
        root: {
          id: "root",
          uses: "builtins/shell",
          with: { command: "go test ./internal/api/..." }
        }
      },
      null,
      2
    ),
    duration: "4m 12s",
    namespacePath: "/team-a",
    source: "stored",
    submittedBy: "mira",
    status: "running"
  },
  {
    id: "run-1239",
    jobName: "docs-publish",
    runNumber: 1239,
    cellName: "edge",
    commit: "1d9a0b3",
    definition: JSON.stringify(
      {
        id: "docs-publish",
        root: {
          id: "root",
          uses: "builtins/shell",
          with: { command: "npm run docs:publish" }
        }
      },
      null,
      2
    ),
    duration: "1m 48s",
    namespacePath: "/team-a/edge",
    source: "stored",
    submittedBy: "lee",
    status: "queued"
  },
  {
    id: "run-1238",
    jobName: "worker-image",
    runNumber: 1238,
    cellName: "prod-west",
    commit: "54fd901",
    definition: JSON.stringify(
      {
        id: "worker-image",
        root: {
          id: "root",
          uses: "builtins/shell",
          with: { command: "podman build -f build/Containerfile" }
        }
      },
      null,
      2
    ),
    duration: "7m 31s",
    namespacePath: "/prod",
    source: "stored",
    submittedBy: "admin",
    status: "succeeded"
  }
];

export const instanceSignals: SignalItem[] = [
  {
    id: "api",
    label: "API",
    detail: "Last heartbeat 12s ago",
    state: "healthy"
  },
  {
    id: "queue",
    label: "Queue",
    detail: "Depth 3",
    state: "healthy"
  },
  {
    id: "logs",
    label: "Logs",
    detail: "Forwarder lag 28s",
    state: "degraded"
  }
];

export const workloadProgress: ProgressFixture[] = [
  {
    id: "queue",
    label: "Queue pressure",
    value: 34,
    detail: "3 waiting, 12 running"
  },
  {
    id: "workers",
    label: "Worker utilization",
    value: 75,
    detail: "6 of 8 active",
    tone: "warning"
  }
];
