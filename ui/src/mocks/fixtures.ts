import type { MetricCardProps } from "../components";
import type { ProgressMeterProps } from "../components";
import type { RunListItem, RunTaskItem, RunTaskStatus } from "../components";
import type { SignalItem } from "../components";

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
    createdAt: "2026-05-31T12:00:00Z",
    definition: JSON.stringify(
      {
        id: "api-test-suite",
        root: {
          id: "root",
          uses: "builtins/script",
          with: { script: "go test ./internal/api/..." }
        }
      },
      null,
      2
    ),
    duration: "4m 12s",
    namespacePath: "/team-a",
    source: "stored",
    startedAt: "2026-05-31T12:00:05Z",
    submittedBy: "mira",
    tasks: mockRunTasks("run-1240", [
      { key: "root", name: "api-test-suite", status: "running" },
      { key: "checkout", name: "Checkout", parentKey: "root", status: "succeeded" },
      { key: "unit", name: "Unit tests", parentKey: "root", status: "succeeded" },
      { key: "api", name: "API tests", parentKey: "root", status: "running" },
      { key: "api-auth", name: "Auth routes", parentKey: "api", status: "succeeded" },
      { key: "api-jobs", name: "Job routes", parentKey: "api", status: "running" },
      { key: "report", name: "Report", parentKey: "root", status: "planned" }
    ]),
    trigger: "manual",
    status: "running"
  },
  {
    id: "run-1239",
    jobName: "docs-publish",
    runNumber: 1239,
    cellName: "edge",
    commit: "1d9a0b3",
    createdAt: "2026-05-31T11:20:00Z",
    definition: JSON.stringify(
      {
        id: "docs-publish",
        root: {
          id: "root",
          uses: "builtins/script",
          with: { script: "npm run docs:publish" }
        }
      },
      null,
      2
    ),
    duration: "1m 48s",
    namespacePath: "/team-a/edge",
    source: "stored",
    submittedBy: "cron",
    tasks: mockRunTasks("run-1239", [
      { key: "root", name: "docs-publish", status: "pending" },
      { key: "build", name: "Build docs", parentKey: "root", status: "planned" },
      { key: "publish", name: "Publish", parentKey: "root", status: "planned" }
    ]),
    trigger: "schedule",
    status: "queued"
  },
  {
    id: "run-1238",
    jobName: "worker-image",
    runNumber: 1238,
    cellName: "prod-west",
    commit: "54fd901",
    createdAt: "2026-05-31T10:00:00Z",
    definition: JSON.stringify(
      {
        id: "worker-image",
        root: {
          id: "root",
          uses: "builtins/script",
          with: { script: "podman build -f build/Containerfile" }
        }
      },
      null,
      2
    ),
    duration: "7m 31s",
    finishedAt: "2026-05-31T10:07:36Z",
    namespacePath: "/prod",
    source: "stored",
    startedAt: "2026-05-31T10:00:05Z",
    submittedBy: "admin",
    tasks: mockRunTasks("run-1238", [
      { key: "root", name: "worker-image", status: "succeeded" },
      { key: "build", name: "Build image", parentKey: "root", status: "succeeded" },
      { key: "scan", name: "Scan image", parentKey: "root", status: "succeeded" },
      { key: "publish", name: "Publish image", parentKey: "root", status: "succeeded" }
    ]),
    trigger: "manual",
    status: "succeeded"
  }
];

type MockTaskInput = {
  key: string;
  name: string;
  parentKey?: string;
  status: RunTaskStatus;
};

export function mockRunTasks(runID: string, tasks: MockTaskInput[]): RunTaskItem[] {
  return tasks.map((task, index) => ({
    attempts:
      task.status === "planned" || task.status === "pending" ? [] : [mockTaskAttempt(runID, task.key, task.status)],
    name: task.name,
    parentTaskID: task.parentKey ? mockTaskID(runID, task.parentKey) : undefined,
    status: task.status,
    taskID: mockTaskID(runID, task.key),
    taskKey: task.key || String(index + 1)
  }));
}

function mockTaskAttempt(runID: string, taskKey: string, status: RunTaskStatus) {
  const hasStarted = status !== "accepted";
  const hasFinished = ["aborted", "cancelled", "failed", "succeeded"].includes(status);

  return {
    attempt: 1,
    attemptID: `${mockTaskID(runID, taskKey)}:attempt:1`,
    cellID: "local",
    executionID: `${mockTaskID(runID, taskKey)}:execution:1`,
    executionStatus: status,
    finishedAt: hasFinished ? "2026-05-31T12:00:42Z" : undefined,
    startedAt: hasStarted ? "2026-05-31T12:00:05Z" : undefined,
    status
  };
}

function mockTaskID(runID: string, taskKey: string) {
  return `${runID}:${taskKey}`;
}

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
