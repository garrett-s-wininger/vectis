import type { RunStatus } from "../components/StatusBadge";
import type { RunListItem } from "../components/RunList";
import type { SignalItem } from "../components/SignalList";
import type { DashboardMetric, ProgressFixture } from "./fixtures";
import { activeRuns, instanceSignals, workloadProgress } from "./fixtures";

export type MockCellStatus = "healthy" | "degraded" | "offline";

export type MockCell = {
  id: string;
  name: string;
  endpoint: string;
  region: string;
  status: MockCellStatus;
  detail: string;
  activeRuns: number;
  queueDepth: number;
  workersOnline: number;
  workersTotal: number;
  components: SignalItem[];
  progress: ProgressFixture[];
};

export type MockJobStatus = "enabled" | "paused";

export type MockJob = {
  id: string;
  name: string;
  repository: string;
  branch: string;
  namespacePath: string;
  schedule: string;
  nextRun: string;
  lastRunStatus: RunStatus;
  status: MockJobStatus;
};

export type MockNamespaceRole = "Admin" | "Operator" | "Viewer";

export type MockNamespace = {
  id: number;
  name: string;
  parentID?: number;
  path: string;
  breakInheritance: boolean;
  role: MockNamespaceRole;
};

export type MockUserRole = "Admin" | "Operator" | "Viewer";
export type MockUserStatus = "active" | "disabled";

export type MockUser = {
  id: string;
  username: string;
  role: MockUserRole;
  status: MockUserStatus;
  lastSeen: string;
  tokens: number;
};

export type NewMockUser = {
  username: string;
  role: MockUserRole;
};

export type NewMockNamespace = {
  name: string;
  parentID: number;
};

export type MockConsoleData = {
  cells: MockCell[];
  jobs: MockJob[];
  namespaces: MockNamespace[];
  progress: ProgressFixture[];
  runs: RunListItem[];
  signals: SignalItem[];
  users: MockUser[];
};

const jobs: MockJob[] = [
  {
    id: "job-api-test-suite",
    name: "api-test-suite",
    repository: "github.com/vectis/api",
    branch: "main",
    namespacePath: "/team-a",
    schedule: "On push",
    nextRun: "Waiting for push",
    lastRunStatus: "running",
    status: "enabled"
  },
  {
    id: "job-docs-publish",
    name: "docs-publish",
    repository: "github.com/vectis/docs",
    branch: "main",
    namespacePath: "/team-a/edge",
    schedule: "Hourly",
    nextRun: "18m",
    lastRunStatus: "queued",
    status: "enabled"
  },
  {
    id: "job-worker-image",
    name: "worker-image",
    repository: "github.com/vectis/worker",
    branch: "release",
    namespacePath: "/prod",
    schedule: "Nightly",
    nextRun: "7h 12m",
    lastRunStatus: "succeeded",
    status: "paused"
  }
];

const cells: MockCell[] = [
  {
    id: "cell-local",
    name: "local",
    endpoint: "https://local.vectis.internal",
    region: "dev",
    status: "healthy",
    detail: "All local control-plane components reporting",
    activeRuns: 4,
    queueDepth: 2,
    workersOnline: 4,
    workersTotal: 4,
    components: [
      { id: "local-api", label: "API", detail: "12 ms p95", state: "healthy" },
      { id: "local-queue", label: "Queue", detail: "Depth 2", state: "healthy" },
      { id: "local-registry", label: "Registry", detail: "4 workers", state: "healthy" },
      { id: "local-logs", label: "Logs", detail: "Lag 4s", state: "healthy" },
      { id: "local-cron", label: "Cron", detail: "Next tick 42s", state: "healthy" },
      { id: "local-reconciler", label: "Reconciler", detail: "No stuck runs", state: "healthy" }
    ],
    progress: [
      { id: "local-queue", label: "Queue pressure", value: 18, detail: "2 waiting" },
      { id: "local-workers", label: "Worker utilization", value: 61, detail: "4 of 4 online" },
      { id: "local-logs", label: "Log pipeline", value: 8, detail: "4s lag" }
    ]
  },
  {
    id: "cell-edge",
    name: "edge",
    endpoint: "https://edge.vectis.internal",
    region: "iad",
    status: "degraded",
    detail: "Log service lagging behind worker output",
    activeRuns: 6,
    queueDepth: 9,
    workersOnline: 5,
    workersTotal: 6,
    components: [
      { id: "edge-api", label: "API", detail: "24 ms p95", state: "healthy" },
      { id: "edge-queue", label: "Queue", detail: "Depth 9", state: "degraded" },
      { id: "edge-registry", label: "Registry", detail: "5 workers", state: "healthy" },
      { id: "edge-logs", label: "Logs", detail: "Lag 2m 14s", state: "degraded" },
      { id: "edge-cron", label: "Cron", detail: "Running", state: "healthy" },
      { id: "edge-reconciler", label: "Reconciler", detail: "1 run under review", state: "degraded" }
    ],
    progress: [
      { id: "edge-queue", label: "Queue pressure", value: 72, detail: "9 waiting", tone: "warning" },
      { id: "edge-workers", label: "Worker utilization", value: 83, detail: "5 of 6 online", tone: "warning" },
      { id: "edge-logs", label: "Log pipeline", value: 88, detail: "2m 14s lag", tone: "critical" }
    ]
  },
  {
    id: "cell-prod-west",
    name: "prod-west",
    endpoint: "https://prod-west.vectis.internal",
    region: "pdx",
    status: "offline",
    detail: "Gateway cannot reach the cell API",
    activeRuns: 0,
    queueDepth: 0,
    workersOnline: 0,
    workersTotal: 8,
    components: [
      { id: "west-api", label: "API", detail: "Gateway timeout", state: "offline" },
      { id: "west-queue", label: "Queue", detail: "Unknown", state: "unknown" },
      { id: "west-registry", label: "Registry", detail: "Unknown", state: "unknown" },
      { id: "west-logs", label: "Logs", detail: "Unknown", state: "unknown" },
      { id: "west-cron", label: "Cron", detail: "Unknown", state: "unknown" },
      { id: "west-reconciler", label: "Reconciler", detail: "Unknown", state: "unknown" }
    ],
    progress: [
      { id: "west-reachability", label: "Reachability", value: 100, detail: "Unavailable", tone: "critical" },
      { id: "west-workers", label: "Worker availability", value: 100, detail: "0 of 8 online", tone: "critical" }
    ]
  }
];

const namespaces: MockNamespace[] = [
  {
    id: 1,
    name: "/",
    path: "/",
    breakInheritance: false,
    role: "Admin"
  },
  {
    id: 2,
    name: "team-a",
    parentID: 1,
    path: "/team-a",
    breakInheritance: false,
    role: "Admin"
  },
  {
    id: 3,
    name: "edge",
    parentID: 2,
    path: "/team-a/edge",
    breakInheritance: true,
    role: "Operator"
  },
  {
    id: 4,
    name: "prod",
    parentID: 1,
    path: "/prod",
    breakInheritance: false,
    role: "Viewer"
  }
];

const users: MockUser[] = [
  {
    id: "user-admin",
    username: "admin",
    role: "Admin",
    status: "active",
    lastSeen: "2m ago",
    tokens: 2
  },
  {
    id: "user-mira",
    username: "mira",
    role: "Operator",
    status: "active",
    lastSeen: "34m ago",
    tokens: 1
  },
  {
    id: "user-lee",
    username: "lee",
    role: "Viewer",
    status: "disabled",
    lastSeen: "3d ago",
    tokens: 0
  }
];

export async function loadMockConsoleData(): Promise<MockConsoleData> {
  return cloneData({
    cells,
    jobs,
    namespaces,
    progress: workloadProgress,
    runs: activeRuns,
    signals: instanceSignals,
    users
  });
}

export function clusterHealthMetricsFor(cells: MockCell[]): DashboardMetric[] {
  const healthy = cells.filter((cell) => cell.status === "healthy").length;
  const degraded = cells.filter((cell) => cell.status === "degraded").length;
  const offline = cells.filter((cell) => cell.status === "offline").length;
  const activeRuns = cells.reduce((total, cell) => total + cell.activeRuns, 0);
  const queueDepth = cells.reduce((total, cell) => total + cell.queueDepth, 0);

  return [
    {
      id: "cells",
      label: "Cells",
      value: String(cells.length),
      detail: `${healthy} healthy, ${degraded} degraded, ${offline} offline`,
      tone: offline > 0 || degraded > 0 ? "attention" : "success"
    },
    {
      id: "active-runs",
      label: "Active runs",
      value: String(activeRuns),
      detail: "Across all reachable cells"
    },
    {
      id: "queue-depth",
      label: "Queued",
      value: String(queueDepth),
      detail: "Across all reachable cells",
      tone: queueDepth > 5 ? "attention" : "neutral"
    },
    {
      id: "offline",
      label: "Offline cells",
      value: String(offline),
      detail: offline === 1 ? "1 cell unreachable" : `${offline} cells unreachable`,
      tone: offline > 0 ? "attention" : "success"
    }
  ];
}

export function dashboardMetricsFor(data: MockConsoleData): DashboardMetric[] {
  const running = data.runs.filter((run) => run.status === "running").length;
  const queued = data.runs.filter((run) => run.status === "queued").length;
  const succeeded = data.runs.filter((run) => run.status === "succeeded").length;
  const failed = data.runs.filter((run) => run.status === "failed").length;

  return [
    {
      id: "running",
      label: "Running",
      value: String(running),
      detail: `${queued} queued behind`,
      tone: running > 0 ? "attention" : "neutral"
    },
    {
      id: "succeeded",
      label: "Succeeded",
      value: String(succeeded),
      detail: "Recent completed runs",
      tone: "success"
    },
    {
      id: "failed",
      label: "Failed",
      value: String(failed),
      detail: failed === 1 ? "1 run needs review" : `${failed} runs need review`,
      tone: failed > 0 ? "attention" : "neutral"
    },
    {
      id: "jobs",
      label: "Jobs",
      value: String(data.jobs.length),
      detail: "In selected namespace"
    }
  ];
}

export function scopeMockConsoleData(
  data: MockConsoleData,
  namespacePath: string
): MockConsoleData {
  return {
    ...data,
    jobs: data.jobs.filter((job) =>
      namespaceContains(namespacePath, job.namespacePath)
    ),
    runs: data.runs.filter(
      (run) =>
        run.namespacePath && namespaceContains(namespacePath, run.namespacePath)
    )
  };
}

export function createMockNamespace(
  data: MockConsoleData,
  input: NewMockNamespace
): MockConsoleData {
  const name = input.name.trim();
  if (!name) {
    return data;
  }

  const parent = data.namespaces.find(
    (namespace) => namespace.id === input.parentID
  );

  if (!parent) {
    return data;
  }

  const path = parent.path === "/" ? `/${name}` : `${parent.path}/${name}`;
  if (data.namespaces.some((namespace) => namespace.path === path)) {
    return data;
  }

  const id = Math.max(0, ...data.namespaces.map((namespace) => namespace.id)) + 1;
  const namespace: MockNamespace = {
    id,
    name,
    parentID: parent.id,
    path,
    breakInheritance: false,
    role: parent.role
  };

  return {
    ...data,
    namespaces: [...data.namespaces, namespace].sort((a, b) =>
      a.path.localeCompare(b.path)
    )
  };
}

export function deleteMockNamespace(
  data: MockConsoleData,
  namespaceID: number
): MockConsoleData {
  if (!canDeleteMockNamespace(data, namespaceID)) {
    return data;
  }

  return {
    ...data,
    namespaces: data.namespaces.filter(
      (namespace) => namespace.id !== namespaceID
    )
  };
}

export function canDeleteMockNamespace(
  data: MockConsoleData,
  namespaceID: number
) {
  if (namespaceID === 1) {
    return false;
  }

  return (
    !data.namespaces.some((namespace) => namespace.parentID === namespaceID) &&
    !data.jobs.some((job) => {
      const namespace = data.namespaces.find(
        (candidate) => candidate.id === namespaceID
      );
      return namespace ? job.namespacePath === namespace.path : false;
    })
  );
}

export function createMockUser(
  data: MockConsoleData,
  input: NewMockUser
): MockConsoleData {
  const username = input.username.trim();
  if (!username) {
    return data;
  }

  const user: MockUser = {
    id: `user-${username.toLowerCase().replace(/[^a-z0-9]+/g, "-")}`,
    username,
    role: input.role,
    status: "active",
    lastSeen: "Never",
    tokens: 0
  };

  return {
    ...data,
    users: [user, ...data.users]
  };
}

export function updateMockUserStatus(
  data: MockConsoleData,
  userID: string,
  status: MockUserStatus
): MockConsoleData {
  return {
    ...data,
    users: data.users.map((user) =>
      user.id === userID ? { ...user, status } : user
    )
  };
}

export function deleteMockUser(
  data: MockConsoleData,
  userID: string
): MockConsoleData {
  return {
    ...data,
    users: data.users.filter((user) => user.id !== userID)
  };
}

export function triggerMockRun(
  data: MockConsoleData,
  jobID: string
): MockConsoleData {
  const job = data.jobs.find((candidate) => candidate.id === jobID);
  if (!job) {
    return data;
  }

  const nextRunNumber =
    Math.max(0, ...data.runs.map((run) => run.runNumber)) + 1;
  const run: RunListItem = {
    id: `run-${nextRunNumber}`,
    jobName: job.name,
    runNumber: nextRunNumber,
    commit: "manual",
    duration: "Queued",
    namespacePath: job.namespacePath,
    status: "queued"
  };

  return {
    ...data,
    jobs: data.jobs.map((candidate) =>
      candidate.id === jobID
        ? { ...candidate, lastRunStatus: "queued", nextRun: "Queued" }
        : candidate
    ),
    runs: [run, ...data.runs]
  };
}

function cloneData(data: MockConsoleData): MockConsoleData {
  return {
    cells: data.cells.map((cell) => ({
      ...cell,
      components: cell.components.map((component) => ({ ...component })),
      progress: cell.progress.map((progress) => ({ ...progress }))
    })),
    jobs: data.jobs.map((job) => ({ ...job })),
    namespaces: data.namespaces.map((namespace) => ({ ...namespace })),
    progress: data.progress.map((progress) => ({ ...progress })),
    runs: data.runs.map((run) => ({ ...run })),
    signals: data.signals.map((signal) => ({ ...signal })),
    users: data.users.map((user) => ({ ...user }))
  };
}

function namespaceContains(parentPath: string, childPath: string) {
  return (
    parentPath === "/" ||
    childPath === parentPath ||
    childPath.startsWith(`${parentPath}/`)
  );
}
