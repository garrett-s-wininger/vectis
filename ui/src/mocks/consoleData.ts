import type { RunStatus } from "../components/StatusBadge";
import type { RunListItem } from "../components/RunList";
import type { SignalItem } from "../components/SignalList";
import type { DashboardMetric, ProgressFixture } from "./fixtures";
import { activeRuns, instanceSignals, workloadProgress } from "./fixtures";

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
    jobs,
    namespaces,
    progress: workloadProgress,
    runs: activeRuns,
    signals: instanceSignals,
    users
  });
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
