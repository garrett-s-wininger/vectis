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
  schedule: string;
  nextRun: string;
  lastRunStatus: RunStatus;
  status: MockJobStatus;
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

export type MockConsoleData = {
  jobs: MockJob[];
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
    schedule: "Nightly",
    nextRun: "7h 12m",
    lastRunStatus: "succeeded",
    status: "paused"
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
  const activeUsers = data.users.filter((user) => user.status === "active").length;

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
      id: "users",
      label: "Active users",
      value: String(activeUsers),
      detail: `${data.users.length} total accounts`
    }
  ];
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
    progress: data.progress.map((progress) => ({ ...progress })),
    runs: data.runs.map((run) => ({ ...run })),
    signals: data.signals.map((signal) => ({ ...signal })),
    users: data.users.map((user) => ({ ...user }))
  };
}
