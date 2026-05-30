import type { RunListItem, RunStatus, SignalItem } from "../components";
import type { ProgressFixture } from "../mocks/fixtures";

export type CellStatus = "healthy" | "degraded" | "offline";

export type Cell = {
  id: string;
  name: string;
  endpoint: string;
  region: string;
  status: CellStatus;
  detail: string;
  activeRuns: number;
  queueDepth: number;
  workersOnline: number;
  workersTotal: number;
  components: SignalItem[];
  progress: ProgressFixture[];
};

export type JobStatus = "enabled" | "paused";

export type Job = {
  id: string;
  name: string;
  repository: string;
  branch: string;
  definition?: string;
  namespacePath: string;
  schedule: string;
  nextRun: string;
  lastRunStatus: RunStatus;
  status: JobStatus;
};

export type NewJob = {
  branch: string;
  definition: string;
  name: string;
  namespacePath: string;
  repository: string;
  schedule: string;
  status: JobStatus;
};

export type UpdateJob = Omit<NewJob, "namespacePath">;

export type NamespaceRole = "Admin" | "Operator" | "Viewer";

export type Namespace = {
  id: number;
  name: string;
  parentID?: number;
  path: string;
  breakInheritance: boolean;
  role: NamespaceRole;
};

export type UserRole = "Admin" | "Operator" | "Viewer";
export type UserStatus = "active" | "disabled";

export type User = {
  id: string;
  username: string;
  role: UserRole;
  status: UserStatus;
  lastSeen: string;
  tokens: number;
};

export type NewUser = {
  username: string;
  role: UserRole;
};

export type NewNamespace = {
  name: string;
  parentID: number;
};

export type NewEphemeralRun = {
  definition: string;
  namespacePath: string;
  submittedBy?: string;
};

export type ConsoleData = {
  cells: Cell[];
  jobs: Job[];
  namespaces: Namespace[];
  progress: ProgressFixture[];
  runs: RunListItem[];
  signals: SignalItem[];
  users: User[];
};
