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
  stuckRuns?: number;
  catalogPending?: number;
  catalogFailed?: number;
  catalogTotal?: number;
  workersOnline: number;
  workersTotal: number;
  components: SignalItem[];
  progress: ProgressFixture[];
};

export type JobStatus = "enabled" | "paused";
export type JobSourceKind = "db" | "repo" | "repo_collection";
export type JobTriggerKind = "manual" | "webhook" | "schedule" | "poll";

export type JobTrigger = {
  detail: string;
  kind: JobTriggerKind;
};

export type Job = {
  id: string;
  name: string;
  description?: string;
  repository: string;
  branch: string;
  sourceDetail: string;
  sourceKind: JobSourceKind;
  definition?: string;
  namespacePath: string;
  schedule: string;
  nextRun: string;
  triggers: JobTrigger[];
  lastRunStatus?: RunStatus;
  status: JobStatus;
};

export type NewJob = {
  branch: string;
  description?: string;
  definition: string;
  manualEnabled?: boolean;
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
  description?: string;
  breakInheritance: boolean;
  role: NamespaceRole;
};

export type AssignableUserRole = "Admin" | "Operator" | "Viewer";
export type RoleBindingRole = AssignableUserRole | "Trigger";
export type UserRole = RoleBindingRole | "Unassigned";
export type UserStatus = "active" | "disabled";

export type RoleBinding = {
  id: string;
  namespaceID: number;
  namespacePath: string;
  role: RoleBindingRole;
  userID: string;
  username?: string;
};

export type User = {
  id: string;
  username: string;
  role: UserRole;
  roleBindings?: RoleBinding[];
  status: UserStatus;
  lastSeen: string;
  tokens: number;
};

export type NewUser = {
  username: string;
  role: AssignableUserRole;
};

export type NewNamespace = {
  description?: string;
  name: string;
  parentID: number;
};

export type UpdateNamespace = {
  description?: string;
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
  roleBindings: RoleBinding[];
  runs: RunListItem[];
  signals: SignalItem[];
  users: User[];
};
