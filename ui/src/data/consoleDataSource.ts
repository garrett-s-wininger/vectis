import type { RunListItem, RunStatus, RunTaskAttemptItem, RunTaskItem, RunTaskStatus } from "../components";
import type {
  Cell,
  ConsoleData,
  Job,
  Namespace,
  NewEphemeralRun,
  NewJob,
  NewNamespace,
  NewUser,
  RoleBindingRole,
  UpdateJob,
  UpdateNamespace,
  User,
  UserStatus
} from "../domain/console";
import { summarizeRoleBindings } from "../domain/roleBindings";
import { requestJSON, requestNoContent } from "../api/client";
import {
  createMockConsoleDataSnapshot,
  createMockJob,
  createMockNamespace,
  deleteMockNamespace,
  scopeMockConsoleData,
  submitMockEphemeralRun,
  triggerMockRun,
  updateMockJob,
  updateMockNamespace,
  updateMockUserStatus,
  createMockUser,
  deleteMockUser,
  grantMockRoleBinding,
  revokeMockRoleBinding,
  type MockConsoleData
} from "../mocks/consoleData";

type PaginatedResponse<T> = {
  data: T[];
  next_cursor?: number;
};

type APIJob = {
  description?: string;
  definition: unknown;
  name: string;
  namespace?: string;
};

type APINamespace = {
  break_inheritance: boolean;
  description?: string;
  id: number;
  name: string;
  parent_id?: number;
  path: string;
};

type APIRun = {
  created_at?: string;
  definition?: unknown;
  definition_version?: number;
  finished_at?: string;
  job_id?: string;
  namespace?: string;
  owning_cell?: string;
  run_id: string;
  run_index: number;
  started_at?: string;
  status: string;
};

type APIRunTaskAttempt = {
  attempt: number;
  attempt_id: string;
  cell_id: string;
  execution_id?: string;
  execution_status?: string;
  finished_at?: string;
  started_at?: string;
  status: string;
};

type APIRunTask = {
  attempts: APIRunTaskAttempt[];
  name: string;
  parent_task_id?: string;
  status: string;
  task_id: string;
  task_key: string;
};

type APIEphemeralRunResponse = {
  id: string;
  run_id: string;
};

type APIUser = {
  created_at?: string;
  enabled: boolean;
  id: number;
  initial_password?: string;
  username: string;
};

type APIRoleBinding = {
  local_user_id: number;
  role: string;
  username?: string;
};

type APICellsStatusResponse = {
  cells: APICellStatus[];
};

type APICellStatus = {
  catalog_failed?: number;
  catalog_pending?: number;
  catalog_total?: number;
  cell_id: string;
  error?: string;
  http_status?: number;
  ingress_configured: boolean;
  ingress_reachable: boolean;
  ingress_required: boolean;
  queued?: number;
  status: string;
  stuck?: number;
};

export type CreatedUserCredential = {
  password: string;
  username: string;
};

export type CreateUserResult = {
  credential?: CreatedUserCredential;
  users: User[];
};

export type ConsoleDataSource = {
  createJob(input: NewJob): Promise<ConsoleData>;
  createNamespace(input: NewNamespace): Promise<ConsoleData>;
  createUser(input: NewUser): Promise<CreateUserResult>;
  deleteNamespace(namespaceID: number): Promise<ConsoleData>;
  deleteUser(userID: string): Promise<User[]>;
  grantRoleBinding(userID: string, namespaceID: number, role: RoleBindingRole): Promise<ConsoleData>;
  loadCells(): Promise<Cell[]>;
  loadConsole(): Promise<ConsoleData>;
  loadRun(runID: string): Promise<RunListItem>;
  revokeRoleBinding(userID: string, namespaceID: number, role: RoleBindingRole): Promise<ConsoleData>;
  submitEphemeralRun(input: NewEphemeralRun): Promise<ConsoleData>;
  triggerRun(jobID: string): Promise<ConsoleData>;
  updateJob(jobID: string, input: UpdateJob): Promise<ConsoleData>;
  updateNamespace(namespaceID: number, input: UpdateNamespace): Promise<ConsoleData>;
  updateUserStatus(userID: string, status: UserStatus): Promise<User[]>;
};

export function createConsoleDataSource(): ConsoleDataSource {
  return shouldUseMockDataSource() ? createMockConsoleDataSource() : createAPIConsoleDataSource();
}

function shouldUseMockDataSource() {
  const configuredSource = import.meta.env.VITE_CONSOLE_DATA_SOURCE;

  if (configuredSource === "api") {
    return false;
  }

  if (configuredSource === "mock") {
    return true;
  }

  return import.meta.env.MODE === "test" || import.meta.env.STORYBOOK === "true";
}

function createMockConsoleDataSource(): ConsoleDataSource {
  let data = createMockConsoleDataSnapshot();

  return {
    async createJob(input) {
      data = createMockJob(data, input);
      return cloneConsoleData(data);
    },
    async createNamespace(input) {
      data = createMockNamespace(data, input);
      return cloneConsoleData(data);
    },
    async createUser(input) {
      data = createMockUser(data, input);
      return {
        credential: {
          password: "mock-generated-password",
          username: input.username.trim()
        },
        users: cloneConsoleData(data).users
      };
    },
    async deleteNamespace(namespaceID) {
      data = deleteMockNamespace(data, namespaceID);
      return cloneConsoleData(data);
    },
    async deleteUser(userID) {
      data = deleteMockUser(data, userID);
      return cloneConsoleData(data).users;
    },
    async grantRoleBinding(userID, namespaceID, role) {
      data = grantMockRoleBinding(data, userID, namespaceID, role);
      return cloneConsoleData(data);
    },
    async loadCells() {
      return cloneConsoleData(data).cells;
    },
    async loadConsole() {
      return cloneConsoleData(data);
    },
    async loadRun(runID) {
      const run = data.runs.find((candidate) => candidate.id === runID);
      if (!run) {
        throw new Error("Run not found.");
      }
      return run;
    },
    async revokeRoleBinding(userID, namespaceID, role) {
      data = revokeMockRoleBinding(data, userID, namespaceID, role);
      return cloneConsoleData(data);
    },
    async submitEphemeralRun(input) {
      data = submitMockEphemeralRun(data, input);
      return cloneConsoleData(data);
    },
    async triggerRun(jobID) {
      data = triggerMockRun(data, jobID);
      return cloneConsoleData(data);
    },
    async updateJob(jobID, input) {
      data = updateMockJob(data, jobID, input);
      return cloneConsoleData(data);
    },
    async updateNamespace(namespaceID, input) {
      data = updateMockNamespace(data, namespaceID, input);
      return cloneConsoleData(data);
    },
    async updateUserStatus(userID, status) {
      data = updateMockUserStatus(data, userID, status);
      return cloneConsoleData(data).users;
    }
  };
}

function createAPIConsoleDataSource(): ConsoleDataSource {
  return {
    async createJob(input) {
      const definition = parseJobDefinition(input.definition);
      definition.id = input.name;

      await requestNoContent("/api/v1/jobs", {
        method: "POST",
        body: JSON.stringify({
          namespace: input.namespacePath,
          job: definition
        })
      });

      return loadAPIConsoleData();
    },
    async createNamespace(input) {
      await requestJSON<APINamespace>("/api/v1/namespaces", {
        method: "POST",
        body: JSON.stringify({
          name: input.name,
          description: input.description,
          parent_id: input.parentID
        })
      });

      return loadAPIConsoleData();
    },
    async createUser(input) {
      const createdUser = await requestJSON<APIUser>("/api/v1/users", {
        method: "POST",
        body: JSON.stringify({
          username: input.username
        })
      });

      return {
        credential: createdUser.initial_password
          ? {
              password: createdUser.initial_password,
              username: createdUser.username
            }
          : undefined,
        users: await loadAPIUsers()
      };
    },
    async deleteNamespace(namespaceID) {
      await requestNoContent(`/api/v1/namespaces/${namespaceID}`, {
        method: "DELETE"
      });

      return loadAPIConsoleData();
    },
    async deleteUser(userID) {
      await requestNoContent(`/api/v1/users/${encodeURIComponent(userID)}`, {
        method: "DELETE"
      });

      return loadAPIUsers();
    },
    async grantRoleBinding(userID, namespaceID, role) {
      await requestJSON<APIRoleBinding>(`/api/v1/namespaces/${namespaceID}/bindings`, {
        method: "POST",
        body: JSON.stringify({
          local_user_id: Number(userID),
          role: apiRoleBindingRole(role)
        })
      });

      return loadAPIConsoleData();
    },
    loadConsole: loadAPIConsoleData,
    loadCells: loadAPICells,
    async loadRun(runID) {
      const [run, jobsPage, tasks] = await Promise.all([loadAPIRun(runID), loadAPIJobs(), loadAPIRunTasks(runID)]);
      const jobs = jobsPage.data.map(apiJobToConsoleJob);
      const job = jobs.find((candidate) => candidate.id === run.job_id);
      return {
        ...apiRunToConsoleRun(run, runDetailJob(run, job)),
        tasks
      };
    },
    async submitEphemeralRun(input) {
      const definition = parseJobDefinition(input.definition);
      const response = await requestJSON<APIEphemeralRunResponse>("/api/v1/jobs/run", {
        method: "POST",
        body: input.definition
      });
      const data = await loadAPIConsoleData();

      if (data.runs.some((run) => run.id === response.run_id)) {
        return data;
      }

      return {
        ...data,
        runs: [apiEphemeralRunToConsoleRun(response, definition, input), ...data.runs]
      };
    },
    async revokeRoleBinding(userID, namespaceID, role) {
      await requestNoContent(
        `/api/v1/namespaces/${namespaceID}/bindings/${encodeURIComponent(userID)}?role=${encodeURIComponent(apiRoleBindingRole(role))}`,
        {
          method: "DELETE"
        }
      );

      return loadAPIConsoleData();
    },
    async triggerRun(jobID) {
      await requestJSON(`/api/v1/jobs/trigger/${encodeURIComponent(jobID)}`, {
        method: "POST"
      });

      return loadAPIConsoleData();
    },
    async updateJob(jobID, input) {
      const definition = parseJobDefinition(input.definition);
      definition.id = jobID;

      await requestNoContent(`/api/v1/jobs/${encodeURIComponent(jobID)}`, {
        method: "PUT",
        body: JSON.stringify(definition)
      });

      return loadAPIConsoleData();
    },
    async updateNamespace(namespaceID, input) {
      await requestJSON<APINamespace>(`/api/v1/namespaces/${namespaceID}`, {
        method: "PUT",
        body: JSON.stringify({
          description: input.description
        })
      });

      return loadAPIConsoleData();
    },
    async updateUserStatus(userID, status) {
      await requestNoContent(`/api/v1/users/${encodeURIComponent(userID)}`, {
        method: "PUT",
        body: JSON.stringify({
          enabled: status === "active"
        })
      });

      return loadAPIUsers();
    }
  };
}

async function loadAPIConsoleData(): Promise<ConsoleData> {
  const mockBase = createMockConsoleDataSnapshot();
  const [namespaces, jobsPage, users] = await Promise.all([loadAPINamespaces(), loadAPIJobs(), loadAPIUsers()]);
  const roleBindings = await loadAPIRoleBindings(namespaces, users);
  const jobs = jobsPage.data.map(apiJobToConsoleJob);
  const runs = await loadAPIRuns(jobs);
  const usersWithRoleBindings = users.map((user) => {
    const bindings = roleBindings.filter((binding) => binding.userID === user.id);
    return {
      ...user,
      role: summarizeRoleBindings(bindings),
      roleBindings: bindings
    };
  });

  return {
    ...mockBase,
    cells: [],
    jobs: jobs.map((job) => ({
      ...job,
      lastRunStatus: runs.find((run) => run.jobName === job.name && run.namespacePath === job.namespacePath)?.status
    })),
    namespaces,
    roleBindings,
    runs,
    users: usersWithRoleBindings
  };
}

async function loadAPINamespaces() {
  const namespaces = await requestJSON<APINamespace[]>("/api/v1/namespaces");

  return namespaces.map(apiNamespaceToConsoleNamespace);
}

async function loadAPIJobs() {
  return requestJSON<PaginatedResponse<APIJob>>("/api/v1/jobs?limit=200");
}

async function loadAPICells() {
  const response = await requestJSON<APICellsStatusResponse>("/api/v1/cells/status");

  return response.cells.map(apiCellStatusToConsoleCell);
}

async function loadAPIRuns(jobs: Job[]) {
  const runs = await requestJSON<PaginatedResponse<APIRun>>("/api/v1/runs?limit=200");

  return runs.data.map((run) => {
    const job = jobs.find((candidate) => candidate.id === run.job_id);
    return apiRunToConsoleRun(run, runDetailJob(run, job));
  });
}

async function loadAPIUsers() {
  const users = await requestJSON<APIUser[]>("/api/v1/users");

  return users.map(apiUserToConsoleUser);
}

async function loadAPIRoleBindings(namespaces: Namespace[], users: User[]) {
  const usersByID = new Map(users.map((user) => [user.id, user]));
  const bindingsByNamespace = await Promise.all(
    namespaces.map(async (namespace) => ({
      namespace,
      bindings: await requestJSON<APIRoleBinding[]>(`/api/v1/namespaces/${namespace.id}/bindings`)
    }))
  );

  return bindingsByNamespace.flatMap(({ bindings, namespace }) =>
    bindings.map((binding) => {
      const userID = String(binding.local_user_id);
      return {
        id: `${namespace.id}:${userID}:${binding.role}`,
        namespaceID: namespace.id,
        namespacePath: namespace.path,
        role: consoleRoleBindingRole(binding.role),
        userID,
        username: binding.username || usersByID.get(userID)?.username
      };
    })
  );
}

async function loadAPIRun(runID: string) {
  return requestJSON<APIRun>(`/api/v1/runs/${encodeURIComponent(runID)}`);
}

async function loadAPIRunTasks(runID: string) {
  const response = await requestJSON<PaginatedResponse<APIRunTask>>(
    `/api/v1/runs/${encodeURIComponent(runID)}/tasks?limit=200`
  );

  return response.data.map(apiRunTaskToConsoleTask);
}

function apiUserToConsoleUser(user: APIUser): User {
  return {
    id: String(user.id),
    username: user.username,
    role: "Unassigned",
    roleBindings: [],
    status: user.enabled ? "active" : "disabled",
    lastSeen: user.created_at ? `Created ${formatDate(user.created_at)}` : "Created date unavailable",
    tokens: 0
  };
}

function apiCellStatusToConsoleCell(cell: APICellStatus): Cell {
  const queued = safeCount(cell.queued);
  const stuck = safeCount(cell.stuck);
  const catalogPending = safeCount(cell.catalog_pending);
  const catalogFailed = safeCount(cell.catalog_failed);
  const catalogTotal = safeCount(cell.catalog_total);

  return {
    id: cell.cell_id,
    name: cell.cell_id,
    endpoint: apiCellEndpointLabel(cell),
    region: apiCellRegionLabel(),
    status: apiCellStatusToConsoleStatus(cell),
    detail: apiCellDetail(cell),
    activeRuns: stuck,
    queueDepth: queued,
    stuckRuns: stuck,
    catalogPending,
    catalogFailed,
    catalogTotal,
    workersOnline: 0,
    workersTotal: 0,
    components: apiCellComponents(cell),
    progress: apiCellProgress(cell)
  };
}

function apiCellStatusToConsoleStatus(cell: APICellStatus): Cell["status"] {
  if (cell.status === "unreachable" || cell.status === "missing_route" || cell.status === "invalid") {
    return "offline";
  }

  if (cell.status === "unhealthy" || safeCount(cell.stuck) > 0 || safeCount(cell.catalog_failed) > 0) {
    return "degraded";
  }

  return "healthy";
}

function apiCellEndpointLabel(cell: APICellStatus) {
  if (!cell.ingress_required) {
    return "Local process";
  }

  return cell.ingress_configured ? "Route configured" : "Not configured";
}

function apiCellRegionLabel() {
  return "Name";
}

function apiCellDetail(cell: APICellStatus) {
  if (cell.error?.trim()) {
    return cell.error.trim();
  }

  if (cell.status === "ready") {
    return "Cell ingress is ready.";
  }

  if (cell.status === "local") {
    return "Local cell inferred from run and catalog state.";
  }

  if (cell.status === "missing_route") {
    return "Cell ingress endpoint is not configured.";
  }

  if (cell.status === "unhealthy" && cell.http_status) {
    return `Cell ingress returned HTTP ${cell.http_status}.`;
  }

  return "Cell health details are unavailable.";
}

function apiCellComponents(cell: APICellStatus): Cell["components"] {
  return [
    {
      id: `${cell.cell_id}-ingress`,
      label: "Ingress",
      detail: apiCellIngressDetail(cell),
      state: apiCellIngressState(cell)
    },
    {
      id: `${cell.cell_id}-queue`,
      label: "Queue",
      detail: `${safeCount(cell.queued)} queued`,
      state: safeCount(cell.queued) > 5 ? "degraded" : "healthy"
    },
    {
      id: `${cell.cell_id}-reconciler`,
      label: "Reconciler",
      detail: safeCount(cell.stuck) > 0 ? `${safeCount(cell.stuck)} stuck` : "No stuck runs",
      state: safeCount(cell.stuck) > 0 ? "degraded" : "healthy"
    },
    {
      id: `${cell.cell_id}-catalog`,
      label: "Catalog",
      detail: apiCellCatalogDetail(cell),
      state: safeCount(cell.catalog_failed) > 0 || safeCount(cell.catalog_pending) > 0 ? "degraded" : "healthy"
    }
  ];
}

function apiCellIngressDetail(cell: APICellStatus) {
  if (!cell.ingress_required) {
    return "Local cell";
  }

  if (!cell.ingress_configured) {
    return "Endpoint not configured";
  }

  if (cell.ingress_reachable) {
    return "Ready";
  }

  return cell.error?.trim() || cell.status;
}

function apiCellIngressState(cell: APICellStatus): Cell["components"][number]["state"] {
  if (!cell.ingress_required || cell.ingress_reachable) {
    return "healthy";
  }

  if (cell.status === "unhealthy") {
    return "degraded";
  }

  return "offline";
}

function apiCellCatalogDetail(cell: APICellStatus) {
  const pending = safeCount(cell.catalog_pending);
  const failed = safeCount(cell.catalog_failed);
  const total = safeCount(cell.catalog_total);

  if (total === 0) {
    return "No catalog events";
  }

  return `${pending} pending, ${failed} failed, ${total} total`;
}

function apiCellProgress(cell: APICellStatus): Cell["progress"] {
  const queued = safeCount(cell.queued);
  const stuck = safeCount(cell.stuck);
  const catalogPending = safeCount(cell.catalog_pending);
  const catalogFailed = safeCount(cell.catalog_failed);
  const catalogTotal = safeCount(cell.catalog_total);
  const catalogBacklog = catalogPending + catalogFailed;

  return [
    {
      id: `${cell.cell_id}-queue-pressure`,
      label: "Queue pressure",
      value: percentFromCount(queued, 10),
      detail: `${queued} queued`,
      tone: queued > 10 ? "critical" : queued > 5 ? "warning" : "neutral"
    },
    {
      id: `${cell.cell_id}-dispatch-attention`,
      label: "Dispatch attention",
      value: stuck > 0 ? 100 : 0,
      detail: stuck > 0 ? `${stuck} stuck` : "No stuck runs",
      tone: stuck > 0 ? "critical" : "neutral"
    },
    {
      id: `${cell.cell_id}-catalog-backlog`,
      label: "Catalog backlog",
      value: catalogTotal > 0 ? Math.round((catalogBacklog / catalogTotal) * 100) : 0,
      detail: apiCellCatalogDetail(cell),
      tone: catalogFailed > 0 ? "critical" : catalogPending > 0 ? "warning" : "neutral"
    }
  ];
}

function percentFromCount(count: number, fullScale: number) {
  if (fullScale <= 0) {
    return 0;
  }

  return Math.min(100, Math.round((count / fullScale) * 100));
}

function safeCount(value?: number) {
  return typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
}

function consoleRoleBindingRole(role: string): RoleBindingRole {
  switch (role) {
    case "admin":
      return "Admin";
    case "operator":
      return "Operator";
    case "trigger":
      return "Trigger";
    case "viewer":
    default:
      return "Viewer";
  }
}

function apiRoleBindingRole(role: RoleBindingRole) {
  return role.toLowerCase();
}

function apiNamespaceToConsoleNamespace(namespace: APINamespace): Namespace {
  return {
    id: namespace.id,
    name: namespace.name,
    description: namespace.description || undefined,
    parentID: namespace.parent_id,
    path: namespace.path,
    breakInheritance: namespace.break_inheritance,
    role: "Admin"
  };
}

function apiJobToConsoleJob(job: APIJob): Job {
  const definition = normalizeJobDefinition(job.definition, job.name);
  const definitionJSON = JSON.stringify(definition, null, 2);

  return {
    id: job.name,
    name: job.name,
    description: job.description ?? stringField(definition, "description"),
    repository: "",
    branch: "",
    sourceDetail: "Stored in Vectis",
    sourceKind: "db",
    definition: definitionJSON,
    namespacePath: job.namespace ?? "/",
    schedule: "Manual",
    nextRun: "On demand",
    triggers: [{ kind: "manual", detail: "On demand" }],
    status: "enabled"
  };
}

function stringField(value: Record<string, unknown>, key: string) {
  const field = value[key];
  return typeof field === "string" && field.trim() ? field : undefined;
}

function apiRunToConsoleRun(run: APIRun, job: Job): RunListItem {
  return {
    id: run.run_id,
    jobName: job.name,
    namespacePath: job.namespacePath,
    runNumber: run.run_index,
    createdAt: run.created_at,
    commit: definitionReference(run.definition_version),
    definitionVersion: run.definition_version,
    status: apiRunStatusToConsoleStatus(run.status),
    duration: formatRunDuration(run),
    finishedAt: run.finished_at,
    cellName: run.owning_cell ?? "local",
    source: job.sourceDetail === "Inline" ? "ephemeral" : "stored",
    startedAt: run.started_at,
    definition: apiRunDefinition(run) ?? job.definition,
    submittedBy: "anonymous",
    trigger: "api"
  };
}

function apiRunTaskToConsoleTask(task: APIRunTask): RunTaskItem {
  return {
    attempts: task.attempts.map(apiRunTaskAttemptToConsoleAttempt),
    name: task.name || task.task_key,
    parentTaskID: task.parent_task_id,
    status: apiTaskStatus(task.status),
    taskID: task.task_id,
    taskKey: task.task_key
  };
}

function apiRunTaskAttemptToConsoleAttempt(attempt: APIRunTaskAttempt): RunTaskAttemptItem {
  return {
    attempt: attempt.attempt,
    attemptID: attempt.attempt_id,
    cellID: attempt.cell_id,
    executionID: attempt.execution_id,
    executionStatus: attempt.execution_status ? apiTaskStatus(attempt.execution_status) : undefined,
    finishedAt: attempt.finished_at,
    startedAt: attempt.started_at,
    status: apiTaskStatus(attempt.status)
  };
}

function runDetailJob(run: APIRun, job?: Job): Job {
  if (job) {
    return job;
  }

  const definition = apiRunDefinition(run);
  const normalizedDefinition = definition
    ? normalizeJobDefinition(JSON.parse(definition), run.job_id ?? run.run_id)
    : null;
  const jobName =
    stringField(normalizedDefinition ?? {}, "name") ??
    stringField(normalizedDefinition ?? {}, "id") ??
    run.job_id ??
    run.run_id;

  return {
    id: jobName,
    name: jobName,
    repository: "",
    branch: "",
    sourceDetail: "Inline",
    sourceKind: "db",
    definition,
    namespacePath: run.namespace ?? "/",
    schedule: "Manual",
    nextRun: "On demand",
    triggers: [{ kind: "manual", detail: "On demand" }],
    status: "enabled"
  };
}

function apiRunDefinition(run: APIRun) {
  if (!run.definition) {
    return undefined;
  }

  return JSON.stringify(run.definition, null, 2);
}

function apiEphemeralRunToConsoleRun(
  response: APIEphemeralRunResponse,
  definition: Record<string, unknown>,
  input: NewEphemeralRun
): RunListItem {
  return {
    id: response.run_id,
    jobName: stringField(definition, "name") ?? stringField(definition, "id") ?? response.id,
    namespacePath: input.namespacePath,
    runNumber: 1,
    commit: "inline definition",
    createdAt: new Date().toISOString(),
    definition: JSON.stringify(definition, null, 2),
    duration: "Queued",
    cellName: "local",
    source: "ephemeral",
    submittedBy: input.submittedBy ?? "anonymous",
    trigger: "ui",
    status: "queued"
  };
}

function definitionReference(version?: number) {
  return version ? `v${version}` : "v1";
}

function apiRunStatusToConsoleStatus(status: string): RunStatus {
  switch (status) {
    case "cancelled":
    case "failed":
    case "queued":
    case "running":
    case "succeeded":
    case "orphaned":
    case "abandoned":
    case "aborted":
      return status;
    default:
      return "queued";
  }
}

function apiTaskStatus(status: string): RunTaskStatus {
  switch (status) {
    case "planned":
    case "pending":
    case "accepted":
    case "running":
    case "succeeded":
    case "failed":
    case "cancelled":
    case "aborted":
      return status;
    default:
      return "pending";
  }
}

function formatRunDuration(run: APIRun) {
  if (!run.started_at) {
    return "not started";
  }

  const startedAt = Date.parse(run.started_at);
  const finishedAt = run.finished_at ? Date.parse(run.finished_at) : Date.now();

  if (Number.isNaN(startedAt) || Number.isNaN(finishedAt)) {
    return "unknown";
  }

  const seconds = Math.max(0, Math.round((finishedAt - startedAt) / 1000));

  if (seconds < 60) {
    return `${seconds}s`;
  }

  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}

function formatDate(value: string) {
  const date = new Date(value);

  if (Number.isNaN(date.getTime())) {
    return "date unavailable";
  }

  return date.toLocaleDateString(undefined, {
    day: "numeric",
    month: "short",
    year: "numeric"
  });
}

function parseJobDefinition(definitionJSON: string) {
  return normalizeJobDefinition(JSON.parse(definitionJSON), "");
}

function normalizeJobDefinition(definition: unknown, fallbackID: string) {
  if (!definition || typeof definition !== "object" || Array.isArray(definition)) {
    throw new Error("Definition must be a JSON object.");
  }

  const normalized = { ...(definition as Record<string, unknown>) };

  if (typeof normalized.id !== "string" || normalized.id === "") {
    normalized.id = fallbackID;
  }

  return normalized;
}

function cloneConsoleData(data: MockConsoleData) {
  return scopeMockConsoleData(data, "/");
}
