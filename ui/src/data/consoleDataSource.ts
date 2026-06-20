import type { RunListItem, RunStatus } from "../components";
import type {
  ConsoleData,
  Job,
  Namespace,
  NewEphemeralRun,
  NewJob,
  NewNamespace,
  UpdateJob,
  UpdateNamespace
} from "../domain/console";
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

type APIEphemeralRunResponse = {
  id: string;
  run_id: string;
};

export type ConsoleDataSource = {
  createJob(input: NewJob): Promise<ConsoleData>;
  createNamespace(input: NewNamespace): Promise<ConsoleData>;
  deleteNamespace(namespaceID: number): Promise<ConsoleData>;
  loadConsole(): Promise<ConsoleData>;
  loadRun(runID: string): Promise<RunListItem>;
  submitEphemeralRun(input: NewEphemeralRun): Promise<ConsoleData>;
  triggerRun(jobID: string): Promise<ConsoleData>;
  updateJob(jobID: string, input: UpdateJob): Promise<ConsoleData>;
  updateNamespace(namespaceID: number, input: UpdateNamespace): Promise<ConsoleData>;
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
    async deleteNamespace(namespaceID) {
      data = deleteMockNamespace(data, namespaceID);
      return cloneConsoleData(data);
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
    async deleteNamespace(namespaceID) {
      await requestNoContent(`/api/v1/namespaces/${namespaceID}`, {
        method: "DELETE"
      });

      return loadAPIConsoleData();
    },
    loadConsole: loadAPIConsoleData,
    async loadRun(runID) {
      const [run, jobsPage] = await Promise.all([loadAPIRun(runID), loadAPIJobs()]);
      const jobs = jobsPage.data.map(apiJobToConsoleJob);
      const job = jobs.find((candidate) => candidate.id === run.job_id);
      return apiRunToConsoleRun(run, runDetailJob(run, job));
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
    }
  };
}

async function loadAPIConsoleData(): Promise<ConsoleData> {
  const mockBase = createMockConsoleDataSnapshot();
  const [namespaces, jobsPage] = await Promise.all([loadAPINamespaces(), loadAPIJobs()]);
  const jobs = jobsPage.data.map(apiJobToConsoleJob);
  const runs = await loadAPIRuns(jobs);

  return {
    ...mockBase,
    jobs: jobs.map((job) => ({
      ...job,
      lastRunStatus: runs.find((run) => run.jobName === job.name && run.namespacePath === job.namespacePath)?.status
    })),
    namespaces,
    runs
  };
}

async function loadAPINamespaces() {
  const namespaces = await requestJSON<APINamespace[]>("/api/v1/namespaces");

  return namespaces.map(apiNamespaceToConsoleNamespace);
}

async function loadAPIJobs() {
  return requestJSON<PaginatedResponse<APIJob>>("/api/v1/jobs?limit=200");
}

async function loadAPIRuns(jobs: Job[]) {
  const runs = await requestJSON<PaginatedResponse<APIRun>>("/api/v1/runs?limit=200");

  return runs.data.map((run) => {
    const job = jobs.find((candidate) => candidate.id === run.job_id);
    return apiRunToConsoleRun(run, runDetailJob(run, job));
  });
}

async function loadAPIRun(runID: string) {
  return requestJSON<APIRun>(`/api/v1/runs/${encodeURIComponent(runID)}`);
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
