import type { RunListItem, RunStatus } from "../components";
import type { ConsoleData, Job, Namespace, NewJob, UpdateJob } from "../domain/console";
import { requestJSON, requestNoContent } from "../api/client";
import {
  createMockConsoleDataSnapshot,
  createMockJob,
  scopeMockConsoleData,
  triggerMockRun,
  updateMockJob,
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
  id: number;
  name: string;
  parent_id?: number;
  path: string;
};

type APIRun = {
  created_at?: string;
  definition_version?: number;
  finished_at?: string;
  run_id: string;
  run_index: number;
  started_at?: string;
  status: string;
};

export type ConsoleDataSource = {
  createJob(input: NewJob): Promise<ConsoleData>;
  loadConsole(): Promise<ConsoleData>;
  triggerRun(jobID: string): Promise<ConsoleData>;
  updateJob(jobID: string, input: UpdateJob): Promise<ConsoleData>;
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
    async loadConsole() {
      return cloneConsoleData(data);
    },
    async triggerRun(jobID) {
      data = triggerMockRun(data, jobID);
      return cloneConsoleData(data);
    },
    async updateJob(jobID, input) {
      data = updateMockJob(data, jobID, input);
      return cloneConsoleData(data);
    }
  };
}

function createAPIConsoleDataSource(): ConsoleDataSource {
  return {
    async createJob(input) {
      const definition = parseJobDefinition(input.definition);
      definition.id = input.name;
      if (input.description) {
        definition.description = input.description;
      }

      await requestNoContent("/api/v1/jobs", {
        method: "POST",
        body: JSON.stringify({
          namespace: input.namespacePath,
          job: definition
        })
      });

      return loadAPIConsoleData();
    },
    loadConsole: loadAPIConsoleData,
    async triggerRun(jobID) {
      await requestJSON(`/api/v1/jobs/trigger/${encodeURIComponent(jobID)}`, {
        method: "POST"
      });

      return loadAPIConsoleData();
    },
    async updateJob(jobID, input) {
      const definition = parseJobDefinition(input.definition);
      definition.id = jobID;
      if (input.description) {
        definition.description = input.description;
      }

      await requestNoContent(`/api/v1/jobs/${encodeURIComponent(jobID)}`, {
        method: "PUT",
        body: JSON.stringify(definition)
      });

      return loadAPIConsoleData();
    }
  };
}

async function loadAPIConsoleData(): Promise<ConsoleData> {
  const mockBase = createMockConsoleDataSnapshot();
  const [namespaces, jobsPage] = await Promise.all([loadAPINamespaces(), loadAPIJobs()]);
  const jobs = jobsPage.data.map(apiJobToConsoleJob);
  const latestRuns = await loadLatestRuns(jobs);

  return {
    ...mockBase,
    jobs: jobs.map((job) => ({
      ...job,
      lastRunStatus: latestRuns.find((run) => run.jobName === job.name && run.namespacePath === job.namespacePath)
        ?.status
    })),
    namespaces,
    runs: latestRuns
  };
}

async function loadAPINamespaces() {
  const namespaces = await requestJSON<APINamespace[]>("/api/v1/namespaces");

  return namespaces.map(apiNamespaceToConsoleNamespace);
}

async function loadAPIJobs() {
  return requestJSON<PaginatedResponse<APIJob>>("/api/v1/jobs?limit=200");
}

async function loadLatestRuns(jobs: Job[]) {
  const runLists = await Promise.all(
    jobs.map(async (job) => {
      const runs = await requestJSON<PaginatedResponse<APIRun>>(
        `/api/v1/jobs/${encodeURIComponent(job.name)}/runs?limit=1`
      );

      return runs.data.map((run) => apiRunToConsoleRun(run, job));
    })
  );

  return runLists.flat();
}

function apiNamespaceToConsoleNamespace(namespace: APINamespace): Namespace {
  return {
    id: namespace.id,
    name: namespace.name,
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
    cellName: "local",
    source: "stored",
    startedAt: run.started_at,
    definition: job.definition,
    submittedBy: "anonymous",
    trigger: "api"
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
