import { requestJSON } from "../api/client";
import type { Job } from "../domain/console";
import { shouldUseMockDataSource } from "./consoleDataSource";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";

export type SourceHotfix = {
  commit: string;
  reason: string;
  actor: string;
  base_commit: string;
  base_ref: string;
  base_path: string;
  created_at: number;
  one_off?: boolean;
};

export type SourceJobRegistration = {
  repository_id: string;
  job_id: string;
  namespace_id: number;
  namespace: string;
  path: string;
  ref: string;
  revision: number;
  hotfix?: SourceHotfix;
};

export type SourceJobLifecycle = {
  registration?: SourceJobRegistration;
  source_definition?: unknown;
  source_json?: string;
  effective_definition?: unknown;
  source_commit?: string;
  source_error?: string;
};

export type SourceJobChange = { registration: SourceJobRegistration; definition?: unknown; sourceCommit?: string };
export type SourceHotfixInput = {
  definition?: unknown;
  reason?: string;
  expected_revision: number;
  expected_source_commit: string;
  idempotency_key?: string;
};

export type SourceJobClient = {
  load(): Promise<SourceJobLifecycle>;
  namespaces(): Promise<{ path: string; label: string }[]>;
  assign(namespace: string): Promise<SourceJobRegistration>;
  activate(input: SourceHotfixInput): Promise<SourceJobRegistration>;
  clear(input: SourceHotfixInput): Promise<SourceJobRegistration>;
};

export const mockSourceJobs = new Map<string, SourceJobLifecycle>();
export const sourceJobKey = (repository: string, job: string) => JSON.stringify([repository, job]);
export function createSourceJobClient(job: Job): SourceJobClient {
  const base = `/api/v1/source-repositories/${encodeURIComponent(job.repository)}/jobs/${encodeURIComponent(job.id)}`;
  const context = `?${new URLSearchParams({ namespace: job.namespacePath })}`;
  if (shouldUseMockDataSource()) {
    const key = sourceJobKey(job.repository, job.id);
    const state = () => {
      let row = mockSourceJobs.get(key);
      if (!row) {
        const definition = JSON.parse(
          job.definition || '{"root":{"id":"root","uses":"builtins/script","with":{"script":"echo source definition"}}}'
        );

        row = {
          source_definition: definition,
          source_json: JSON.stringify(definition, null, 2),
          effective_definition: definition,
          source_commit: "1111111111111111111111111111111111111111"
        };

        mockSourceJobs.set(key, row);
      }

      if (job.definition && !row.registration?.hotfix) {
        row.source_definition = JSON.parse(job.definition);
        row.source_json = job.definition;
        row.effective_definition = row.source_definition;
      }

      return row;
    };

    return {
      load: async () => structuredClone(state()),
      namespaces: async () =>
        createMockConsoleDataSnapshot().namespaces.map((row) => ({
          path: row.path,
          label: row.path === "/" ? "Root" : row.path
        })),
      assign: async (namespace) => {
        const row = state();
        row.registration ??= {
          repository_id: job.repository,
          job_id: job.id,
          namespace_id: 1,
          namespace,
          path: job.sourceDetail,
          ref: "",
          revision: 1
        };

        return structuredClone(row.registration);
      },
      activate: async (input) => {
        const row = state();
        if (!row.registration) throw new Error("Assign an owner first.");
        if (input.expected_revision !== row.registration.revision) throw new Error("Job changed; reload.");

        row.registration = {
          ...row.registration,
          revision: row.registration.revision + 1,
          hotfix: {
            commit: "2222222222222222222222222222222222222222",
            reason: input.reason || "",
            actor: "admin",
            base_commit: row.source_commit!,
            base_ref: job.branch,
            base_path: job.sourceDetail,
            created_at: Math.floor(Date.now() / 1000)
          }
        };

        row.effective_definition = input.definition;
        return structuredClone(row.registration);
      },
      clear: async (input) => {
        const row = state();
        if (!row.registration || input.expected_revision !== row.registration.revision)
          throw new Error("Job changed; reload.");

        row.registration = { ...row.registration, revision: row.registration.revision + 1, hotfix: undefined };
        row.effective_definition = row.source_definition;
        return structuredClone(row.registration);
      }
    };
  }

  return {
    load: () => requestJSON(`${base}/lifecycle${context}`),
    async namespaces() {
      const grants = await requestJSON<{ namespace: string }[]>(
        `/api/v1/source-repositories/${encodeURIComponent(job.repository)}/namespaces`
      );

      return grants.map((row) => ({ path: row.namespace, label: row.namespace === "/" ? "Root" : row.namespace }));
    },
    assign: (namespace) => requestJSON(`${base}/ownership`, { method: "PUT", body: JSON.stringify({ namespace }) }),
    activate: (input) => requestJSON(`${base}/hotfix${context}`, { method: "PUT", body: JSON.stringify(input) }),
    clear: (input) => requestJSON(`${base}/hotfix${context}`, { method: "DELETE", body: JSON.stringify(input) })
  };
}

export type RepositoryImpact = {
  jobs: SourceJobRegistration[];
  automations: { job_id: string; namespace: string; kind: string; enabled: boolean }[];
  namespaces: string[];
  unregistered_jobs: { job_id: string; path: string }[];
  discovery_error?: string;
  revision: string;
  commit?: string;
};

export function updateSourceJobCatalog(jobs: Job[], job: Job, change: SourceJobChange): Job[] {
  const canonical: Job = {
    ...job,
    namespacePath: change.registration.namespace,
    sourceDetail: change.registration.path,
    definition: change.definition === undefined ? undefined : JSON.stringify(change.definition, null, 2),
    sourceCommit: change.sourceCommit,
    hotfix: change.registration.hotfix
  };

  let included = false;
  const updated = jobs.flatMap((candidate) => {
    if (candidate.id !== job.id || candidate.repository !== job.repository) return [candidate];
    if (included) return [];
    included = true;
    return [canonical];
  });

  if (!included) updated.push(canonical);
  return updated;
}
