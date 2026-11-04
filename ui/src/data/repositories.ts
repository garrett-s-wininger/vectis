import { requestJSON, VectisAPIError } from "../api/client";
import type { Namespace } from "../domain/console";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { shouldUseMockDataSource } from "./consoleDataSource";

export type RepositorySync = {
  status: string;
  last_started_at_unix?: number;
  last_finished_at_unix?: number;
  ref?: string;
  commit?: string;
  error?: string;
};

export type Repository = {
  repository_id: string;
  namespace: string;
  namespaces?: string[];
  source_kind: string;
  enabled: boolean;
  declared: boolean;
  canonical_url?: string;
  default_ref?: string;
  checkout_path?: string;
  checkout_mode?: string;
  authoring?: { write_definitions: boolean; reason?: string };
  sync: RepositorySync;
};

export type RepositoryStatus = {
  status: string;
  resolved_commit?: string;
  head_ref?: string;
  default_ref?: string;
  default_ref_resolved: boolean;
  path_exists: boolean;
  path_is_directory: boolean;
  git_repository: boolean;
  error?: { code: string; message: string };
};

export type RepositoryJob = { job_id: string; path: string; name: string; blob_sha: string };
export type RepositoryJobs = {
  jobs: RepositoryJob[];
  invalid?: { path: string; error: string }[];
  resolved_commit: string;
  requested_ref: string;
  truncated: boolean;
  next_cursor?: string;
};

export type RepositoryGrant = { namespace_id: number; namespace: string };
export type RepositorySource = {
  access(id: string): Promise<RepositoryGrant[]>;
  grant(id: string, namespaceID: number): Promise<RepositoryGrant[]>;
  revoke(id: string, namespaceID: number): Promise<RepositoryGrant[]>;
  namespaces(): Promise<Namespace[]>;
  list(namespace: string): Promise<Repository[]>;
  get(id: string): Promise<Repository>;
  status(id: string): Promise<RepositoryStatus>;
  jobs(id: string, cursor?: string, ref?: string): Promise<RepositoryJobs>;
  sync(id: string): Promise<Repository>;
};

let sharedMockStore: ReturnType<typeof createMockRepositoryStore> | undefined;
const base = (id: string) => `/api/v1/source-repositories/${encodeURIComponent(id)}`;

export function createRepositorySource(namespace?: string, management = false): RepositorySource {
  if (shouldUseMockDataSource()) {
    sharedMockStore ??= createMockRepositoryStore();
    return createMockRepositorySource(namespace, management, sharedMockStore);
  }

  const context = (path: string) => {
    const url = new URL(path, window.location.origin);

    if (management) url.searchParams.set("scope", "all");
    else if (namespace) url.searchParams.set("namespace", namespace);

    return `${url.pathname}${url.search}`;
  };

  return {
    access: (id) => requestJSON(`${base(id)}/namespaces`),
    grant: (id, namespaceID) => requestJSON(`${base(id)}/namespaces/${namespaceID}`, { method: "PUT" }),
    revoke: (id, namespaceID) => requestJSON(`${base(id)}/namespaces/${namespaceID}`, { method: "DELETE" }),
    async namespaces() {
      const rows =
        await requestJSON<{ id: number; name: string; path: string; parent_id?: number; break_inheritance: boolean }[]>(
          "/api/v1/namespaces"
        );

      return rows.map((row) => ({
        id: row.id,
        name: row.name,
        path: row.path,
        parentID: row.parent_id,
        breakInheritance: row.break_inheritance,
        role: "Not reported"
      }));
    },
    list: (namespace) =>
      requestJSON(
        management
          ? "/api/v1/source-repositories?scope=all"
          : `/api/v1/source-repositories?${new URLSearchParams({ namespace })}`
      ),
    get: (id) => requestJSON(context(base(id))),
    status: (id) => requestJSON(context(`${base(id)}/status`)),
    jobs: (id, cursor, ref) => {
      const query = new URLSearchParams({ limit: "100" });
      if (cursor) query.set("cursor", cursor);
      if (ref) query.set("ref", ref);

      return requestJSON(context(`${base(id)}/jobs?${query}`));
    },
    sync: (id) => requestJSON(`${base(id)}/sync`, { method: "POST" })
  };
}

function createMockRepositoryStore() {
  const data = createMockConsoleDataSnapshot();
  const repositories = Array.from(
    new Map(
      data.repositories.map((row) => [
        row.id,
        {
          repository_id: row.id,
          namespace: row.namespacePath,
          source_kind: "local_checkout",
          enabled: true,
          declared: true,
          default_ref: "main",
          checkout_mode: "external",
          checkout_path: `/workspace/${row.id}`,
          authoring: { write_definitions: row.canAuthor },
          sync: {
            status: "succeeded",
            last_finished_at_unix: 1793664000,
            commit: "b28a74c41946e6952910ddab7e5bd791d5bd0194"
          }
        } satisfies Repository
      ])
    ).values()
  );

  const grants = new Map<string, Set<string>>();
  for (const repo of data.repositories) {
    if (!grants.has(repo.id)) grants.set(repo.id, new Set());
    grants.get(repo.id)!.add(repo.namespacePath);
  }

  return { data, repositories, grants };
}

export function createMockRepositorySource(
  namespace?: string,
  management = false,
  store = createMockRepositoryStore()
): RepositorySource {
  const { data, repositories, grants } = store;
  const access = async (id: string) =>
    data.namespaces
      .filter((row) => grants.get(id)?.has(row.path))
      .map((row) => ({ namespace_id: row.id, namespace: row.path }));

  const get = async (id: string) => {
    const row = repositories.find((item) => item.repository_id === id);

    if (!row) throw new VectisAPIError(404, "source_repository_not_found", "Repository not found.");
    if (namespace && !management && !grants.get(id)?.has(namespace))
      throw new VectisAPIError(403, "authorization_denied", "Repository is not available in this namespace.");

    return {
      ...row,
      namespace: management ? "" : (namespace ?? row.namespace),
      namespaces: management ? [...(grants.get(id) ?? [])] : undefined
    };
  };

  return {
    access,
    grant: async (id, namespaceID) => {
      await get(id);
      const ns = data.namespaces.find((row) => row.id === namespaceID);
      if (!ns) throw new Error("Namespace not found.");

      grants.get(id)!.add(ns.path);
      return access(id);
    },
    revoke: async (id, namespaceID) => {
      await get(id);
      const ns = data.namespaces.find((row) => row.id === namespaceID);
      if (ns) grants.get(id)!.delete(ns.path);

      return access(id);
    },
    namespaces: async () => data.namespaces,
    list: async (namespace) =>
      repositories
        .filter((row) => management || grants.get(row.repository_id)?.has(namespace))
        .map((row) => ({
          ...row,
          namespace: management ? "" : namespace,
          namespaces: management ? [...(grants.get(row.repository_id) ?? [])] : undefined
        })),
    get,
    status: async (id) => {
      await get(id);
      return {
        status: "ok",
        resolved_commit: "b28a74c41946e6952910ddab7e5bd791d5bd0194",
        default_ref_resolved: true,
        path_exists: true,
        path_is_directory: true,
        git_repository: true
      };
    },
    jobs: async (id) => {
      await get(id);
      return {
        jobs: data.jobs
          .filter((job) => job.repository === id)
          .map((job) => ({ job_id: job.id, path: job.sourceDetail, name: job.name, blob_sha: "b28a74c4" })),
        resolved_commit: "b28a74c41946e6952910ddab7e5bd791d5bd0194",
        requested_ref: "main",
        truncated: false
      };
    },
    sync: async (id) => {
      const row = await get(id);

      row.sync = { ...row.sync, status: "succeeded", last_finished_at_unix: Math.floor(Date.now() / 1000) };
      repositories.find((item) => item.repository_id === id)!.sync = row.sync;

      return { ...row };
    }
  };
}
