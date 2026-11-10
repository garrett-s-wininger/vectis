import { createSourceJobClient, updateSourceJobCatalog, type SourceJobChange } from "./sourceJobs";
import type { Job } from "../domain/console";
const job: Job = {
  id: "build",
  name: "build",
  repository: "mono",
  namespacePath: "/team-a",
  sourceDetail: ".vectis/build.json",
  sourceKind: "repo",
  branch: "main",
  schedule: "Manual",
  nextRun: "On demand",
  triggers: [],
  status: "enabled"
};

const change: SourceJobChange = {
  registration: {
    repository_id: "mono",
    job_id: "build",
    namespace_id: 3,
    namespace: "/prod",
    path: ".vectis/build.json",
    ref: "",
    revision: 2,
    hotfix: {
      commit: "fix",
      reason: "production repair",
      actor: "owner",
      base_commit: "base",
      base_ref: "main",
      base_path: ".vectis/build.json",
      created_at: 1
    }
  },
  definition: { root: { id: "root" } },
  sourceCommit: "base"
};

it("deduplicates an owned job across namespaces while preserving other repositories and list order", () => {
  const before = { ...job, id: "before" };
  const otherRepo = { ...job, repository: "different-repo" };
  const input = [before, job, { ...job, namespacePath: "/prod" }, otherRepo];
  const result = updateSourceJobCatalog(input, job, change);
  expect(result).toHaveLength(3);
  expect(result[0]).toBe(before);
  expect(result[2]).toBe(otherRepo);
  expect(result[1]).toMatchObject({ namespacePath: "/prod", hotfix: { reason: "production repair" } });
  expect(input).toHaveLength(4);
  expect(job.namespacePath).toBe("/team-a");

  const cleared = updateSourceJobCatalog(result, result[1], {
    ...change,
    registration: { ...change.registration, revision: 3, hotfix: undefined }
  });

  expect(cleared[1].hotfix).toBeUndefined();
});

it("uses owner-scoped lifecycle endpoints and sends revision-fenced hotfix requests", async () => {
  vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
  const fetch = vi
    .fn()
    .mockImplementation(
      async (path: string) => new Response(JSON.stringify(path.endsWith("/namespaces") ? [] : {}), { status: 200 })
    );

  vi.stubGlobal("fetch", fetch);
  try {
    const client = createSourceJobClient(job);
    await client.load();
    await client.assign("/prod");
    const input = {
      definition: { root: { id: "root" } },
      reason: "repair",
      expected_revision: 2,
      expected_source_commit: "base"
    };

    await client.activate(input);
    await client.clear({ expected_revision: 3, expected_source_commit: "new-source" });
    expect(fetch).toHaveBeenNthCalledWith(
      1,
      "/api/v1/source-repositories/mono/jobs/build/lifecycle?namespace=%2Fteam-a",
      expect.anything()
    );

    expect(fetch).toHaveBeenNthCalledWith(
      2,
      "/api/v1/source-repositories/mono/jobs/build/ownership",
      expect.objectContaining({ method: "PUT", body: JSON.stringify({ namespace: "/prod" }) })
    );

    expect(fetch).toHaveBeenNthCalledWith(
      3,
      "/api/v1/source-repositories/mono/jobs/build/hotfix?namespace=%2Fteam-a",
      expect.objectContaining({ method: "PUT", body: JSON.stringify(input) })
    );

    expect(fetch).toHaveBeenNthCalledWith(
      4,
      "/api/v1/source-repositories/mono/jobs/build/hotfix?namespace=%2Fteam-a",
      expect.objectContaining({
        method: "DELETE",
        body: JSON.stringify({ expected_revision: 3, expected_source_commit: "new-source" })
      })
    );
  } finally {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
  }
});
