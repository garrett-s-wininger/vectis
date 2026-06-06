import {
  canDeleteMockNamespace,
  clusterHealthMetricsFor,
  createMockJob,
  createMockNamespace,
  createMockUser,
  deleteMockJob,
  dashboardMetricsFor,
  deleteMockNamespace,
  deleteMockUser,
  loadMockConsoleData,
  scopeMockConsoleData,
  submitMockEphemeralRun,
  triggerMockRun,
  updateMockJob,
  updateMockUserStatus
} from "./consoleData";

describe("mock console data", () => {
  it("loads isolated snapshots", async () => {
    const first = await loadMockConsoleData();
    const second = await loadMockConsoleData();

    first.users.pop();

    expect(second.users).toHaveLength(3);
    expect(second.cells).toHaveLength(3);
    expect(second.namespaces.map((namespace) => namespace.path)).toEqual(["/", "/team-a", "/team-a/edge", "/prod"]);
  });

  it("derives cluster health metrics from cells", async () => {
    const data = await loadMockConsoleData();

    expect(clusterHealthMetricsFor(data.cells)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({
          id: "cells",
          detail: "1 healthy, 1 degraded, 1 offline"
        }),
        expect.objectContaining({ id: "offline", value: "1" })
      ])
    );
  });

  it("derives dashboard metrics", async () => {
    const data = await loadMockConsoleData();

    expect(dashboardMetricsFor(data)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "running", value: "1" }),
        expect.objectContaining({ id: "jobs", value: "3" })
      ])
    );
  });

  it("scopes jobs and runs by namespace hierarchy", async () => {
    const data = await loadMockConsoleData();
    const scoped = scopeMockConsoleData(data, "/team-a");

    expect(scoped.jobs.map((job) => job.name)).toEqual(["api-test-suite", "docs-publish"]);
    expect(scoped.runs.map((run) => run.jobName)).toEqual(["api-test-suite", "docs-publish"]);
  });

  it("creates and deletes empty namespaces", async () => {
    const data = await loadMockConsoleData();
    const withNamespace = createMockNamespace(data, {
      name: "sandbox",
      parentID: 1
    });
    const sandbox = withNamespace.namespaces.find((namespace) => namespace.path === "/sandbox");

    expect(sandbox).toMatchObject({
      name: "sandbox",
      parentID: 1,
      role: "Admin"
    });
    expect(canDeleteMockNamespace(withNamespace, sandbox?.id ?? 0)).toBe(true);
    expect(
      deleteMockNamespace(withNamespace, sandbox?.id ?? 0).namespaces.find((namespace) => namespace.path === "/sandbox")
    ).toBeUndefined();
  });

  it("blocks deleting root, parent, and occupied namespaces", async () => {
    const data = await loadMockConsoleData();

    expect(canDeleteMockNamespace(data, 1)).toBe(false);
    expect(canDeleteMockNamespace(data, 2)).toBe(false);
    expect(canDeleteMockNamespace(data, 4)).toBe(false);
  });

  it("creates, updates, and deletes mock users", async () => {
    const data = await loadMockConsoleData();
    const withUser = createMockUser(data, {
      username: "taylor",
      role: "Operator"
    });
    const disabled = updateMockUserStatus(withUser, "user-taylor", "disabled");
    const removed = deleteMockUser(disabled, "user-taylor");

    expect(withUser.users[0]).toMatchObject({
      username: "taylor",
      role: "Operator",
      status: "active"
    });

    expect(disabled.users[0]).toMatchObject({ status: "disabled" });
    expect(removed.users.find((user) => user.id === "user-taylor")).toBeUndefined();
  });

  it("creates, updates, deletes, and triggers stored jobs", async () => {
    const data = await loadMockConsoleData();
    const created = createMockJob(data, {
      branch: "main",
      definition: JSON.stringify({ id: "cache-warmup", root: {} }),
      name: "cache-warmup",
      namespacePath: "/team-a",
      repository: "github.com/vectis/cache",
      schedule: "Manual",
      status: "enabled"
    });
    const job = created.jobs[0];

    expect(job).toMatchObject({
      id: "job-cache-warmup",
      name: "cache-warmup",
      namespacePath: "/team-a",
      nextRun: "Manual"
    });

    const updated = updateMockJob(created, job.id, {
      branch: "release",
      definition: JSON.stringify({ id: "cache-prime", root: {} }),
      name: "cache-prime",
      repository: "github.com/vectis/cache",
      schedule: "Hourly",
      status: "paused"
    });

    expect(updated.jobs[0]).toMatchObject({
      id: "job-cache-warmup",
      name: "cache-prime",
      branch: "release",
      nextRun: "1h",
      status: "paused"
    });

    const enabled = updateMockJob(updated, job.id, {
      branch: "release",
      definition: JSON.stringify({ id: "cache-prime", root: {} }),
      name: "cache-prime",
      repository: "github.com/vectis/cache",
      schedule: "Hourly",
      status: "enabled"
    });
    const triggered = triggerMockRun(enabled, job.id);

    expect(triggered.runs[0]).toMatchObject({
      definition: JSON.stringify({ id: "cache-prime", root: {} }),
      jobName: "cache-prime",
      namespacePath: "/team-a",
      source: "stored",
      trigger: "ui"
    });

    expect(deleteMockJob(triggered, job.id).jobs).toHaveLength(data.jobs.length);
  });

  it("triggers a queued run for a job", async () => {
    const data = await loadMockConsoleData();
    const next = triggerMockRun(data, "job-docs-publish");

    expect(next.runs[0]).toMatchObject({
      jobName: "docs-publish",
      cellName: "edge",
      namespacePath: "/team-a/edge",
      runNumber: 1241,
      source: "stored",
      status: "queued",
      trigger: "ui"
    });

    expect(next.jobs.find((job) => job.id === "job-docs-publish")).toMatchObject({
      lastRunStatus: "queued",
      nextRun: "Queued"
    });
  });

  it("submits an ephemeral run without creating a stored job", async () => {
    const data = await loadMockConsoleData();
    const next = submitMockEphemeralRun(data, {
      definition: JSON.stringify({ id: "database-backfill", root: {} }),
      namespacePath: "/team-a",
      submittedBy: "admin"
    });

    expect(next.jobs).toHaveLength(data.jobs.length);
    expect(next.runs[0]).toMatchObject({
      jobName: "database-backfill",
      cellName: "local",
      namespacePath: "/team-a",
      source: "ephemeral",
      submittedBy: "admin",
      status: "queued",
      trigger: "ui"
    });
  });
});
