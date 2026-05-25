import {
  canDeleteMockNamespace,
  clusterHealthMetricsFor,
  createMockNamespace,
  createMockUser,
  dashboardMetricsFor,
  deleteMockNamespace,
  deleteMockUser,
  loadMockConsoleData,
  scopeMockConsoleData,
  triggerMockRun,
  updateMockUserStatus
} from "./consoleData";

describe("mock console data", () => {
  it("loads isolated snapshots", async () => {
    const first = await loadMockConsoleData();
    const second = await loadMockConsoleData();

    first.users.pop();

    expect(second.users).toHaveLength(3);
    expect(second.cells).toHaveLength(3);
    expect(second.namespaces.map((namespace) => namespace.path)).toEqual([
      "/",
      "/team-a",
      "/team-a/edge",
      "/prod"
    ]);
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

    expect(scoped.jobs.map((job) => job.name)).toEqual([
      "api-test-suite",
      "docs-publish"
    ]);
    expect(scoped.runs.map((run) => run.jobName)).toEqual([
      "api-test-suite",
      "docs-publish"
    ]);
  });

  it("creates and deletes empty namespaces", async () => {
    const data = await loadMockConsoleData();
    const withNamespace = createMockNamespace(data, {
      name: "sandbox",
      parentID: 1
    });
    const sandbox = withNamespace.namespaces.find(
      (namespace) => namespace.path === "/sandbox"
    );

    expect(sandbox).toMatchObject({
      name: "sandbox",
      parentID: 1,
      role: "Admin"
    });
    expect(canDeleteMockNamespace(withNamespace, sandbox?.id ?? 0)).toBe(true);
    expect(
      deleteMockNamespace(withNamespace, sandbox?.id ?? 0).namespaces.find(
        (namespace) => namespace.path === "/sandbox"
      )
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

  it("triggers a queued run for a job", async () => {
    const data = await loadMockConsoleData();
    const next = triggerMockRun(data, "job-docs-publish");

    expect(next.runs[0]).toMatchObject({
      jobName: "docs-publish",
      namespacePath: "/team-a/edge",
      runNumber: 1241,
      status: "queued"
    });

    expect(next.jobs.find((job) => job.id === "job-docs-publish")).toMatchObject({
      lastRunStatus: "queued",
      nextRun: "Queued"
    });
  });
});
