import {
  createMockUser,
  dashboardMetricsFor,
  deleteMockUser,
  loadMockConsoleData,
  triggerMockRun,
  updateMockUserStatus
} from "./consoleData";

describe("mock console data", () => {
  it("loads isolated snapshots", async () => {
    const first = await loadMockConsoleData();
    const second = await loadMockConsoleData();

    first.users.pop();

    expect(second.users).toHaveLength(3);
  });

  it("derives dashboard metrics", async () => {
    const data = await loadMockConsoleData();

    expect(dashboardMetricsFor(data)).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ id: "running", value: "1" }),
        expect.objectContaining({ id: "users", value: "2" })
      ])
    );
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
      runNumber: 1241,
      status: "queued"
    });

    expect(next.jobs.find((job) => job.id === "job-docs-publish")).toMatchObject({
      lastRunStatus: "queued",
      nextRun: "Queued"
    });
  });
});
