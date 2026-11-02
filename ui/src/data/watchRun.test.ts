import type { RunListItem } from "../components";
import { retainRunTasks, watchRun } from "./watchRun";

const run: RunListItem = {
  id: "run-1",
  jobName: "test",
  runNumber: 1,
  commit: "v1",
  duration: "Queued",
  status: "queued",
  tasks: [{ taskID: "root", taskKey: "root", name: "Root", status: "pending", attempts: [] }]
};

const completed: RunListItem = {
  ...run,
  status: "succeeded",
  tasks: run.tasks?.map((task) => ({ ...task, status: "succeeded" }))
};

describe("watchRun", () => {
  beforeEach(() => vi.useFakeTimers());
  afterEach(() => vi.useRealTimers());

  it("follows run and task transitions and stops after the final snapshot", async () => {
    const load = vi
      .fn()
      .mockResolvedValueOnce(run)
      .mockResolvedValueOnce({ ...run, status: "running" })
      .mockResolvedValueOnce({ ...run, status: "succeeded" })
      .mockResolvedValue(completed);
    const update = vi.fn();
    const stop = watchRun(load, update, () => true);
    await vi.advanceTimersByTimeAsync(6000);
    expect(update.mock.calls.map(([value]) => value.status)).toEqual(["queued", "running", "succeeded", "succeeded"]);
    expect(update).toHaveBeenLastCalledWith(completed);

    await vi.advanceTimersByTimeAsync(30000);
    expect(load).toHaveBeenCalledTimes(4);
    stop();
  });

  it("waits for the selected attempt's final state as well as the task summary", async () => {
    const load = vi
      .fn()
      .mockResolvedValueOnce({
        ...completed,
        tasks: completed.tasks?.map((task) => ({
          ...task,
          attempts: [{ attempt: 1, attemptID: "attempt-1", cellID: "local", status: "pending" }]
        }))
      })
      .mockResolvedValue(completed);

    const update = vi.fn();
    const stop = watchRun(load, update, () => true);
    await vi.advanceTimersByTimeAsync(2000);

    expect(update).toHaveBeenLastCalledWith(completed);
    expect(load).toHaveBeenCalledTimes(2);

    stop();
  });

  it("does not overlap requests and ignores an in-flight result after navigation", async () => {
    let resolve!: (value: RunListItem) => void;
    const load = vi.fn(
      () =>
        new Promise<RunListItem>((done) => {
          resolve = done;
        })
    );

    const update = vi.fn();
    const stop = watchRun(load, update, () => true);
    await vi.advanceTimersByTimeAsync(30000);
    expect(load).toHaveBeenCalledOnce();

    stop();
    resolve(run);

    await vi.advanceTimersByTimeAsync(30000);
    expect(update).not.toHaveBeenCalled();
    expect(load).toHaveBeenCalledOnce();
  });

  it("backs off transient failures and retries without a refresh loop", async () => {
    const load = vi.fn().mockRejectedValueOnce(new Error("temporarily unavailable")).mockResolvedValue(completed);
    const update = vi.fn();
    const error = vi.fn(() => true);
    const stop = watchRun(load, update, error);
    await vi.advanceTimersByTimeAsync(3999);
    expect(load).toHaveBeenCalledOnce();
    expect(error).toHaveBeenCalledOnce();

    await vi.advanceTimersByTimeAsync(1);
    expect(update).toHaveBeenCalledWith(completed);

    stop();
  });

  it("stops when the run is missing", async () => {
    const load = vi.fn().mockRejectedValue(new Error("Run not found."));
    const stop = watchRun(load, vi.fn(), () => false);
    await vi.advanceTimersByTimeAsync(30000);

    expect(load).toHaveBeenCalledOnce();
    stop();
  });

  it.each(["running", "succeeded"] as const)(
    "retains known nodes through empty and partial snapshots and polls for the full final tree, last child state: %s",
    async (childStatus) => {
      const known: RunListItem = {
        ...run,
        status: "running",
        tasks: [
          { ...run.tasks![0], status: "succeeded" },
          { taskID: "child", taskKey: "child", name: "Child", parentTaskID: "root", status: childStatus, attempts: [] }
        ]
      };

      const final = {
        ...known,
        status: "succeeded" as const,
        tasks: known.tasks!.map((task) => ({ ...task, status: "succeeded" as const }))
      };

      const load = vi
        .fn()
        .mockResolvedValueOnce({ ...final, tasks: [] })
        .mockResolvedValueOnce({ ...final, tasks: [final.tasks[0]] })
        .mockResolvedValue(final);

      const update = vi.fn();
      const stop = watchRun(load, update, () => true, known);
      await vi.advanceTimersByTimeAsync(0);
      expect(update.mock.calls[0][0]).toMatchObject({ tasksUpdating: true, tasks: known.tasks });

      await vi.advanceTimersByTimeAsync(2000);
      expect(update.mock.calls[1][0].tasks.map((task: { taskID: string }) => task.taskID)).toEqual(["root", "child"]);
      expect(update.mock.calls[1][0].tasksUpdating).toBe(true);

      await vi.advanceTimersByTimeAsync(2000);
      expect(update).toHaveBeenLastCalledWith(final);

      await vi.advanceTimersByTimeAsync(10000);
      expect(load).toHaveBeenCalledTimes(3);

      stop();
    }
  );

  it("does not stop at an empty terminal snapshot before it has seen any tasks", async () => {
    const load = vi
      .fn()
      .mockResolvedValueOnce({ ...completed, tasks: [] })
      .mockResolvedValue(completed);

    const stop = watchRun(load, vi.fn(), () => true);
    await vi.advanceTimersByTimeAsync(2000);
    expect(load).toHaveBeenCalledTimes(2);

    stop();
  });

  it("starts a fresh task snapshot for a new root attempt or a different run", () => {
    const previous: RunListItem = {
      ...run,
      tasks: [
        { ...run.tasks![0], attempts: [{ attempt: 1, attemptID: "root-1", cellID: "local", status: "succeeded" }] },
        { taskID: "old-child", taskKey: "old-child", name: "Old child", status: "succeeded", attempts: [] }
      ]
    };

    const next: RunListItem = {
      ...run,
      tasks: [{ ...run.tasks![0], attempts: [{ attempt: 2, attemptID: "root-2", cellID: "local", status: "running" }] }]
    };

    expect(retainRunTasks(previous, next)).toBe(next);
    expect(retainRunTasks(previous, { ...run, id: "other-run" }).tasks).toEqual(run.tasks);
    expect(retainRunTasks(next, previous)).toMatchObject({ tasks: next.tasks, tasksUpdating: true });
  });
});
