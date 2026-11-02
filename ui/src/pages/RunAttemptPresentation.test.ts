import { runAttemptNumbers, runForAttempt } from "./RunAttemptPresentation";
import { retriedArtifactRun } from "../mocks/runAttempts";

it("projects task history by the run attempt number without inventing an overall run outcome", () => {
  expect(runAttemptNumbers(retriedArtifactRun)).toEqual([2, 1]);
  const first = runForAttempt(retriedArtifactRun, 1);
  expect(first.tasks?.find((task) => task.taskKey === "report")).toMatchObject({
    status: "failed",
    attempts: [{ attempt: 1 }]
  });

  expect(first.status).toBe(retriedArtifactRun.status);
  expect(first.duration).toBe(retriedArtifactRun.duration);

  const second = runForAttempt(retriedArtifactRun, 2);
  expect(second.tasks?.find((task) => task.taskKey === "archive")).toMatchObject({ status: "planned", attempts: [] });
});

it("preserves the latest task state while its execution is still planned", () => {
  const run = {
    ...retriedArtifactRun,
    tasks: retriedArtifactRun.tasks!.map((task) => ({
      ...task,
      status: "pending" as const,
      attempts: task.attempts.map((item) =>
        item.attempt === 2 ? { ...item, executionStatus: "planned" as const } : item
      )
    }))
  };

  expect(runForAttempt(run, 2).tasks?.find((task) => task.taskKey === "report")).toMatchObject({
    status: "pending",
    attempts: []
  });

  expect(runForAttempt(run, 2).tasks?.find((task) => task.taskKey === "archive")).toMatchObject({
    status: "planned",
    attempts: []
  });
});
