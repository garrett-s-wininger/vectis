import type { RunListItem } from "../components";
import {
  formatRunDefinition,
  referenceLabel,
  runDefinitionDescription,
  runDetailDescription,
  runDefinitionTitle,
  runGraphDescription,
  runLogLines,
  runTaskExecutionLabel,
  runTaskStatusLabel,
  runTaskTimingLabel,
  runTasksForDisplay,
  preferredRunTaskID,
  runTimelineEvents,
  sourceLabel
} from "./RunDetailPresentation";

const run: RunListItem = {
  id: "run-1",
  jobName: "api-test-suite",
  runNumber: 1,
  cellName: "local",
  commit: "v3",
  createdAt: "2026-05-31T12:00:00Z",
  definition: JSON.stringify({ id: "api-test-suite", root: { id: "root" } }),
  definitionVersion: 3,
  duration: "42s",
  finishedAt: "2026-05-31T12:00:42Z",
  namespacePath: "/",
  source: "stored",
  startedAt: "2026-05-31T12:00:05Z",
  status: "succeeded",
  submittedBy: "anonymous",
  trigger: "api"
};

describe("run detail presentation", () => {
  it("describes run investigation states", () => {
    expect(runDetailDescription()).toBe("Execution graph, logs, timeline, and definition context.");
    expect(runGraphDescription()).toBe("Execution flow for the run.");
  });

  it("formats source and reference labels", () => {
    expect(sourceLabel("stored")).toBe("Saved");
    expect(sourceLabel("ephemeral")).toBe("Ephemeral");
    expect(referenceLabel(run)).toBe("v3");
    expect(referenceLabel({ ...run, source: "ephemeral" })).toBe("Inline");
  });

  it("describes the definition panel for stored and ephemeral runs", () => {
    expect(runDefinitionTitle(run)).toBe("Job Definition");
    expect(runDefinitionDescription(run)).toBe("Definition used when this run started.");
    expect(runDefinitionTitle({ ...run, source: "ephemeral" })).toBe("Submitted Definition");
    expect(runDefinitionDescription({ ...run, source: "ephemeral" })).toBe("Inline work submitted for this run.");
  });

  it("formats JSON definitions and falls back to generated metadata", () => {
    expect(formatRunDefinition(run)).toContain('"id": "api-test-suite"');
    expect(formatRunDefinition({ ...run, definition: "{bad" })).toBe("{bad");
    expect(formatRunDefinition({ ...run, definition: undefined })).toContain('"run_id": "run-1"');
  });

  it("builds timeline events with deltas", () => {
    expect(runTimelineEvents(run)).toEqual([
      expect.objectContaining({ label: "Accepted", detail: "Submitted via API by Anonymous." }),
      expect.objectContaining({ label: "Persisted", detail: "Assigned ID run-1.", delta: "+0s" }),
      expect.objectContaining({ label: "Dispatched", detail: "Worker selected on local.", delta: "+5s" }),
      expect.objectContaining({ label: "Finished", detail: "Worker finished execution.", delta: "+37s" })
    ]);
  });

  it("uses concise generated log lines without run-id prefixes", () => {
    expect(runLogLines(run)).toEqual(["accepted api-test-suite", "finished execution"]);
    expect(runLogLines({ ...run, status: "running" })).toEqual([
      "accepted api-test-suite",
      "worker claimed run on local",
      "streaming output"
    ]);
  });

  it("builds task display state", () => {
    const tasks = runTasksForDisplay({
      ...run,
      tasks: [
        {
          attempts: [],
          name: "root",
          status: "succeeded",
          taskID: "task-root",
          taskKey: "root"
        },
        {
          attempts: [
            {
              attempt: 1,
              attemptID: "attempt-build",
              cellID: "local",
              executionID: "execution-build",
              executionStatus: "running",
              startedAt: "2026-05-31T12:00:05Z",
              status: "running"
            }
          ],
          name: "build",
          parentTaskID: "task-root",
          status: "running",
          taskID: "task-build",
          taskKey: "build"
        }
      ]
    });

    expect(tasks.map((task) => [task.taskKey, task.depth])).toEqual([
      ["root", 0],
      ["build", 1]
    ]);

    expect(preferredRunTaskID(tasks)).toBe("task-build");
    expect(runTaskStatusLabel("accepted")).toBe("Accepted");
    expect(runTaskTimingLabel(tasks[1])).toMatch(/^Running /);
    expect(runTaskExecutionLabel(tasks[1])).toBe("execuild");
    expect(
      runTaskTimingLabel({
        ...tasks[1],
        attempts: [{ ...tasks[1].attempts[0], finishedAt: "2026-05-31T12:00:42Z" }]
      })
    ).toBe("37s");
  });
});
