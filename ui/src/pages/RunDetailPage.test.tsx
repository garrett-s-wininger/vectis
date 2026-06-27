import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import type { RunListItem } from "../components";
import { RunDetailPage } from "./RunDetailPage";

const run: RunListItem = {
  id: "run-1",
  jobName: "api-test-suite",
  runNumber: 12,
  cellName: "local",
  commit: "v2",
  createdAt: "2026-05-31T12:00:00Z",
  definition: JSON.stringify({ id: "api-test-suite", root: { id: "root" } }),
  duration: "42s",
  finishedAt: "2026-05-31T12:00:42Z",
  namespacePath: "/team-a",
  source: "stored",
  startedAt: "2026-05-31T12:00:05Z",
  status: "failed",
  submittedBy: "mira",
  tasks: [
    {
      attempts: [],
      name: "root",
      status: "failed",
      taskID: "task-root",
      taskKey: "root"
    },
    {
      attempts: [
        {
          attempt: 1,
          attemptID: "attempt-api",
          cellID: "local",
          executionID: "execution-api",
          executionStatus: "failed",
          finishedAt: "2026-05-31T12:00:42Z",
          startedAt: "2026-05-31T12:00:05Z",
          status: "failed"
        }
      ],
      name: "API tests",
      parentTaskID: "task-root",
      status: "failed",
      taskID: "task-api",
      taskKey: "api"
    },
    {
      attempts: [],
      name: "Report",
      parentTaskID: "task-root",
      status: "planned",
      taskID: "task-report",
      taskKey: "report"
    }
  ],
  trigger: "manual"
};

describe("RunDetailPage", () => {
  it("renders and selects task execution contexts", async () => {
    render(<RunDetailPage onBack={() => undefined} run={run} runID={run.id} />);

    expect(screen.getByRole("heading", { name: "Graph" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /API tests/i })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getByText("Worker output for the selected task.")).toBeInTheDocument();
    expect(screen.getAllByText("API tests").length).toBeGreaterThan(0);

    await userEvent.click(screen.getByRole("button", { name: /Report/i }));

    expect(screen.getByRole("button", { name: /Report/i })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getAllByText("Report").length).toBeGreaterThan(0);
    expect(screen.getByText("No execution attempt yet")).toBeInTheDocument();
  });
});
