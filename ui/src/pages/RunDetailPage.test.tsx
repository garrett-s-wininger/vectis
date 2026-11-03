import { render, screen, within } from "@testing-library/react";
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
  it("keeps actions in the header and shows identifiers with replay context in the summary", () => {
    const jobID = "2a5ed99c-6d51-4c50-97e9-e31b23da7469";
    const original: RunListItem = {
      ...run,
      id: "original-execution",
      jobID,
      jobName: jobID,
      source: "ephemeral",
      runNumber: 1
    };

    const replay: RunListItem = { ...original, id: "different-execution", runNumber: 2, replayOfRunID: original.id };
    render(
      <RunDetailPage
        run={replay}
        originalRun={original}
        runID={replay.id}
        onBack={() => undefined}
        onReplay={async () => undefined}
      />
    );

    const heading = screen.getByRole("heading", { level: 1, name: "Run once (#2)" });
    expect(heading.closest("header")).toContainElement(screen.getByRole("button", { name: "Run again" }));

    const summary = screen.getByRole("region", { name: "Run summary" });
    expect(summary).toHaveTextContent("Re-run of");
    expect(summary).toHaveTextContent("Run once #1");
    expect(screen.getByRole("link", { name: /^View original run/ })).toHaveAttribute(
      "href",
      "/runs/original-execution"
    );

    expect(screen.getByText("different-execution")).toBeVisible();
    expect(within(summary).getByText(jobID)).toBeVisible();
    expect(summary).toContainElement(screen.getByRole("link", { name: /^View original run/ }));

    const crumbs = screen.getByRole("navigation", { name: "Run location" });
    expect(Array.from(crumbs.children).map((crumb) => crumb.textContent)).toEqual([
      "/team-a",
      "Runs",
      "e31b23da7469",
      "#2"
    ]);
  });

  it("shows the fanout handoff until child tasks arrive without changing root status", () => {
    const root = { ...run.tasks![0], status: "succeeded" as const };
    const activeRun: RunListItem = { ...run, status: "running", tasks: [root] };
    const view = render(<RunDetailPage onBack={() => undefined} run={activeRun} runID={run.id} />);

    const rootNode = screen.getByRole("button", { name: "root: Succeeded" });
    expect(screen.getByRole("status")).toHaveTextContent("Waiting for task updates");
    expect(rootNode).toHaveAttribute("aria-pressed", "false");

    view.rerender(
      <RunDetailPage
        onBack={() => undefined}
        run={{ ...activeRun, tasks: [root, { ...run.tasks![1], status: "running" }] }}
        runID={run.id}
      />
    );

    expect(screen.queryByText("Waiting for task updates")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "root: Succeeded" })).toBe(rootNode);
    expect(screen.getByRole("button", { name: "API tests: Running" })).toHaveAttribute("aria-pressed", "false");
  });

  it.each(["succeeded", "failed", "cancelled", "aborted", "abandoned"] as const)(
    "does not imply more work for a %s run",
    (status) => {
      render(
        <RunDetailPage
          onBack={() => undefined}
          run={{ ...run, status, tasks: [{ ...run.tasks![0], status: "succeeded" }] }}
          runID={run.id}
        />
      );

      expect(screen.queryByText("Waiting for task updates")).not.toBeInTheDocument();
    }
  );

  it("renders and selects task execution contexts", async () => {
    const view = render(<RunDetailPage onBack={() => undefined} run={run} runID={run.id} />);

    expect(screen.getByRole("heading", { name: "Graph" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: /API tests/i })).toHaveAttribute("aria-pressed", "false");
    expect(screen.getByRole("heading", { name: "Run Logs" })).toBeInTheDocument();
    expect(screen.queryByLabelText(/^Attempt:/)).not.toBeInTheDocument();

    await userEvent.click(screen.getByRole("button", { name: /API tests/i }));
    expect(screen.getByText("Worker output for the selected task.")).toBeInTheDocument();
    expect(screen.getAllByText("API tests").length).toBeGreaterThan(0);

    await userEvent.click(screen.getByRole("button", { name: /Report/i }));

    expect(screen.getByRole("button", { name: /Report/i })).toHaveAttribute("aria-pressed", "true");
    expect(screen.getAllByText("Report").length).toBeGreaterThan(0);
    expect(screen.getByText("This task has no execution in attempt 1.")).toBeInTheDocument();

    view.rerender(
      <RunDetailPage
        onBack={() => undefined}
        run={{ ...run, tasks: run.tasks?.map((task) => ({ ...task, status: "succeeded" })) }}
        runID={run.id}
      />
    );

    expect(screen.getByRole("button", { name: /Report/i })).toHaveAttribute("aria-pressed", "true");
  });

  it("keeps the run-wide context through task updates", () => {
    const view = render(<RunDetailPage onBack={() => undefined} run={run} runID={run.id} />);
    expect(screen.getByRole("button", { name: /API tests/i })).toHaveAttribute("aria-pressed", "false");

    view.rerender(
      <RunDetailPage
        onBack={() => undefined}
        run={{
          ...run,
          tasks: run.tasks?.map((task) => ({
            ...task,
            status: task.taskID === "task-report" ? "running" : "succeeded"
          }))
        }}
        runID={run.id}
      />
    );

    expect(screen.getByRole("button", { name: /Report/i })).toHaveAttribute("aria-pressed", "false");
  });

  it.each([false, true])(
    "preserves task or run-wide context through the status handoff, task selected: %s",
    async (explicit) => {
      const active: RunListItem = {
        ...run,
        status: "running",
        tasks: run.tasks?.map((task) => ({ ...task, status: task.taskID === "task-report" ? "running" : "succeeded" }))
      };

      const view = render(<RunDetailPage onBack={() => undefined} run={active} runID={run.id} />);
      const report = screen.getByRole("button", { name: "Report: Running" });
      if (explicit) await userEvent.click(report);

      expect(report).toHaveAttribute("aria-pressed", String(explicit));

      const tasks = active.tasks?.map((task) => ({ ...task, status: "succeeded" as const }));
      view.rerender(<RunDetailPage onBack={() => undefined} run={{ ...active, tasks }} runID={run.id} />);
      expect(screen.getByRole("button", { name: "Report: Succeeded" })).toBe(report);
      expect(report).toHaveAttribute("aria-pressed", String(explicit));
      expect(screen.queryByText("Waiting for task updates")).not.toBeInTheDocument();
      expect(screen.getByRole("button", { name: "root: Succeeded" })).toHaveAttribute("aria-pressed", "false");

      view.rerender(
        <RunDetailPage onBack={() => undefined} run={{ ...active, status: "succeeded", tasks }} runID={run.id} />
      );

      expect(report).toHaveAttribute("aria-pressed", String(explicit));
      expect(screen.getAllByText("Report").length).toBeGreaterThan(0);
    }
  );

  it("resets task context when navigating to a different run", async () => {
    const view = render(<RunDetailPage onBack={() => undefined} run={run} runID={run.id} />);
    await userEvent.click(screen.getByRole("button", { name: "API tests: Failed" }));
    expect(screen.getByRole("button", { name: "API tests: Failed" })).toHaveAttribute("aria-pressed", "true");

    const next: RunListItem = {
      ...run,
      id: "next-run",
      status: "succeeded",
      tasks: run.tasks?.map((task) => ({ ...task, status: "succeeded" }))
    };

    view.rerender(<RunDetailPage onBack={() => undefined} run={next} runID={next.id} />);
    expect(screen.getByRole("button", { name: "root: Succeeded" })).toHaveAttribute("aria-pressed", "false");
  });

  it("opens the run definition disclosure on request", async () => {
    render(<RunDetailPage onBack={() => undefined} run={run} runID={run.id} />);

    const summary = screen.getByText("Job Definition").closest("summary");
    const details = summary?.closest("details");

    expect(summary).toBeInTheDocument();
    expect(details).toBeInTheDocument();
    if (!summary || !details) {
      throw new Error("Expected the job definition to render as a disclosure");
    }

    expect(details).not.toHaveAttribute("open");

    await userEvent.click(summary);

    expect(details).toHaveAttribute("open");
  });
});

it("restores incoming investigation context without remounting graph nodes", () => {
  const view = render(
    <RunDetailPage onBack={() => undefined} run={run} runID={run.id} investigation={{ task: "task-api", attempt: 1 }} />
  );

  const node = screen.getByRole("button", { name: "API tests: Failed" });

  expect(node).toHaveAttribute("aria-pressed", "true");
  expect(screen.getByRole("heading", { name: "Task Logs" })).toBeVisible();

  view.rerender(
    <RunDetailPage onBack={() => undefined} run={run} runID={run.id} investigation={{ attempt: "latest" }} />
  );

  expect(screen.getByRole("button", { name: "API tests: Failed" })).toBe(node);
  expect(node).toHaveAttribute("aria-pressed", "false");
  expect(screen.getByRole("heading", { name: "Run Logs" })).toBeVisible();
});

it("preserves a linked task until its graph node arrives", () => {
  const view = render(
    <RunDetailPage
      onBack={() => undefined}
      run={{ ...run, tasksUpdating: true, status: "running", tasks: [run.tasks![0]] }}
      runID={run.id}
      investigation={{ task: "task-api", attempt: "latest" }}
    />
  );

  view.rerender(
    <RunDetailPage
      onBack={() => undefined}
      run={run}
      runID={run.id}
      investigation={{ task: "task-api", attempt: "latest" }}
    />
  );

  expect(screen.getByRole("button", { name: "API tests: Failed" })).toHaveAttribute("aria-pressed", "true");
});

it("explains unavailable tasks and lets users clear the linked selection", async () => {
  const onInvestigationChange = vi.fn();
  render(
    <RunDetailPage
      onBack={() => undefined}
      run={run}
      runID={run.id}
      investigation={{ task: "removed-task", attempt: 1 }}
      onInvestigationChange={onInvestigationChange}
    />
  );

  expect(screen.getByRole("status")).toHaveTextContent("Showing all tasks.");

  await userEvent.click(screen.getByRole("button", { name: "Clear task selection" }));
  expect(onInvestigationChange).toHaveBeenCalledWith({ task: undefined, attempt: 1 });
});

it("uses task keys without parsing IDs or requiring unique display names", async () => {
  const keyedRun: RunListItem = {
    ...run,
    tasks: run.tasks?.map((task) =>
      task.taskID === "task-root"
        ? task
        : {
            ...task,
            name: "Build",
            taskKey: task.taskID === "task-api" ? "compile:linux" : "compile:windows"
          }
    )
  };

  const onInvestigationChange = vi.fn();
  render(
    <RunDetailPage
      onBack={() => undefined}
      run={keyedRun}
      runID={run.id}
      investigation={{ task: "compile:linux", attempt: 1 }}
      onInvestigationChange={onInvestigationChange}
    />
  );

  expect(screen.getByRole("button", { name: "Build: Failed" })).toHaveAttribute("aria-pressed", "true");

  await userEvent.click(screen.getByRole("button", { name: "Build: Planned" }));
  expect(onInvestigationChange).toHaveBeenCalledWith({ task: "compile:windows", attempt: 1 });
});
