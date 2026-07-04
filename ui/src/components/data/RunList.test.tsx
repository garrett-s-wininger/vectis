import { fireEvent, render, screen } from "@testing-library/react";
import { RunList, type RunListItem } from "./RunList";

const runs: RunListItem[] = [
  {
    id: "run-184",
    jobName: "linux-ci",
    runNumber: 184,
    cellName: "local",
    commit: "6f4c2d7a",
    duration: "4m 12s",
    namespacePath: "/team-a",
    source: "ephemeral",
    submittedBy: "admin",
    trigger: "manual",
    status: "running"
  }
];

describe("RunList", () => {
  it("renders run summaries with status badges", () => {
    render(<RunList title="Active runs" runs={runs} />);

    expect(screen.getByRole("heading", { name: "Active runs" })).toBeInTheDocument();
    expect(screen.getByText("linux-ci")).toBeInTheDocument();
    expect(screen.getByText("Elapsed")).toBeInTheDocument();
    expect(screen.getByText("4m 12s")).toBeInTheDocument();
    expect(screen.getByText("Cell")).toBeInTheDocument();
    expect(screen.getByText("local")).toBeInTheDocument();
    expect(screen.getByText("Trigger")).toBeInTheDocument();
    expect(screen.getByText("Manual")).toBeInTheDocument();
    expect(screen.getByText("Actor")).toBeInTheDocument();
    expect(screen.getByText("admin")).toBeInTheDocument();
    expect(screen.getByText("Running")).toBeInTheDocument();
    expect(screen.queryByText("Ephemeral")).not.toBeInTheDocument();
    expect(screen.queryByText("Namespace")).not.toBeInTheDocument();
    expect(screen.queryByText("Commit")).not.toBeInTheDocument();
  });

  it("can avoid restating the job name in scoped lists", () => {
    render(<RunList hideJobName title="linux-ci runs" runs={runs} />);

    expect(screen.getByRole("heading", { name: "linux-ci runs" })).toBeInTheDocument();
    expect(screen.getByText("Run #184")).toBeInTheDocument();
    expect(screen.queryByText("linux-ci")).not.toBeInTheDocument();
  });

  it("opens a run from the row", () => {
    const selectRun = vi.fn();

    render(<RunList onSelectRun={selectRun} title="Active runs" runs={runs} />);

    fireEvent.click(screen.getByRole("button", { name: "Open run linux-ci #184" }));

    expect(selectRun).toHaveBeenCalledWith("run-184");
  });

  it("uses a concise label for generated inline runs", () => {
    const selectRun = vi.fn();

    render(
      <RunList
        onSelectRun={selectRun}
        title="Active runs"
        runs={[
          {
            ...runs[0],
            id: "run-2a5ed99c-6d51-4c50-97e9-e31b23da7469",
            jobName: "2a5ed99c-6d51-4c50-97e9-e31b23da7469",
            runNumber: 1,
            source: "ephemeral"
          }
        ]}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Open run 97e9-e31b23da7469 #1" }));

    expect(screen.getByText("97e9-e31b23da7469")).toBeInTheDocument();
    expect(screen.queryByText("2a5ed99c-6d51-4c50-97e9-e31b23da7469")).not.toBeInTheDocument();
    expect(selectRun).toHaveBeenCalledWith("run-2a5ed99c-6d51-4c50-97e9-e31b23da7469");
  });

  it("renders an empty state when no runs are present", () => {
    render(<RunList title="Recent failures" runs={[]} />);

    expect(screen.getByText("No runs match the current filters.")).toBeInTheDocument();
  });
});
