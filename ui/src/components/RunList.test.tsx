import { render, screen } from "@testing-library/react";
import { RunList, type RunListItem } from "./RunList";

const runs: RunListItem[] = [
  {
    id: "run-184",
    jobName: "linux-ci",
    runNumber: 184,
    commit: "6f4c2d7a",
    duration: "4m 12s",
    status: "running"
  }
];

describe("RunList", () => {
  it("renders run summaries with status badges", () => {
    render(<RunList title="Active runs" runs={runs} />);

    expect(screen.getByRole("heading", { name: "Active runs" })).toBeInTheDocument();
    expect(screen.getByText("linux-ci")).toBeInTheDocument();
    expect(screen.getByText(/6f4c2d7a/)).toBeInTheDocument();
    expect(screen.getByText("Running")).toBeInTheDocument();
  });

  it("renders an empty state when no runs are present", () => {
    render(<RunList title="Recent failures" runs={[]} />);

    expect(screen.getByText("No runs to show.")).toBeInTheDocument();
  });
});
