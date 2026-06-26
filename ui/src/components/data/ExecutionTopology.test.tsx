import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { ExecutionTopology, type ExecutionTopologyCell } from "./ExecutionTopology";

const cells: ExecutionTopologyCell[] = [
  {
    id: "cell-local",
    endpoint: "Route configured",
    name: "local",
    queueDepth: 0,
    status: "healthy",
    workloadLabel: "Stuck",
    workloadValue: "0",
    workers: "N/A"
  }
];

describe("ExecutionTopology", () => {
  it("renders gateway and cell nodes", () => {
    render(<ExecutionTopology cells={cells} countLabel="1 cell" emptyMessage="No cells registered." />);

    expect(screen.getByRole("heading", { name: "Cells" })).toBeInTheDocument();
    expect(screen.getByText("Gateway")).toBeInTheDocument();
    expect(screen.getByText("local")).toBeInTheDocument();
    expect(screen.getByText("Healthy")).toBeInTheDocument();
  });

  it("opens a cell when a node is selected", async () => {
    const onSelectCell = vi.fn();

    render(
      <ExecutionTopology
        cells={cells}
        countLabel="1 cell"
        emptyMessage="No cells registered."
        onSelectCell={onSelectCell}
      />
    );

    await userEvent.click(screen.getByRole("button", { name: "Inspect local" }));

    expect(onSelectCell).toHaveBeenCalledWith("cell-local");
  });

  it("renders an empty state", () => {
    render(<ExecutionTopology cells={[]} countLabel="0 cells" emptyMessage="No cells registered." />);

    expect(screen.getByText("No cells registered.")).toBeInTheDocument();
  });
});
