import { render, screen } from "@testing-library/react";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { HealthPage } from "./HealthPage";

describe("HealthPage", () => {
  it("summarizes cell status in a single distribution metric", () => {
    const data = createMockConsoleDataSnapshot();

    render(<HealthPage cells={data.cells} onSelectCell={() => undefined} />);

    expect(screen.getByRole("heading", { name: "Health" })).toBeInTheDocument();
    expect(screen.getByLabelText("Health location")).toHaveTextContent("ClusterHealth");
    expect(screen.queryByText("Root")).not.toBeInTheDocument();
    expect(screen.getByText("Execution Topology")).toBeInTheDocument();
    expect(screen.getByText("1 healthy")).toBeInTheDocument();
    expect(screen.getByText("1 degraded")).toBeInTheDocument();
    expect(screen.getByText("1 offline")).toBeInTheDocument();
    expect(screen.queryByText("Offline cells")).not.toBeInTheDocument();
  });
});

it("retains the Health header and cards while cell data loads, without reporting false empty health", () => {
  const data = createMockConsoleDataSnapshot();
  const { rerender } = render(<HealthPage cells={[]} loading onSelectCell={() => undefined} />);
  const heading = screen.getByRole("heading", { name: "Health" });
  const metrics = screen.getByLabelText("Cluster health");
  expect(metrics).toHaveAttribute("aria-busy", "true");
  expect(screen.getByText("Loading cell status…")).toBeInTheDocument();
  expect(screen.queryByText("No cells registered.")).not.toBeInTheDocument();
  expect(screen.queryByText("0 healthy")).not.toBeInTheDocument();

  rerender(<HealthPage cells={data.cells} onSelectCell={() => undefined} />);
  expect(screen.getByRole("heading", { name: "Health" })).toBe(heading);
  expect(screen.getByLabelText("Cluster health")).toBe(metrics);
  expect(metrics).toHaveAttribute("aria-busy", "false");
  expect(screen.getByText("1 healthy")).toBeInTheDocument();
});
