import { render, screen } from "@testing-library/react";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { HealthPage } from "./HealthPage";

describe("HealthPage", () => {
  it("summarizes cell status in a single distribution metric", () => {
    const data = createMockConsoleDataSnapshot();

    render(<HealthPage cells={data.cells} onSelectCell={() => undefined} />);

    expect(screen.getByRole("heading", { name: "Health" })).toBeInTheDocument();
    expect(screen.getByText("Execution Topology")).toBeInTheDocument();
    expect(screen.getByText("1 healthy")).toBeInTheDocument();
    expect(screen.getByText("1 degraded")).toBeInTheDocument();
    expect(screen.getByText("1 offline")).toBeInTheDocument();
    expect(screen.queryByText("Offline cells")).not.toBeInTheDocument();
  });
});
