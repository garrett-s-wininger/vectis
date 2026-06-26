import { render, screen } from "@testing-library/react";
import type { Cell } from "../domain/console";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { DashboardPage } from "./DashboardPage";

const baseCell = createMockConsoleDataSnapshot().cells[0];

describe("DashboardPage", () => {
  it("shows an empty workload state when telemetry is unavailable", () => {
    render(<DashboardPage cell={missingTelemetryCell()} />);

    expect(screen.getByRole("heading", { name: "unreported" })).toBeInTheDocument();
    expect(screen.getByText("No signals to show.")).toBeInTheDocument();
    expect(screen.getByText("No workload telemetry reported.")).toBeInTheDocument();
  });
});

function missingTelemetryCell(): Cell {
  return {
    ...baseCell,
    id: "cell-missing-telemetry",
    name: "unreported",
    endpoint: "Route configured",
    region: "N/A",
    detail: "No component telemetry has been reported for this cell yet.",
    activeRuns: 0,
    queueDepth: 0,
    workersOnline: 0,
    workersTotal: 0,
    components: [],
    progress: []
  };
}
