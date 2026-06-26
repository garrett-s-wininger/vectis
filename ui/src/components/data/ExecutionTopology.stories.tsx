import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../../mocks/consoleData";
import { ExecutionTopology, type ExecutionTopologyCell } from "./ExecutionTopology";

const data = createMockConsoleDataSnapshot();

const cells = data.cells.map((cell) => ({
  id: cell.id,
  endpoint: cell.endpoint,
  name: cell.name,
  queueDepth: cell.queueDepth,
  status: cell.status,
  workloadLabel: cell.stuckRuns === undefined ? "Runs" : "Stuck",
  workloadValue: String(cell.stuckRuns ?? cell.activeRuns),
  workers: cell.workersTotal > 0 ? `${cell.workersOnline}/${cell.workersTotal}` : "N/A"
})) satisfies ExecutionTopologyCell[];

const singleHealthyCell = cells.filter((cell) => cell.status === "healthy").slice(0, 1);
const degradedAndOfflineCells = cells.filter((cell) => cell.status !== "healthy");

const meta = {
  title: "Components/Data/ExecutionTopology",
  component: ExecutionTopology,
  args: {
    cells,
    countLabel: "3 cells",
    emptyMessage: "No cells registered.",
    onSelectCell: () => undefined
  }
} satisfies Meta<typeof ExecutionTopology>;

export default meta;

type Story = StoryObj<typeof meta>;

export const MixedStatus: Story = {};

export const SingleHealthyCell: Story = {
  args: {
    cells: singleHealthyCell,
    countLabel: "1 cell"
  }
};

export const DegradedAndOfflineCells: Story = {
  args: {
    cells: degradedAndOfflineCells,
    countLabel: "2 cells"
  }
};

export const Empty: Story = {
  args: {
    cells: [],
    countLabel: "0 cells"
  }
};
