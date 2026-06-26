import type { Meta, StoryObj } from "@storybook/react-vite";
import type { Cell } from "../domain/console";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { PageStoryFrame } from "../mocks/pageHarnesses";
import { DashboardPage } from "./DashboardPage";

const data = createMockConsoleDataSnapshot();

const healthyCell = cellByStatus("healthy");
const degradedCell = cellByStatus("degraded");
const offlineCell = cellByStatus("offline");
const missingTelemetryCell = {
  ...healthyCell,
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
} satisfies Cell;

const meta = {
  title: "Pages/Health/CellDetail",
  component: DashboardPage,
  args: {
    cell: healthyCell,
    onOpenHealth: () => undefined
  },
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof DashboardPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Ready: Story = {};

export const Degraded: Story = {
  args: {
    cell: degradedCell
  }
};

export const Offline: Story = {
  args: {
    cell: offlineCell
  }
};

export const MissingTelemetry: Story = {
  args: {
    cell: missingTelemetryCell
  }
};

function cellByStatus(status: Cell["status"]) {
  return data.cells.find((cell) => cell.status === status) ?? data.cells[0];
}
