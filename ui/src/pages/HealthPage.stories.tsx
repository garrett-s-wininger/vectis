import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { HealthPageHarness, PageStoryFrame } from "../mocks/pageHarnesses";
import { DashboardPage } from "./DashboardPage";
import { HealthPage } from "./HealthPage";

const data = createMockConsoleDataSnapshot();

const meta = {
  title: "Pages/Health",
  component: HealthPage,
  args: {
    cells: data.cells,
    onSelectCell: () => undefined
  },
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof HealthPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Cluster: Story = {
  render: () => <HealthPageHarness cells={data.cells} />
};

export const SingleCellCluster: Story = {
  args: {
    cells: data.cells.slice(0, 1),
    onSelectCell: () => undefined
  }
};

export const CellDrilldown: Story = {
  render: () => (
    <DashboardPage cell={data.cells.find((cell) => cell.id === "cell-edge") ?? data.cells[0]} onOpenHealth={() => undefined} />
  )
};

export const OfflineCell: Story = {
  args: {
    cells: data.cells.filter((cell) => cell.status === "offline"),
    onSelectCell: () => undefined
  }
};

export const Empty: Story = {
  args: {
    cells: [],
    onSelectCell: () => undefined
  }
};
