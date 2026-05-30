import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { HealthPageHarness, PageStoryFrame } from "../mocks/pageHarnesses";
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

export const CellDrilldown: Story = {
  args: {
    cells: data.cells,
    onSelectCell: () => undefined,
    selectedCellID: "cell-edge"
  }
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
