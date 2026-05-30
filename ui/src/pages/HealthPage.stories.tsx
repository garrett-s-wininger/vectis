import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import {
  createMockConsoleDataSnapshot,
  type MockCell
} from "../mocks/consoleData";
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
      <main className="storybook-page-main">
        <Story />
      </main>
    )
  ]
} satisfies Meta<typeof HealthPage>;

export default meta;

type Story = StoryObj<typeof meta>;

function HealthPageMock({ cells }: { cells: MockCell[] }) {
  const [selectedCellID, setSelectedCellID] = useState<string | undefined>();

  return (
    <HealthPage
      cells={cells}
      onSelectCell={setSelectedCellID}
      selectedCellID={selectedCellID}
    />
  );
}

export const Cluster: Story = {
  render: () => <HealthPageMock cells={data.cells} />
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
