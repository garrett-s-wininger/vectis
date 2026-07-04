import type { Meta, StoryObj } from "@storybook/react-vite";
import { storyJobColumns, storyJobRows, type JobTableFixtureRow } from "../../mocks/storyFixtures";
import { DataTable } from "./DataTable";

const meta = {
  title: "Components/Data/DataTable",
  component: DataTable<JobTableFixtureRow>,
  args: {
    columns: storyJobColumns,
    getRowKey: (row) => row.id,
    rows: storyJobRows
  }
} satisfies Meta<typeof DataTable<JobTableFixtureRow>>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Jobs: Story = {};

export const Empty: Story = {
  args: {
    emptyMessage: "No jobs.",
    rows: []
  }
};
