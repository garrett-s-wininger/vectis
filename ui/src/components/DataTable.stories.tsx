import type { Meta, StoryObj } from "@storybook/react-vite";
import { DataTable, type DataTableColumn } from "./DataTable";

type JobRow = {
  id: string;
  name: string;
  repository: string;
  status: string;
};

const rows: JobRow[] = [
  {
    id: "api",
    name: "api-test-suite",
    repository: "github.com/vectis/api",
    status: "Enabled"
  },
  {
    id: "docs",
    name: "docs-publish",
    repository: "github.com/vectis/docs",
    status: "Paused"
  }
];

const columns: DataTableColumn<JobRow>[] = [
  { header: "Name", cell: (row) => row.name },
  { header: "Repository", cell: (row) => row.repository },
  { header: "Status", cell: (row) => row.status, align: "end" }
];

const meta = {
  title: "Components/DataTable",
  component: DataTable<JobRow>,
  args: {
    columns,
    getRowKey: (row) => row.id,
    rows
  }
} satisfies Meta<typeof DataTable<JobRow>>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Jobs: Story = {};

export const Empty: Story = {
  args: {
    emptyMessage: "No jobs.",
    rows: []
  }
};
