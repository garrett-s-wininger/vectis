import type { Meta, StoryObj } from "@storybook/react-vite";
import { Button } from "./Button";
import { FilterBar } from "./FilterBar";
import { SectionPanel } from "./SectionPanel";

const filters = (
  <>
    <label className="field">
      <span>Search</span>
      <input placeholder="Search jobs or runs" type="search" />
    </label>
    <label className="field">
      <span>Status</span>
      <select defaultValue="running">
        <option value="all">All statuses</option>
        <option value="queued">Queued</option>
        <option value="running">Running</option>
        <option value="failed">Failed</option>
        <option value="succeeded">Succeeded</option>
      </select>
    </label>
  </>
);

const meta = {
  title: "Components/FilterBar",
  component: FilterBar,
  args: {
    filters,
    actions: <Button>Refresh</Button>
  }
} satisfies Meta<typeof FilterBar>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Runs: Story = {};

export const InPanel: Story = {
  render: () => (
    <SectionPanel description="Narrow execution history." title="Runs">
      <FilterBar actions={<Button>Refresh</Button>} filters={filters} />
    </SectionPanel>
  )
};
