import type { Meta, StoryObj } from "@storybook/react-vite";
import { Button } from "../primitives/Button";
import { FilterBar } from "./FilterBar";
import { FormField } from "../primitives/FormField";
import { SectionPanel } from "../layout/SectionPanel";
import { SelectField } from "../primitives/SelectField";

const filters = (
  <>
    <FormField label="Search" placeholder="Search jobs or runs" type="search" />
    <SelectField
      defaultValue="running"
      label="Status"
      options={[
        { label: "All statuses", value: "all" },
        { label: "Queued", value: "queued" },
        { label: "Running", value: "running" },
        { label: "Failed", value: "failed" },
        { label: "Succeeded", value: "succeeded" }
      ]}
    />
  </>
);

const meta = {
  title: "Components/Navigation/FilterBar",
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
