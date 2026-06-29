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

export const OverlayBoundary: Story = {
  render: () => (
    <div style={{ display: "grid", gap: "var(--space-lg)", width: "min(var(--layout-panel-max), 100%)" }}>
      <FilterBar actions={<Button>Refresh</Button>} filters={filters} />
      <section className="polished-panel" style={{ minHeight: 112, padding: "var(--space-xl)" }}>
        <strong>Following panel</strong>
        <p style={{ color: "var(--text-subtle)", margin: "var(--space-xs) 0 0" }}>
          Open the status menu above to confirm it renders over this panel rather than clipping at the filter bar edge.
        </p>
      </section>
    </div>
  )
};
