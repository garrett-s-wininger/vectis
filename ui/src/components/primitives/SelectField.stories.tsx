import type { Meta, StoryObj } from "@storybook/react-vite";
import { SelectField } from "./SelectField";

const meta = {
  title: "Components/Primitives/SelectField",
  component: SelectField,
  args: {
    label: "Status",
    name: "status",
    options: [
      { label: "All", value: "all" },
      { label: "Queued", value: "queued" },
      { label: "Running", value: "running" },
      { label: "Succeeded", value: "succeeded" }
    ],
    value: "all"
  }
} satisfies Meta<typeof SelectField>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const InOverlayPanel: Story = {
  render: () => (
    <section
      className="polished-panel polished-panel--accent-top polished-panel--overlay-safe"
      style={{
        display: "grid",
        gap: "var(--space-lg)",
        padding: "var(--space-xl)",
        width: "min(420px, 100%)"
      }}
    >
      <div>
        <h3 style={{ color: "var(--text-primary)", margin: 0 }}>Overlay Host</h3>
        <p style={{ color: "var(--text-subtle)", margin: "var(--space-xs) 0 0" }}>
          The select menu should escape the polished panel boundary.
        </p>
      </div>
      <SelectField
        defaultValue="queued"
        label="Status"
        name="overlay-status"
        options={[
          { label: "All", value: "all" },
          { label: "Queued", value: "queued" },
          { label: "Running", value: "running" },
          { label: "Succeeded", value: "succeeded" }
        ]}
        wide
      />
    </section>
  )
};
