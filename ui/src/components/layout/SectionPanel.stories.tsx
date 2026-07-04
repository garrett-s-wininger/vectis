import type { Meta, StoryObj } from "@storybook/react-vite";
import { storyRunListItems } from "../../mocks/storyFixtures";
import { Button } from "../primitives/Button";
import { MetricCard } from "../data/MetricCard";
import { RunList } from "../data/RunList";
import { SectionPanel } from "./SectionPanel";

const meta = {
  title: "Components/Layout/SectionPanel",
  component: SectionPanel,
  args: {
    title: "Compute workload",
    description: "Currently executing or queued work.",
    actions: <Button>View all</Button>,
    children: <RunList title="Active runs" runs={storyRunListItems} />
  }
} satisfies Meta<typeof SectionPanel>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Workload: Story = {};

export const Metrics: Story = {
  args: {
    title: "Component health",
    description: "Service availability and capacity signals.",
    children: (
      <div className="metric-card-grid">
        <MetricCard label="Queue" value="3 queued" detail="oldest queued 2m 14s" />
        <MetricCard label="Logs" value="68%" detail="filesystem used" tone="attention" />
        <MetricCard label="Registry" value="Healthy" detail="7 services registered" tone="success" />
      </div>
    )
  }
};
