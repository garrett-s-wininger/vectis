import type { Meta, StoryObj } from "@storybook/react-vite";
import { Button } from "../primitives/Button";
import { MetricCard } from "../data/MetricCard";
import { RunList, type RunListItem } from "../data/RunList";
import { SectionPanel } from "./SectionPanel";

const runs: RunListItem[] = [
  {
    id: "run-184",
    jobName: "linux-ci",
    runNumber: 184,
    commit: "6f4c2d7a",
    duration: "4m 12s",
    status: "running"
  },
  {
    id: "run-88",
    jobName: "nightly-load",
    runNumber: 88,
    commit: "a19f03de",
    duration: "14m 02s",
    status: "failed"
  }
];

const meta = {
  title: "Components/Layout/SectionPanel",
  component: SectionPanel,
  args: {
    title: "Compute workload",
    description: "Currently executing or queued work.",
    actions: <Button>View all</Button>,
    children: <RunList title="Active runs" runs={runs} />
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
