import type { Meta, StoryObj } from "@storybook/react-vite";
import { MetricCard } from "./MetricCard";

const meta = {
  title: "Components/Data/MetricCard",
  component: MetricCard,
  args: {
    label: "Queue pressure",
    value: "3 queued",
    detail: "0 idle workers available",
    tone: "attention"
  },
  argTypes: {
    tone: {
      control: "select",
      options: ["neutral", "attention", "success"]
    }
  }
} satisfies Meta<typeof MetricCard>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Attention: Story = {};

export const DashboardSet: Story = {
  render: () => (
    <div className="metric-card-grid">
      <MetricCard label="Services" value="Healthy" detail="all systems reporting" tone="success" />
      <MetricCard label="Queue pressure" value="3 queued" detail="0 idle workers" tone="attention" />
      <MetricCard label="Active capacity" value="6 executing" detail="1 worker drained" />
    </div>
  )
};
