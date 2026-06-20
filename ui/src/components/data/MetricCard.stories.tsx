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
    },
    variant: {
      control: "select",
      options: ["default", "plain"]
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

export const PlainSet: Story = {
  render: () => (
    <div className="metric-card-grid">
      <MetricCard detail="Immediate descendants" label="Child Namespaces" value="2" variant="plain" />
      <MetricCard detail="Directly organized here" label="Stored Jobs" value="8" variant="plain" />
      <MetricCard detail="Inherited boundary" label="Access" value="Admin" variant="plain" />
    </div>
  )
};
