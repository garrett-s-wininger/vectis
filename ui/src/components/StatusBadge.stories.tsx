import type { Meta, StoryObj } from "@storybook/react-vite";
import { StatusBadge } from "./StatusBadge";

const meta = {
  title: "Components/StatusBadge",
  component: StatusBadge,
  args: {
    status: "running"
  },
  argTypes: {
    status: {
      control: "select",
      options: ["queued", "running", "succeeded", "failed", "cancelled", "abandoned"]
    }
  }
} satisfies Meta<typeof StatusBadge>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Running: Story = {};

export const AllStatuses: Story = {
  render: () => (
    <div className="status-badge-row">
      <StatusBadge status="queued" />
      <StatusBadge status="running" />
      <StatusBadge status="succeeded" />
      <StatusBadge status="failed" />
      <StatusBadge status="cancelled" />
      <StatusBadge status="abandoned" />
    </div>
  )
};
