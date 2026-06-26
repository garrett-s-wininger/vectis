import type { Meta, StoryObj } from "@storybook/react-vite";
import { ResourceStatus } from "./ResourceStatus";
import styles from "./StatusBadge.module.css";

const meta = {
  title: "Components/Status/ResourceStatus",
  component: ResourceStatus,
  args: {
    children: "Healthy",
    tone: "success"
  },
  argTypes: {
    tone: {
      control: "select",
      options: ["success", "warning", "danger", "neutral", "info", "enabled", "paused", "disabled", "active"]
    }
  }
} satisfies Meta<typeof ResourceStatus>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const AllTones: Story = {
  render: () => (
    <div className={styles.row}>
      <ResourceStatus tone="success">Healthy</ResourceStatus>
      <ResourceStatus tone="warning">Degraded</ResourceStatus>
      <ResourceStatus tone="danger">Offline</ResourceStatus>
      <ResourceStatus tone="neutral">Unknown</ResourceStatus>
      <ResourceStatus tone="info">Selected</ResourceStatus>
    </div>
  )
};
