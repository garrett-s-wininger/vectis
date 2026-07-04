import type { Meta, StoryObj } from "@storybook/react-vite";
import { ProgressMeter } from "./ProgressMeter";
import { SectionPanel } from "../layout/SectionPanel";

const meta = {
  title: "Components/Data/ProgressMeter",
  component: ProgressMeter,
  args: {
    label: "Log filesystem usage",
    value: 68,
    detail: "219 GB free of 684 GB",
    tone: "warning"
  },
  argTypes: {
    tone: {
      control: "select",
      options: ["neutral", "warning", "critical"]
    },
    value: {
      control: {
        max: 100,
        min: 0,
        type: "range"
      }
    }
  }
} satisfies Meta<typeof ProgressMeter>;

export default meta;

type Story = StoryObj<typeof meta>;

export const StorageUsage: Story = {};

export const Card: Story = {
  args: {
    variant: "card"
  }
};

export const InPanel: Story = {
  render: () => (
    <SectionPanel description="Capacity indicators for log storage." title="Logs">
      <div className="progress-meter-stack">
        <ProgressMeter
          detail="219 GB free of 684 GB"
          label="Filesystem usage"
          tone="warning"
          value={68}
          variant="card"
        />
        <ProgressMeter detail="retention target is 14 days" label="Retention budget" value={42} variant="card" />
        <ProgressMeter
          detail="operator attention required"
          label="Archive pressure"
          tone="critical"
          value={91}
          variant="card"
        />
      </div>
    </SectionPanel>
  )
};
