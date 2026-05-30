import type { Meta, StoryObj } from "@storybook/react-vite";
import { SectionPanel } from "./SectionPanel";
import { SignalList, type SignalItem } from "./SignalList";

const signals: SignalItem[] = [
  {
    id: "cron",
    label: "Cron",
    detail: "schedules evaluated 34s ago",
    state: "healthy"
  },
  {
    id: "registry",
    label: "Registry",
    detail: "one peer lagging",
    state: "degraded"
  },
  {
    id: "logs",
    label: "Logs",
    detail: "ingest connected",
    state: "healthy"
  },
  {
    id: "worker-pool",
    label: "Worker pool",
    detail: "one worker drained",
    state: "unknown"
  }
];

const meta = {
  title: "Components/SignalList",
  component: SignalList,
  args: {
    signals
  }
} satisfies Meta<typeof SignalList>;

export default meta;

type Story = StoryObj<typeof meta>;

export const ComponentHealth: Story = {};

export const InPanel: Story = {
  render: () => (
    <SectionPanel description="Service availability and capacity signals." title="Component health">
      <SignalList signals={signals} />
    </SectionPanel>
  )
};

export const Empty: Story = {
  args: {
    signals: []
  }
};
