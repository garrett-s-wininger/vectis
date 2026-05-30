import type { Meta, StoryObj } from "@storybook/react-vite";
import { storySignals } from "../../mocks/storyFixtures";
import { SectionPanel } from "../layout/SectionPanel";
import { SignalList } from "./SignalList";

const meta = {
  title: "Components/Data/SignalList",
  component: SignalList,
  args: {
    signals: storySignals
  }
} satisfies Meta<typeof SignalList>;

export default meta;

type Story = StoryObj<typeof meta>;

export const ComponentHealth: Story = {};

export const InPanel: Story = {
  render: () => (
    <SectionPanel description="Service availability and capacity signals." title="Component health">
      <SignalList signals={storySignals} />
    </SectionPanel>
  )
};

export const Empty: Story = {
  args: {
    signals: []
  }
};
