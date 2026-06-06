import { Clock, Server, User, Zap } from "lucide-react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { OperationalFact } from "./OperationalFact";

const meta = {
  title: "Components/Primitives/OperationalFact",
  component: OperationalFact,
  args: {
    icon: Clock,
    label: "Elapsed",
    value: "4m 12s"
  }
} satisfies Meta<typeof OperationalFact>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const FactSet: Story = {
  render: () => (
    <div style={{ display: "flex", flexWrap: "wrap", gap: "24px" }}>
      <OperationalFact emphasis icon={Clock} label="Elapsed" value="4m 12s" />
      <OperationalFact icon={Server} label="Cell" value="local" />
      <OperationalFact icon={Zap} label="Trigger" value="UI" />
      <OperationalFact icon={User} label="Actor" value="admin" />
    </div>
  )
};
