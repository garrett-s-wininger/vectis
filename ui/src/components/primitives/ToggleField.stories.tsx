import type { Meta, StoryObj } from "@storybook/react-vite";
import { ToggleField } from "./ToggleField";

const meta = {
  component: ToggleField,
  title: "Components/ToggleField"
} satisfies Meta<typeof ToggleField>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Enabled: Story = {
  args: {
    checked: true,
    label: "State",
    name: "state",
    offText: "Paused",
    onChange: () => undefined,
    onText: "Enabled"
  }
};

export const Disabled: Story = {
  args: {
    checked: false,
    label: "Manual",
    name: "manual",
    offText: "Off",
    onChange: () => undefined,
    onText: "Allowed"
  }
};
