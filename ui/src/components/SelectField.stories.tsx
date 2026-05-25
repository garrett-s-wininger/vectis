import type { Meta, StoryObj } from "@storybook/react-vite";
import { SelectField } from "./SelectField";

const meta = {
  title: "Components/SelectField",
  component: SelectField,
  args: {
    label: "Status",
    name: "status",
    options: [
      { label: "All", value: "all" },
      { label: "Queued", value: "queued" },
      { label: "Running", value: "running" },
      { label: "Succeeded", value: "succeeded" }
    ],
    value: "all"
  }
} satisfies Meta<typeof SelectField>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
