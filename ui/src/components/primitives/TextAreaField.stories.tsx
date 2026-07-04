import type { Meta, StoryObj } from "@storybook/react-vite";
import { TextAreaField } from "./TextAreaField";

const meta = {
  title: "Components/Primitives/TextAreaField",
  component: TextAreaField,
  args: {
    label: "Definition JSON",
    name: "definition",
    rows: 6,
    value: JSON.stringify({ id: "ad-hoc-check", root: {} }, null, 2)
  }
} satisfies Meta<typeof TextAreaField>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Wide: Story = {
  args: {
    wide: true
  }
};
