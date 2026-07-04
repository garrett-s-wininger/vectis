import type { Meta, StoryObj } from "@storybook/react-vite";
import { FormField } from "./FormField";

const meta = {
  title: "Components/Primitives/FormField",
  component: FormField,
  args: {
    label: "Username",
    name: "username",
    placeholder: "admin"
  }
} satisfies Meta<typeof FormField>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Text: Story = {};

export const Password: Story = {
  args: {
    label: "Password",
    name: "password",
    type: "password"
  }
};
