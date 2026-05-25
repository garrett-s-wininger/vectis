import type { Meta, StoryObj } from "@storybook/react-vite";
import { FormError } from "./FormError";

const meta = {
  title: "Components/FormError",
  component: FormError,
  args: {
    message: "Invalid username or password."
  }
} satisfies Meta<typeof FormError>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
