import type { Meta, StoryObj } from "@storybook/react-vite";
import { ErrorAlert } from "./ErrorAlert";

const meta = {
  title: "Components/Feedback/ErrorAlert",
  component: ErrorAlert,
  args: {
    message: "Unable to trigger job. The saved definition could not be enqueued.",
    title: "Action Failed"
  }
} satisfies Meta<typeof ErrorAlert>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};

export const Untitled: Story = {
  args: {
    message: "Invalid username or password.",
    title: undefined
  }
};
