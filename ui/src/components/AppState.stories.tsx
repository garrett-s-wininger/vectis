import type { Meta, StoryObj } from "@storybook/react-vite";
import { Button } from "./Button";
import { AppState } from "./AppState";

const meta = {
  title: "Components/AppState",
  component: AppState,
  args: {
    title: "No runs to show",
    description: "Runs will appear here after a job is triggered.",
    tone: "empty"
  }
} satisfies Meta<typeof AppState>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Empty: Story = {};

export const Error: Story = {
  args: {
    actions: <Button>Retry</Button>,
    description: "The request did not complete.",
    title: "Unable to load runs",
    tone: "error"
  }
};
