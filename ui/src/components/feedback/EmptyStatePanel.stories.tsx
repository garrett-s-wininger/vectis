import type { Meta, StoryObj } from "@storybook/react-vite";
import { Button } from "../primitives/Button";
import { EmptyStatePanel } from "./EmptyStatePanel";

const meta = {
  title: "Components/Feedback/EmptyStatePanel",
  component: EmptyStatePanel,
  args: {
    actions: <Button>Create</Button>,
    description:
      "Stored jobs are reusable definitions you can trigger manually now and connect to richer sources later.",
    eyebrow: "No stored jobs",
    title: "Create One Today",
    titleID: "empty-state-panel-title"
  }
} satisfies Meta<typeof EmptyStatePanel>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Default: Story = {};
