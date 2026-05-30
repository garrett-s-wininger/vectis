import type { Meta, StoryObj } from "@storybook/react-vite";
import { Button } from "../primitives/Button";
import { PageHeader } from "./PageHeader";

const meta = {
  title: "Components/Layout/PageHeader",
  component: PageHeader,
  args: {
    eyebrow: "Operator Console",
    title: "Cluster dashboard",
    description: "Current workload, queue pressure, and service health.",
    actions: <Button>Refresh</Button>
  }
} satisfies Meta<typeof PageHeader>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Dashboard: Story = {};

export const WithoutActions: Story = {
  args: {
    title: "Runs",
    description: "Review recent execution history and failure states.",
    actions: undefined
  }
};
