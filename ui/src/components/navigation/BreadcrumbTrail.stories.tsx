import type { Meta, StoryObj } from "@storybook/react-vite";
import { BreadcrumbTrail } from "./BreadcrumbTrail";

const meta = {
  title: "Components/Navigation/BreadcrumbTrail",
  component: BreadcrumbTrail,
  args: {
    label: "Job location",
    items: [{ label: "Root" }, { label: "Jobs", onClick: () => undefined }, { label: "docs-publish", current: true }]
  }
} satisfies Meta<typeof BreadcrumbTrail>;

export default meta;

type Story = StoryObj<typeof meta>;

export const JobsIndex: Story = {
  args: {
    label: "Jobs location",
    items: [{ label: "Root" }, { label: "Jobs", current: true }]
  }
};

export const JobDetail: Story = {
  args: {
    items: [
      { label: "/team-a/edge" },
      { label: "Jobs", onClick: () => undefined },
      { label: "docs-publish", current: true }
    ]
  }
};

export const JobConfig: Story = {
  args: {
    label: "Job editor location",
    items: [
      { label: "/team-a/edge" },
      { label: "Jobs", onClick: () => undefined },
      { label: "docs-publish", onClick: () => undefined },
      { label: "Configure", current: true }
    ]
  }
};

export const LongLabels: Story = {
  args: {
    items: [
      { label: "/platform/data-science/model-training/east" },
      { label: "Jobs", onClick: () => undefined },
      { label: "nightly-foundation-model-evaluation-and-reporting", current: true }
    ]
  }
};
