import type { Meta, StoryObj } from "@storybook/react-vite";
import { storyRunListItems } from "../../mocks/storyFixtures";
import { RunList } from "./RunList";

const meta = {
  title: "Components/Data/RunList",
  component: RunList,
  args: {
    title: "Active runs",
    runs: storyRunListItems
  }
} satisfies Meta<typeof RunList>;

export default meta;

type Story = StoryObj<typeof meta>;

export const ActiveRuns: Story = {};

export const VariedContent: Story = {
  args: {
    title: "Runs with varied content",
    runs: [
      { ...storyRunListItems[0], id: "short", jobName: "ci", duration: "1s", status: "running" },
      {
        ...storyRunListItems[0],
        id: "long",
        jobName: "build-and-publish-the-documentation-and-release-artifacts-for-every-supported-platform",
        duration: "12h 59m 59s",
        cellName: "development-cell-with-a-very-long-name",
        submittedBy: "organization-administrator-with-a-long-name",
        status: "succeeded"
      },
      {
        ...storyRunListItems[0],
        id: "unknown-actor",
        jobName: "2a5ed99c-6d51-4c50-97e9-e31b23da7469",
        jobID: "2a5ed99c-6d51-4c50-97e9-e31b23da7469",
        source: "ephemeral",
        runNumber: 2,
        submittedBy: undefined,
        status: "queued"
      },
      { ...storyRunListItems[0], id: "failed", jobName: "integration-tests", status: "failed" }
    ]
  }
};

export const Empty: Story = {
  args: {
    title: "Recent failures",
    runs: []
  }
};
