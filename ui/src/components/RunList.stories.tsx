import type { Meta, StoryObj } from "@storybook/react-vite";
import { RunList, type RunListItem } from "./RunList";

const sampleRuns: RunListItem[] = [
  {
    id: "run-184",
    jobName: "linux-ci",
    runNumber: 184,
    commit: "6f4c2d7a",
    duration: "4m 12s",
    status: "running"
  },
  {
    id: "run-812",
    jobName: "api-smoke",
    runNumber: 812,
    commit: "b82e19f4",
    duration: "6m 03s",
    status: "running"
  },
  {
    id: "run-88",
    jobName: "nightly-load",
    runNumber: 88,
    commit: "a19f03de",
    duration: "14m 02s",
    status: "failed"
  }
];

const meta = {
  title: "Components/RunList",
  component: RunList,
  args: {
    title: "Active runs",
    runs: sampleRuns
  }
} satisfies Meta<typeof RunList>;

export default meta;

type Story = StoryObj<typeof meta>;

export const ActiveRuns: Story = {};

export const Empty: Story = {
  args: {
    title: "Recent failures",
    runs: []
  }
};
