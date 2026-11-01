import type { Meta, StoryObj } from "@storybook/react-vite";
import { RunLogOutput } from "./RunLogs";

const meta = {
  title: "Pages/RunLogOutput",
  component: RunLogOutput,
  args: {
    entries: [
      { sequence: 1, stream: "stdout", data: "Starting task\n" },
      { sequence: 2, stream: "stdout", data: "hel" },
      { sequence: 3, stream: "stdout", data: "lo\n" },
      { sequence: 4, stream: "stderr", data: "Expected demo warning\n" },
      { sequence: 5, stream: "stdout", data: "Finished\n" }
    ]
  }
} satisfies Meta<typeof RunLogOutput>;

export default meta;
type Story = StoryObj<typeof meta>;
export const MixedStreams: Story = {};

export const Overflow: Story = {
  args: {
    entries: Array.from({ length: 100 }, (_, sequence) => ({
      sequence,
      stream: sequence % 5 === 0 ? "stderr" : "stdout",
      data: `Line ${sequence + 1}: task output continues here.\n`
    }))
  }
};
