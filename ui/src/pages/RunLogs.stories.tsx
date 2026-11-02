import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { Button } from "../components";
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

function GrowingOutputPreview() {
  const [lines, setLines] = useState(0);
  return (
    <>
      <Button onClick={() => setLines(lines === 0 ? 1 : 100)}>Append output</Button>
      <RunLogOutput
        entries={Array.from({ length: lines }, (_, sequence) => ({
          sequence,
          stream: "stdout",
          data: `Line ${sequence + 1}\n`
        }))}
      />
      <p>Content after logs</p>
    </>
  );
}

export const GrowingOutput: Story = { render: () => <GrowingOutputPreview /> };

export const Overflow: Story = {
  args: {
    entries: Array.from({ length: 100 }, (_, sequence) => ({
      sequence,
      stream: sequence % 5 === 0 ? "stderr" : "stdout",
      data: `Line ${sequence + 1}: task output continues here.\n`
    }))
  }
};
