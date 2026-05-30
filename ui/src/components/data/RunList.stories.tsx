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

export const Empty: Story = {
  args: {
    title: "Recent failures",
    runs: []
  }
};
