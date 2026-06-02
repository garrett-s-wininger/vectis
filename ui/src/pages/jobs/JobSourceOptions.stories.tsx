import type { Meta, StoryObj } from "@storybook/react-vite";
import { PageStoryFrame } from "../../mocks/pageHarnesses";
import { JobSourceOptions } from "./JobSourceOptions";

const meta = {
  title: "Pages/Jobs/JobSourceOptions",
  component: JobSourceOptions,
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof JobSourceOptions>;

export default meta;

type Story = StoryObj<typeof meta>;

export const Options: Story = {};
