import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { PageStoryFrame } from "../../mocks/pageHarnesses";
import { JobTriggerControls } from "./JobTriggerControls";

function JobTriggerControlsStory({
  initialCronSpec = "",
  initialManualEnabled = true,
  initialSchedule = "None"
}: {
  initialCronSpec?: string;
  initialManualEnabled?: boolean;
  initialSchedule?: string;
}) {
  const [cronSpec, setCronSpec] = useState(initialCronSpec);
  const [manualEnabled, setManualEnabled] = useState(initialManualEnabled);
  const [schedule, setSchedule] = useState(initialSchedule);

  return (
    <JobTriggerControls
      cronSpec={cronSpec}
      manualEnabled={manualEnabled}
      onCronSpecChange={setCronSpec}
      onManualChange={setManualEnabled}
      onScheduleChange={setSchedule}
      schedule={schedule}
    />
  );
}

const meta = {
  title: "Pages/Jobs/JobTriggerControls",
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta;

export default meta;

type Story = StoryObj<typeof meta>;

export const ManualOnly: Story = {
  render: () => <JobTriggerControlsStory />
};

export const CustomSchedule: Story = {
  render: () => <JobTriggerControlsStory initialCronSpec="*/15 * * * *" initialSchedule="Custom" />
};

export const PresetSchedule: Story = {
  render: () => <JobTriggerControlsStory initialSchedule="Nightly" />
};
