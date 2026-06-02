import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { PageStoryFrame } from "../../mocks/pageHarnesses";
import { emptyJobForm, JobEditor, type JobEditorMode, type JobFormValues } from "./JobEditor";

function JobEditorStory({ mode }: { mode: JobEditorMode }) {
  const [error, setError] = useState("");
  const [values, setValues] = useState<JobFormValues>(() => ({
    ...emptyJobForm,
    cronSpec: "0 2 * * *",
    name: mode.kind === "edit" ? "worker-image" : ""
  }));

  return (
    <JobEditor
      error={error}
      mode={mode}
      namespacePath="/"
      onCancel={() => undefined}
      onCreateJob={() => undefined}
      onError={setError}
      onUpdateJob={() => undefined}
      onValuesChange={setValues}
      values={values}
    />
  );
}

const meta = {
  title: "Pages/Jobs/JobEditor",
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

export const Create: Story = {
  render: () => <JobEditorStory mode={{ kind: "create" }} />
};

export const Configure: Story = {
  render: () => <JobEditorStory mode={{ kind: "edit", jobID: "worker-image" }} />
};
