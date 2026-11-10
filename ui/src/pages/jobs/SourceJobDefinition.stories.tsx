import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import type { Job } from "../../domain/console";
import { PageStoryFrame } from "../../mocks/pageHarnesses";
import { SourceJobDefinition } from "./SourceJobDefinition";

const original = {
  root: { id: "root", uses: "builtins/script", with: { script: "printf 'Starting demo\\n'\nsleep 5\nexit 7" } }
};

const job: Job = {
  id: "hotfix-demo",
  name: "hotfix-demo",
  repository: "monorepo",
  namespacePath: "/team-a",
  branch: "main",
  sourceDetail: ".vectis/hotfix-demo.json",
  sourceKind: "repo",
  definition: JSON.stringify(original, null, 2),
  schedule: "Manual",
  nextRun: "On demand",
  triggers: [],
  status: "enabled",
  canAuthor: false
};

const meta = {
  title: "Pages/Source Job Definition",
  component: SourceJobDefinition,
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ],
  args: { job }
} satisfies Meta<typeof SourceJobDefinition>;

export default meta;
type Story = StoryObj<typeof meta>;

function Demo() {
  const [submitted, setSubmitted] = useState(false);
  return (
    <>
      <SourceJobDefinition
        job={job}
        onRun={async () => {
          setSubmitted(true);
        }}
      />
      {submitted ? <p role="status">One-off run submitted.</p> : null}
    </>
  );
}

export const HotfixDemo: Story = { render: () => <Demo /> };
