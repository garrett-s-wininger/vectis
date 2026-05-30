import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import {
  createMockConsoleDataSnapshot,
  createMockJob,
  deleteMockJob,
  scopeMockConsoleData,
  triggerMockRun,
  updateMockJob,
  type MockConsoleData
} from "../mocks/consoleData";
import { JobsPage } from "./JobsPage";

const data = createMockConsoleDataSnapshot();

const meta = {
  title: "Pages/Jobs",
  component: JobsPage,
  args: {
    jobs: data.jobs,
    namespacePath: "/",
    onCreateJob: () => undefined,
    onDeleteJob: () => undefined,
    onTriggerRun: () => undefined,
    onUpdateJob: () => undefined
  },
  decorators: [
    (Story) => (
      <main className="storybook-page-main">
        <Story />
      </main>
    )
  ]
} satisfies Meta<typeof JobsPage>;

export default meta;

type Story = StoryObj<typeof meta>;

function JobsPageMock({ namespacePath = "/" }: { namespacePath?: string }) {
  const [data, setData] = useState<MockConsoleData>(() => createMockConsoleDataSnapshot());
  const scopedData = scopeMockConsoleData(data, namespacePath);

  return (
    <JobsPage
      jobs={scopedData.jobs}
      namespacePath={namespacePath}
      onCreateJob={(input) => setData((current) => createMockJob(current, input))}
      onDeleteJob={(jobID) => setData((current) => deleteMockJob(current, jobID))}
      onTriggerRun={(jobID) => setData((current) => triggerMockRun(current, jobID))}
      onUpdateJob={(jobID, input) => setData((current) => updateMockJob(current, jobID, input))}
    />
  );
}

export const CRUD: Story = {
  render: () => <JobsPageMock />
};

export const NamespaceScoped: Story = {
  render: () => <JobsPageMock namespacePath="/team-a/edge" />
};

export const PausedJob: Story = {
  render: () => {
    return (
      <JobsPage
        jobs={data.jobs.filter((job) => job.status === "paused")}
        namespacePath="/prod"
        onCreateJob={() => undefined}
        onDeleteJob={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
      />
    );
  }
};

export const Empty: Story = {
  render: () => (
    <JobsPage
      jobs={[]}
      namespacePath="/sandbox"
      onCreateJob={() => undefined}
      onDeleteJob={() => undefined}
      onTriggerRun={() => undefined}
      onUpdateJob={() => undefined}
    />
  )
};
