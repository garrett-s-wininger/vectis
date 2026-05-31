import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { JobsPageHarness, PageStoryFrame } from "../mocks/pageHarnesses";
import { JobsPage } from "./JobsPage";

const data = createMockConsoleDataSnapshot();

const meta = {
  title: "Pages/Jobs",
  component: JobsPage,
  args: {
    jobs: data.jobs,
    namespaces: data.namespaces,
    namespacePath: "/",
    onCreateJob: () => undefined,
    onDeleteJob: () => undefined,
    onSelectNamespace: () => undefined,
    onTriggerRun: () => undefined,
    onUpdateJob: () => undefined
  },
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof JobsPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const CRUD: Story = {
  render: () => <JobsPageHarness />
};

export const NamespaceScoped: Story = {
  render: () => <JobsPageHarness namespacePath="/team-a/edge" />
};

export const PausedJob: Story = {
  render: () => {
    return (
      <JobsPage
        jobs={data.jobs.filter((job) => job.status === "paused")}
        namespaces={data.namespaces}
        namespacePath="/prod"
        onCreateJob={() => undefined}
        onDeleteJob={() => undefined}
        onSelectNamespace={() => undefined}
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
      namespaces={data.namespaces}
      namespacePath="/sandbox"
      onCreateJob={() => undefined}
      onDeleteJob={() => undefined}
      onSelectNamespace={() => undefined}
      onTriggerRun={() => undefined}
      onUpdateJob={() => undefined}
    />
  )
};
