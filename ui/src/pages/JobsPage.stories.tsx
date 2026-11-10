import { ConsoleAccessProvider, type ConsoleAccess } from "../domain/access";
import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { JobsPageHarness, PageStoryFrame } from "../mocks/pageHarnesses";
import { mockSourceJobs, sourceJobKey } from "../data/sourceJobs";
import { JobsPage } from "./JobsPage";

const data = createMockConsoleDataSnapshot();

const meta = {
  title: "Pages/Jobs",
  component: JobsPage,
  args: {
    jobs: data.jobs,
    repositories: data.repositories,
    namespaces: data.namespaces,
    namespacePath: "/",
    onCloseEditor: () => undefined,
    onCreateJob: () => undefined,
    onOpenCreate: () => undefined,
    onOpenEditor: () => undefined,
    onOpenJob: () => undefined,
    onSelectRun: () => undefined,
    onSelectNamespace: () => undefined,
    onTriggerRun: () => undefined,
    onUpdateJob: () => undefined,
    runs: data.runs
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

export const RowNavigation: Story = {
  render: () => <RowNavigationStory />
};

export const PausedJob: Story = {
  render: () => {
    return (
      <JobsPage
        jobs={data.jobs.filter((job) => job.status === "paused")}
        namespaces={data.namespaces}
        namespacePath="/prod"
        onCloseEditor={() => undefined}
        onCreateJob={() => undefined}
        onOpenCreate={() => undefined}
        onOpenEditor={() => undefined}
        onOpenJob={() => undefined}
        onSelectRun={() => undefined}
        onSelectNamespace={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={data.runs}
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
      onCloseEditor={() => undefined}
      onCreateJob={() => undefined}
      onOpenCreate={() => undefined}
      onOpenEditor={() => undefined}
      onOpenJob={() => undefined}
      onSelectRun={() => undefined}
      onSelectNamespace={() => undefined}
      onTriggerRun={() => undefined}
      onUpdateJob={() => undefined}
      runs={data.runs}
    />
  )
};

function RowNavigationStory() {
  const [detailJobID, setDetailJobID] = useState<string | undefined>();

  return (
    <JobsPage
      allJobs={data.jobs}
      detailJobID={detailJobID}
      jobs={data.jobs}
      namespaces={data.namespaces}
      namespacePath="/"
      onCloseEditor={() => setDetailJobID(undefined)}
      onCreateJob={() => undefined}
      onOpenCreate={() => undefined}
      onOpenEditor={() => undefined}
      onOpenJob={(jobID) => setDetailJobID(jobID || undefined)}
      onOpenJobRuns={() => undefined}
      onSelectRun={() => undefined}
      onSelectNamespace={() => undefined}
      onTriggerRun={() => undefined}
      onUpdateJob={() => undefined}
      runs={data.runs}
    />
  );
}

const operatorAccess: ConsoleAccess = {
  actions: ["job:read", "run:read", "run:trigger", "run:operator", "api:any"],
  namespaces: { "/team-a": ["job:read", "run:read", "run:trigger", "run:operator"] }
};

export const Operator: Story = {
  render: () => (
    <ConsoleAccessProvider access={operatorAccess}>
      <JobsPageHarness namespacePath="/team-a" />
    </ConsoleAccessProvider>
  )
};

export const Viewer: Story = {
  render: () => (
    <ConsoleAccessProvider
      access={{ actions: ["job:read", "run:read", "api:any"], namespaces: { "/team-a": ["job:read", "run:read"] } }}
    >
      <JobsPageHarness namespacePath="/team-a" />
    </ConsoleAccessProvider>
  )
};

export const LongIdentifiers: Story = {
  render: (args) => {
    const job = {
      ...data.jobs[0],
      id: "release-source-validation-".repeat(5),
      name: "release-validation",
      sourceCommit: "0123456789abcdef".repeat(4)
    };

    return <JobsPage {...args} jobs={[job]} detailJobID={job.id} />;
  }
};

const hotfixSource = { root: { id: "root", uses: "builtins/script", with: { script: "npm run test\nexit 7" } } };
const hotfixDefinition = { root: { id: "root", uses: "builtins/script", with: { script: "npm run test\nexit 0" } } };
const workspaceJob = {
  ...data.jobs[0]!,
  id: "hotfix-workspace",
  name: "api-test-suite",
  canAuthor: false,
  sourceCommit: "0123456789abcdef".repeat(4),
  definition: JSON.stringify(hotfixSource),
  hotfix: {
    commit: "2222222222222222222222222222222222222222",
    reason: "Repair the release script",
    actor: "admin",
    base_commit: "1111111111111111111111111111111111111111",
    base_ref: "main",
    base_path: data.jobs[0]!.sourceDetail,
    created_at: 1793664000
  }
};

export const HotfixActive: Story = {
  args: {
    jobs: [workspaceJob],
    detailJobID: workspaceJob.id,
    runs: Array.from({ length: 5 }, (_, index) => ({
      ...data.runs[0]!,
      id: `workspace-run-${index}`,
      jobID: workspaceJob.id,
      jobName: workspaceJob.name,
      namespacePath: workspaceJob.namespacePath,
      repositoryID: workspaceJob.repository,
      runNumber: 1240 - index,
      status: (["running", "failed", "succeeded", "cancelled", "succeeded"] as const)[index]!,
      duration: ["4m 12s", "2m 05s", "3m 11s", "1m 40s", "3m 02s"][index]!,
      startedAt: new Date(Date.UTC(2026, 10, 5, 16 - index)).toISOString()
    }))
  },
  loaders: [
    () => {
      mockSourceJobs.set(sourceJobKey(workspaceJob.repository, workspaceJob.id), {
        registration: {
          repository_id: workspaceJob.repository,
          job_id: workspaceJob.id,
          namespace_id: 2,
          namespace: workspaceJob.namespacePath,
          path: workspaceJob.sourceDetail,
          ref: "main",
          revision: 1,
          hotfix: workspaceJob.hotfix
        },
        source_commit: workspaceJob.sourceCommit,
        source_json: JSON.stringify(hotfixSource),
        source_definition: hotfixSource,
        effective_definition: hotfixDefinition
      });

      return {};
    }
  ]
};
