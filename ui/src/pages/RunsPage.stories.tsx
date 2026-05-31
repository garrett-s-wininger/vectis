import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot } from "../mocks/consoleData";
import { MixedSourceRunsPageHarness, PageStoryFrame, RunsPageHarness } from "../mocks/pageHarnesses";
import { RunsPage } from "./RunsPage";

const data = createMockConsoleDataSnapshot();

const meta = {
  title: "Pages/Runs",
  component: RunsPage,
  args: {
    namespaces: data.namespaces,
    namespacePath: "/",
    onSelectNamespace: () => undefined,
    onSelectRun: () => undefined,
    onSubmitEphemeralRun: () => undefined,
    runs: data.runs
  },
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ]
} satisfies Meta<typeof RunsPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const ActiveRuns: Story = {
  render: () => <RunsPageHarness />
};

export const MixedSources: Story = {
  render: () => <MixedSourceRunsPageHarness />
};

export const NamespaceScoped: Story = {
  render: () => <RunsPageHarness namespacePath="/team-a/edge" />
};

export const Empty: Story = {
  render: () => (
    <RunsPage
      namespaces={data.namespaces}
      namespacePath="/sandbox"
      onSelectNamespace={() => undefined}
      onSelectRun={() => undefined}
      onSubmitEphemeralRun={() => undefined}
      runs={[]}
    />
  )
};
