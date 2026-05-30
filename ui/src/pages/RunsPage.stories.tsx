import { useState } from "react";
import type { Meta, StoryObj } from "@storybook/react-vite";
import {
  createMockConsoleDataSnapshot,
  scopeMockConsoleData,
  submitMockEphemeralRun,
  type MockConsoleData
} from "../mocks/consoleData";
import { RunsPage } from "./RunsPage";

const meta = {
  title: "Pages/Runs",
  component: RunsPage,
  args: {
    namespacePath: "/",
    onSelectRun: () => undefined,
    onSubmitEphemeralRun: () => undefined,
    runs: createMockConsoleDataSnapshot().runs
  },
  decorators: [
    (Story) => (
      <main className="console-shell__main">
        <Story />
      </main>
    )
  ]
} satisfies Meta<typeof RunsPage>;

export default meta;

type Story = StoryObj<typeof meta>;

function RunsPageMock({ namespacePath = "/" }: { namespacePath?: string }) {
  const [data, setData] = useState<MockConsoleData>(() =>
    createMockConsoleDataSnapshot()
  );
  const scopedData = scopeMockConsoleData(data, namespacePath);

  return (
    <RunsPage
      namespacePath={namespacePath}
      onSelectRun={() => undefined}
      onSubmitEphemeralRun={(definition) =>
        setData((current) =>
          submitMockEphemeralRun(current, {
            definition,
            namespacePath,
            submittedBy: "admin"
          })
        )
      }
      runs={scopedData.runs}
    />
  );
}

function MixedSourceRunsPage() {
  const [data, setData] = useState<MockConsoleData>(() =>
    submitMockEphemeralRun(createMockConsoleDataSnapshot(), {
      definition: JSON.stringify({ id: "database-backfill", root: {} }, null, 2),
      namespacePath: "/team-a",
      submittedBy: "admin"
    })
  );
  const namespacePath = "/team-a";
  const scopedData = scopeMockConsoleData(data, namespacePath);

  return (
    <RunsPage
      namespacePath={namespacePath}
      onSelectRun={() => undefined}
      onSubmitEphemeralRun={(definition) =>
        setData((current) =>
          submitMockEphemeralRun(current, {
            definition,
            namespacePath,
            submittedBy: "admin"
          })
        )
      }
      runs={scopedData.runs}
    />
  );
}

export const ActiveRuns: Story = {
  render: () => <RunsPageMock />
};

export const MixedSources: Story = {
  render: () => <MixedSourceRunsPage />
};

export const NamespaceScoped: Story = {
  render: () => <RunsPageMock namespacePath="/team-a/edge" />
};

export const Empty: Story = {
  render: () => (
    <RunsPage
      namespacePath="/sandbox"
      onSelectRun={() => undefined}
      onSubmitEphemeralRun={() => undefined}
      runs={[]}
    />
  )
};
