import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot, submitMockEphemeralRun } from "../mocks/consoleData";
import { mockRunTasks } from "../mocks/fixtures";
import { PageStoryFrame } from "../mocks/pageHarnesses";
import { RunDetailPage } from "./RunDetailPage";

const data = createMockConsoleDataSnapshot();
const ephemeralData = submitMockEphemeralRun(createMockConsoleDataSnapshot(), {
  definition: JSON.stringify(
    {
      id: "database-backfill",
      root: {
        id: "root",
        uses: "builtins/script",
        with: { script: "echo backfill" }
      }
    },
    null,
    2
  ),
  namespacePath: "/team-a",
  submittedBy: "admin"
});

const meta = {
  title: "Pages/Run Detail",
  component: RunDetailPage,
  decorators: [
    (Story) => (
      <PageStoryFrame>
        <Story />
      </PageStoryFrame>
    )
  ],
  args: {
    onBack: () => undefined,
    run: data.runs[0],
    runID: data.runs[0].id
  }
} satisfies Meta<typeof RunDetailPage>;

export default meta;

type Story = StoryObj<typeof meta>;

export const StoredRun: Story = {
  args: {
    run: data.runs[0],
    runID: data.runs[0].id
  }
};

export const EphemeralRun: Story = {
  args: {
    run: ephemeralData.runs[0],
    runID: ephemeralData.runs[0].id
  }
};

export const RunningRun: Story = {
  args: {
    run: {
      ...data.runs[0],
      duration: "18s",
      finishedAt: undefined,
      id: "run-running",
      status: "running",
      tasks: mockRunTasks("run-running", [
        { key: "root", name: "api-test-suite", status: "running" },
        { key: "checkout", name: "Checkout", parentKey: "root", status: "succeeded" },
        { key: "unit", name: "Unit tests", parentKey: "root", status: "succeeded" },
        { key: "api", name: "API tests", parentKey: "root", status: "running" },
        { key: "api-auth", name: "Auth routes", parentKey: "api", status: "succeeded" },
        { key: "api-jobs", name: "Job routes", parentKey: "api", status: "running" },
        { key: "report", name: "Report", parentKey: "root", status: "planned" }
      ])
    },
    runID: "run-running"
  }
};

export const FailedTaskTree: Story = {
  args: {
    run: {
      ...data.runs[0],
      duration: "42s",
      finishedAt: "2026-05-31T12:00:42Z",
      id: "run-failed",
      status: "failed",
      tasks: mockRunTasks("run-failed", [
        { key: "root", name: "api-test-suite", status: "failed" },
        { key: "checkout", name: "Checkout", parentKey: "root", status: "succeeded" },
        { key: "unit", name: "Unit tests", parentKey: "root", status: "succeeded" },
        { key: "api", name: "API tests", parentKey: "root", status: "failed" },
        { key: "report", name: "Report", parentKey: "root", status: "planned" }
      ])
    },
    runID: "run-failed"
  }
};

export const NoTaskTelemetry: Story = {
  args: {
    run: {
      ...data.runs[2],
      id: "run-legacy",
      tasks: undefined
    },
    runID: "run-legacy"
  }
};

export const MissingRun: Story = {
  args: {
    run: undefined,
    runID: "run-missing"
  }
};
