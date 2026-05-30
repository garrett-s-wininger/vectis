import type { Meta, StoryObj } from "@storybook/react-vite";
import { createMockConsoleDataSnapshot, submitMockEphemeralRun } from "../mocks/consoleData";
import { RunDetailPage } from "./RunDetailPage";

const data = createMockConsoleDataSnapshot();
const ephemeralData = submitMockEphemeralRun(createMockConsoleDataSnapshot(), {
  definition: JSON.stringify(
    {
      id: "database-backfill",
      root: {
        id: "root",
        uses: "builtins/shell",
        with: { command: "echo backfill" }
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
      <main className="storybook-page-main">
        <Story />
      </main>
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

export const MissingRun: Story = {
  args: {
    run: undefined,
    runID: "run-missing"
  }
};
