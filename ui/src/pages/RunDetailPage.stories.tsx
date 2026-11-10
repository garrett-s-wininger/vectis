import type { Meta, StoryObj } from "@storybook/react-vite";
import { useState } from "react";
import { Button, type RunListItem } from "../components";
import { retainRunTasks } from "../data/watchRun";
import { createMockConsoleDataSnapshot, submitMockEphemeralRun } from "../mocks/consoleData";
import { mockRunTasks } from "../mocks/fixtures";
import { PageStoryFrame } from "../mocks/pageHarnesses";
import { RunDetailPage } from "./RunDetailPage";
import { artifactRun, loadMockArtifacts } from "../mocks/runArtifacts";
import { retriedArtifactRun, loadRetriedArtifacts } from "../mocks/runAttempts";

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

export const WithArtifacts: Story = {
  args: { run: artifactRun, runID: artifactRun.id, loadArtifacts: loadMockArtifacts }
};

export const StoredRun: Story = {
  args: {
    run: data.runs[0],
    runID: data.runs[0].id
  }
};

export const CapturedDefinition: Story = {
  args: {
    run: { ...data.runs[0], definition: undefined },
    loadDefinition: async (runID) => ({
      run_id: runID,
      definition_version: 3,
      definition: {
        root: { id: "root", uses: "builtins/script", with: { script: "echo captured definition" } }
      },
      source: {
        repository_id: "monorepo",
        requested_ref: "main",
        resolved_commit: "1234567890abcdef1234567890abcdef12345678",
        path: ".vectis/platform.build.json"
      }
    })
  }
};

export const EphemeralRun: Story = {
  args: {
    run: ephemeralData.runs[0],
    runID: ephemeralData.runs[0].id
  }
};

function TaskCompletionPreview({ partial = false }: { partial?: boolean }) {
  const [stage, setStage] = useState(0);
  const run: RunListItem = {
    ...data.runs[0],
    status: stage === 2 ? "succeeded" : "running",
    tasks: [
      { taskID: "root", taskKey: "root", name: "root", status: "succeeded", attempts: [] },
      {
        taskID: "finish",
        taskKey: "finish",
        name: "Finish",
        parentTaskID: "root",
        status: stage === 0 ? "running" : "succeeded",
        attempts: []
      }
    ]
  };

  const visibleRun =
    partial && stage === 1
      ? retainRunTasks(
          {
            ...run,
            tasks: run.tasks?.map((task) => ({ ...task, status: task.taskKey === "root" ? "succeeded" : "running" }))
          },
          { ...run, tasks: run.tasks?.slice(0, 1) }
        )
      : run;

  return (
    <>
      <Button onClick={() => setStage(stage + 1)} disabled={stage === 2}>
        {partial
          ? stage === 0
            ? "Receive partial update"
            : "Receive complete update"
          : stage === 0
            ? "Finish task"
            : "Finish run"}
      </Button>
      <RunDetailPage onBack={() => undefined} run={visibleRun} runID={run.id} />
    </>
  );
}

export const TaskCompletion: Story = { render: () => <TaskCompletionPreview /> };
export const PartialTaskUpdates: Story = { render: () => <TaskCompletionPreview partial /> };

const metadataJobID = "2a5ed99c-6d51-4c50-97e9-e31b23da7469";
const metadataRunID = "95d24ba7-aef2-43cb-ae40-4fe6a63c2f82";
const metadataOriginal = {
  ...data.runs[0],
  id: "db1d61ea-426e-46f1-8b20-b28d78747150",
  jobID: metadataJobID,
  jobName: metadataJobID,
  source: "ephemeral" as const,
  runNumber: 1,
  status: "cancelled" as const
};

export const ReplayMetadata: Story = {
  args: {
    onReplay: async () => undefined,
    originalRun: metadataOriginal,
    run: {
      ...metadataOriginal,
      id: metadataRunID,
      runNumber: 2,
      replayOfRunID: metadataOriginal.id,
      tasks: mockRunTasks(metadataRunID, [
        { key: "root", name: "root", status: "cancelled" },
        { key: "heartbeat", name: "Heartbeat", parentKey: "root", status: "cancelled" },
        { key: "finish", name: "Finish", parentKey: "root", status: "planned" }
      ])
    },
    runID: metadataRunID
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

export const FanoutHandoff: Story = {
  args: {
    run: {
      ...data.runs[0],
      id: "run-fanout-handoff",
      status: "running",
      finishedAt: undefined,
      tasks: mockRunTasks("run-fanout-handoff", [{ key: "root", name: "Fanout", status: "succeeded" }])
    },
    runID: "run-fanout-handoff"
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

export const RetriedRun: Story = {
  args: {
    runID: "run-retried",
    run: {
      ...data.runs[0],
      id: "run-retried",
      status: "succeeded",
      tasks: [
        {
          taskID: "run-retried:root",
          taskKey: "root",
          name: "Retried task",
          status: "succeeded",
          attempts: [
            {
              attempt: 1,
              attemptID: "attempt-1",
              executionID: "exec-old",
              cellID: "local",
              status: "failed",
              startedAt: "2026-05-31T12:00:00Z",
              finishedAt: "2026-05-31T12:00:05Z"
            },
            {
              attempt: 2,
              attemptID: "attempt-2",
              executionID: "exec-latest",
              cellID: "local",
              status: "succeeded",
              startedAt: "2026-05-31T12:01:00Z",
              finishedAt: "2026-05-31T12:01:08Z"
            }
          ]
        }
      ]
    }
  }
};

export const SharedAttemptContext: Story = {
  args: { run: retriedArtifactRun, runID: retriedArtifactRun.id, loadArtifacts: loadRetriedArtifacts }
};

const runOnlyChange = {
  one_off: true,
  actor: "mira",
  reason: "Run the release validation with a repaired checkout step.",
  commit: "override-commit",
  base_commit: "1234567890abcdef1234567890abcdef12345678",
  base_ref: "main",
  base_path: ".vectis/platform.build.json",
  created_at: 1793664000
};

export const RunOnlyDefinition: Story = {
  args: {
    run: {
      ...data.runs[0],
      jobID: "platform.build",
      repositoryID: "monorepo",
      submittedBy: "mira",
      hotfix: runOnlyChange
    },
    loadDefinition: async (runID) => ({
      run_id: runID,
      definition_version: 3,
      definition: { root: { id: "root", uses: "builtins/script", with: { script: "echo captured definition" } } },
      source: {
        repository_id: "monorepo",
        requested_ref: "main",
        resolved_commit: "captured-commit",
        path: ".vectis/platform.build.json",
        hotfix: runOnlyChange
      }
    })
  }
};
