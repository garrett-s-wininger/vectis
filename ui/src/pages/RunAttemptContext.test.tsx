import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { RunDetailPage } from "./RunDetailPage";
import { artifactRun, loadMockArtifacts } from "../mocks/runArtifacts";
import { retriedArtifactRun, loadRetriedArtifacts } from "../mocks/runAttempts";
import { streamRunLogs } from "../data/runLogs";
import type { RunLogEntry } from "../data/runLogs";

vi.mock("../data/runLogs", () => ({ streamRunLogs: vi.fn() }));
let emit: (entry: RunLogEntry) => void;
beforeEach(() =>
  vi
    .mocked(streamRunLogs)
    .mockReset()
    .mockImplementation((_id, entry) => {
      emit = entry;
      return () => {};
    })
);

function chooseAttempt(label: string) {
  fireEvent.click(within(screen.getByRole("region", { name: "Graph" })).getByLabelText(/^Attempt:/));
  fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name: label }));
}

it("uses one attempt selection for graph, logs, and artifacts across task switches", async () => {
  const load = vi.fn(loadRetriedArtifacts);
  render(
    <RunDetailPage run={retriedArtifactRun} runID={retriedArtifactRun.id} onBack={() => {}} loadArtifacts={load} />
  );

  fireEvent.click(screen.getByRole("button", { name: "Report: Succeeded" }));
  const report = retriedArtifactRun.tasks![1];
  act(() => {
    emit({ data: "first output\n", sequence: 1, stream: "stdout", executionID: report.attempts[0].executionID });
    emit({ data: "second output\n", sequence: 2, stream: "stdout", executionID: report.attempts[1].executionID });
    emit({ data: "unattributed output\n", sequence: 3, stream: "stdout" });
  });

  expect(screen.getByLabelText("Log output")).toHaveTextContent("second output");
  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("first output");
  expect(await screen.findByRole("link", { name: "Download report-attempt-2" })).toBeVisible();
  expect(screen.queryByRole("link", { name: "Download report-attempt-1" })).not.toBeInTheDocument();
  expect(screen.getAllByLabelText(/^Attempt:/)).toHaveLength(1);
  expect(screen.queryByLabelText(/^Output:/)).not.toBeInTheDocument();
  expect(screen.queryByLabelText(/^Artifact scope:/)).not.toBeInTheDocument();

  chooseAttempt("Attempt 1");
  expect(screen.getByRole("button", { name: "Report: Failed" })).toHaveAttribute("aria-pressed", "true");
  expect(screen.getByLabelText("Log output")).toHaveTextContent("first output");
  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("second output");
  expect(await screen.findByRole("link", { name: "Download report-attempt-1" })).toBeVisible();

  await waitFor(() =>
    expect(load).toHaveBeenLastCalledWith(retriedArtifactRun.id, expect.objectContaining({ attempt: 1 }))
  );

  fireEvent.click(screen.getByRole("button", { name: "Archive: Succeeded" }));
  expect(within(screen.getByRole("region", { name: "Graph" })).getByLabelText(/^Attempt:/)).toHaveTextContent(
    "Attempt 1"
  );

  expect(
    screen.getByText(
      retriedArtifactRun.tasks![2].attempts[0].executionID!.slice(0, 4) +
        retriedArtifactRun.tasks![2].attempts[0].executionID!.slice(-4)
    )
  ).toBeVisible();

  chooseAttempt("Attempt 2");
  expect(screen.getByRole("button", { name: "Archive: Planned" })).toHaveAttribute("aria-pressed", "true");
  expect(screen.getByText("This task has no execution in attempt 2.")).toBeVisible();
  expect(screen.queryByLabelText("Log output")).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Clear task selection" }));
  expect(await screen.findByRole("link", { name: "Download report-attempt-2" })).toBeVisible();
  expect(screen.queryByRole("link", { name: "Download legacy-output" })).not.toBeInTheDocument();
  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("first output");
  expect(screen.getByLabelText("Log output")).toHaveTextContent("second output");
  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("unattributed output");

  fireEvent.click(screen.getByLabelText(/^Attempt:/));
  expect(screen.queryByRole("option", { name: "All attempts" })).not.toBeInTheDocument();
  expect(streamRunLogs).toHaveBeenCalledOnce();
});

it("pins a numbered attempt as new retries arrive, and resets context for a different run", async () => {
  const view = render(
    <RunDetailPage run={retriedArtifactRun} runID={retriedArtifactRun.id} onBack={() => {}} onCancel={async () => {}} />
  );

  chooseAttempt("Attempt 1");
  const next = {
    ...retriedArtifactRun,
    status: "running" as const,
    tasks: retriedArtifactRun.tasks?.map((task) => ({
      ...task,
      status: "running" as const,
      attempts: [
        ...task.attempts,
        {
          ...task.attempts[0],
          attempt: 3,
          attemptID: `${task.taskID}:attempt:3`,
          executionID: `${task.taskID}:exec:3`,
          status: "running" as const
        }
      ]
    }))
  };

  view.rerender(<RunDetailPage run={next} runID={next.id} onBack={() => {}} onCancel={async () => {}} />);
  expect(screen.getByLabelText(/^Attempt:/)).toHaveTextContent("Attempt 1");
  expect(screen.queryByRole("button", { name: "Cancel run" })).not.toBeInTheDocument();

  chooseAttempt("Latest (attempt 3)");
  expect(screen.getByRole("button", { name: "Cancel run" })).toBeVisible();

  chooseAttempt("Attempt 1");
  view.rerender(<RunDetailPage run={{ ...next, id: "new-run" }} runID="new-run" onBack={() => {}} />);
  expect(screen.getByLabelText(/^Attempt:/)).toHaveTextContent("Latest (attempt 3)");
});

it("defaults ordinary runs to all tasks and clears both panels together without attempt controls", async () => {
  const load = vi.fn(loadMockArtifacts);
  render(<RunDetailPage run={artifactRun} runID={artifactRun.id} onBack={() => {}} loadArtifacts={load} />);
  expect(screen.getByRole("heading", { name: "Run Logs" })).toBeVisible();
  expect(screen.queryByLabelText(/^Attempt:/)).not.toBeInTheDocument();
  expect(screen.queryByText("All attempts")).not.toBeInTheDocument();
  expect(await screen.findByRole("link", { name: "Download demo-report" })).toBeVisible();
  expect(await screen.findByRole("link", { name: "Download demo-bundle" })).toBeVisible();

  const report = artifactRun.tasks!.find((task) => task.taskKey === "report")!;
  act(() => {
    emit({ data: "report output\n", sequence: 1, stream: "stdout", executionID: report.attempts[0].executionID });
    emit({ data: "legacy output\n", sequence: 2, stream: "stdout" });
  });

  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("legacy output");

  fireEvent.click(screen.getByRole("button", { name: "Report: Succeeded" }));
  expect(screen.getByRole("heading", { name: "Task Logs" })).toBeVisible();
  expect(screen.getByLabelText("Log output")).toHaveTextContent("report output");
  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("legacy output");
  await waitFor(() =>
    expect(load).toHaveBeenLastCalledWith(artifactRun.id, expect.objectContaining({ taskID: report.taskID }))
  );

  await waitFor(() => expect(screen.queryByRole("link", { name: "Download demo-bundle" })).not.toBeInTheDocument());
  fireEvent.click(screen.getByRole("button", { name: "Clear task selection" }));
  expect(screen.getByRole("button", { name: "Report: Succeeded" })).toHaveAttribute("aria-pressed", "false");
  expect(screen.getByRole("heading", { name: "Run Logs" })).toBeVisible();
  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("legacy output");
  expect(await screen.findByRole("link", { name: "Download demo-bundle" })).toBeVisible();
  expect(streamRunLogs).toHaveBeenCalledOnce();
});
