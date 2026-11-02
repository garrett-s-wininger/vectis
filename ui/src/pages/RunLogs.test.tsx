import { act, fireEvent, render, screen, within } from "@testing-library/react";
import type { RunListItem } from "../components";
import type { RunLogEntry } from "../data/runLogs";
import { streamRunLogs } from "../data/runLogs";
import { RunLogs } from "./RunLogs";
import type { RunTaskDisplayNode } from "./RunDetailPresentation";

vi.mock("../data/runLogs", () => ({ streamRunLogs: vi.fn() }));

const run = { id: "run-1" } as RunListItem;
const task: RunTaskDisplayNode = {
  taskID: "task-a",
  taskKey: "a",
  name: "Build",
  depth: 0,
  status: "succeeded",
  attempts: [
    { attempt: 1, attemptID: "attempt-1", executionID: "execution-1", cellID: "local", status: "failed" },
    { attempt: 2, attemptID: "attempt-2", executionID: "execution-2", cellID: "local", status: "succeeded" }
  ]
};

let entry: (entry: RunLogEntry) => void;
let fail: (message: string, streamOpened?: boolean) => void;
let complete: (() => void) | undefined;
let close = vi.fn<() => void>();

beforeEach(() => {
  close = vi.fn<() => void>();
  vi.mocked(streamRunLogs)
    .mockReset()
    .mockImplementation((_id, onEntry, onError, onComplete) => {
      entry = onEntry;
      fail = onError;
      complete = onComplete;
      return close;
    });
});

it("keeps completed output visible and clears an earlier transport error", () => {
  render(<RunLogs run={run} selectedTask={task} />);
  emit("finished output\n", 1, "execution-2");
  act(() => fail("offline"));
  expect(screen.getByRole("alert")).toHaveTextContent("offline");

  act(() => complete?.());
  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(screen.getByRole("status")).toHaveTextContent("Output complete.");
  expect(screen.getByLabelText("Log output")).toHaveTextContent("finished output");
});

it.each(
  (["succeeded", "failed", "cancelled", "aborted", "abandoned"] as const).flatMap((status) =>
    [true, false].map((statusFirst) => ({ status, statusFirst }))
  )
)("reconciles a closed stream with $status, status arrives first: $statusFirst", ({ status, statusFirst }) => {
  const running = { ...run, status: "running" as const, cancelRequested: true };
  const view = render(<RunLogs run={running} selectedTask={task} />);
  emit("last output before cancellation\n", 1, "execution-2");

  if (statusFirst) view.rerender(<RunLogs run={{ ...running, status }} selectedTask={task} />);
  act(() => fail("Log stream is unavailable.", true));

  if (!statusFirst) {
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Waiting for run status…");
    view.rerender(<RunLogs run={{ ...running, status }} selectedTask={task} />);
  }

  expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  expect(screen.getByRole("status")).toHaveTextContent(`Run ${status}. Live log stream closed.`);
  expect(screen.queryByText("Output complete.")).not.toBeInTheDocument();
  expect(screen.getByLabelText("Log output")).toHaveTextContent("last output before cancellation");
  expect(streamRunLogs).toHaveBeenCalledTimes(1);
});

it("reports a genuine established-stream failure after the status grace period, even with cancellation pending", () => {
  vi.useFakeTimers();
  try {
    render(<RunLogs run={{ ...run, status: "running", cancelRequested: true }} selectedTask={task} />);
    emit("retained output\n", 1, "execution-2");
    act(() => fail("Log stream is unavailable.", true));
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();

    act(() => vi.advanceTimersByTime(3000));
    expect(screen.getByRole("alert")).toHaveTextContent("Log stream is unavailable.");
    expect(screen.getByLabelText("Log output")).toHaveTextContent("retained output");
  } finally {
    vi.useRealTimers();
  }
});

it("clears a stream error when the terminal status arrives after the grace period", () => {
  vi.useFakeTimers();

  try {
    const view = render(<RunLogs run={{ ...run, status: "running" }} selectedTask={task} />);
    act(() => fail("Log stream is unavailable.", true));
    act(() => vi.advanceTimersByTime(3000));
    expect(screen.getByRole("alert")).toBeVisible();

    view.rerender(<RunLogs run={{ ...run, status: "succeeded" }} selectedTask={task} />);
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
    expect(screen.getByRole("status")).toHaveTextContent("Run succeeded. Live log stream closed.");
  } finally {
    vi.useRealTimers();
  }
});

it("reports a failed log connection even when the run is already cancelled", () => {
  render(<RunLogs run={{ ...run, status: "cancelled" }} selectedTask={task} />);
  act(() => fail("Log stream is unavailable.", false));

  expect(screen.getByRole("alert")).toHaveTextContent("Log stream is unavailable.");
  expect(screen.queryByText("Run cancelled. Live log stream closed.")).not.toBeInTheDocument();
});

function emit(data: string, sequence: number, executionID?: string) {
  act(() => entry({ data, sequence, executionID, stream: "stdout" }));
}

function choose(label: string, option: string) {
  fireEvent.click(screen.getByLabelText(new RegExp(`^${label}: `)));
  fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name: option }));
}

it("isolates task attempts, keeps raw chunks intact, and exposes legacy output only run-wide", () => {
  render(<RunLogs run={run} selectedTask={task} />);
  emit("old attempt", 1, "execution-1");
  emit("unrelated", 2, "other-task");
  emit("legacy", 3);
  emit("hel", 4, "execution-2");
  emit("lo\n", 5, "execution-2");
  emit("duplicate", 5, "execution-2");
  expect(screen.getByLabelText("Log output").textContent).toBe("hello\n");

  choose("Attempt", "Attempt 1");
  expect(screen.getByText("Failed")).toBeInTheDocument();
  expect(screen.queryByText("Succeeded")).not.toBeInTheDocument();
  expect(screen.getByLabelText("Log output").textContent).toBe("old attempt");

  choose("Attempt", "All attempts");
  expect(screen.getByLabelText("Log output").textContent).toBe("old attempthello\n");
  choose("Output", "All run output");
  expect(screen.getByLabelText("Log output").textContent).toBe("old attemptunrelatedlegacyhello\n");
});

it("switches tasks without reconnecting or leaking output, and retains logs on errors", () => {
  const view = render(<RunLogs run={run} selectedTask={task} />);
  emit("build output", 1, "execution-2");
  view.rerender(<RunLogs run={run} selectedTask={{ ...task, taskID: "pending", attempts: [] }} />);
  expect(screen.queryByLabelText("Log output")).not.toBeInTheDocument();
  expect(screen.getByText("This task has not started an execution attempt.")).toBeInTheDocument();

  view.rerender(<RunLogs run={run} selectedTask={task} />);
  act(() => fail("offline"));
  expect(screen.getByLabelText("Log output")).toHaveTextContent("build output");
  expect(screen.getByRole("alert")).toHaveTextContent("offline");
  expect(streamRunLogs).toHaveBeenCalledTimes(1);

  const staleEntry = entry;
  view.rerender(<RunLogs run={{ ...run, id: "run-2" }} selectedTask={task} />);
  expect(close).toHaveBeenCalledOnce();
  expect(screen.queryByLabelText("Log output")).not.toBeInTheDocument();
  expect(streamRunLogs).toHaveBeenCalledTimes(2);

  emit("new run", 1, "execution-2");
  act(() => staleEntry({ data: "stale", sequence: 20, stream: "stdout", executionID: "execution-2" }));
  expect(screen.getByLabelText("Log output").textContent).toBe("new run");
});

it("reconnects for a new root dispatch and retains earlier-attempt output", () => {
  const root = { ...task, taskKey: "root", attempts: [task.attempts[0]] };
  const view = render(<RunLogs run={{ ...run, tasks: [root] }} selectedTask={root} />);
  emit("earlier output\n", 1, "execution-1");

  const retried = { ...root, attempts: task.attempts };
  view.rerender(<RunLogs run={{ ...run, tasks: [retried] }} selectedTask={retried} />);
  expect(streamRunLogs).toHaveBeenCalledTimes(2);
  expect(close).toHaveBeenCalledOnce();

  emit("latest output\n", 2, "execution-2");
  expect(screen.getByLabelText("Log output")).toHaveTextContent("latest output");
  choose("Attempt", "Attempt 1");
  expect(screen.getByLabelText("Log output")).toHaveTextContent("earlier output");
  expect(screen.getByLabelText("Log output")).not.toHaveTextContent("latest output");
});
