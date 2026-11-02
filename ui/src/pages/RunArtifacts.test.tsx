import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { artifactRun, loadMockArtifacts, mockArtifacts } from "../mocks/runArtifacts";
import type { ArtifactPage, LoadRunArtifacts } from "../data/runArtifacts";
import { runTasksForDisplay } from "./RunDetailPresentation";
import { RunArtifacts } from "./RunArtifacts";

const report = runTasksForDisplay(artifactRun).find((task) => task.taskKey === "report")!;

it("shows metadata and native downloads, and filters by the selected task on the server", async () => {
  const load = vi.fn(loadMockArtifacts);
  const view = render(<RunArtifacts run={artifactRun} loadArtifacts={load} />);
  const link = await screen.findByRole("link", { name: "Download demo-report" });
  expect(link).toHaveAttribute("href", "/api/v1/runs/artifact-demo-run/artifacts/demo-report/download");
  expect(link).toHaveAttribute("download", "report.txt");
  expect(link).toHaveAttribute("target", "_blank");
  expect(screen.getByText("Report")).toBeVisible();
  expect(screen.getByText("Archive")).toBeVisible();
  expect(screen.getByText("2 KiB")).toBeVisible();
  expect(screen.getByText("0 B")).toBeVisible();

  view.rerender(<RunArtifacts run={artifactRun} selectedTask={report} loadArtifacts={load} />);
  await waitFor(() =>
    expect(load).toHaveBeenLastCalledWith(artifactRun.id, expect.objectContaining({ taskID: report.taskID }))
  );

  await waitFor(() => expect(screen.queryByRole("link", { name: "Download demo-bundle" })).not.toBeInTheDocument());
  expect(await screen.findByRole("link", { name: "Download demo-report" })).toBeVisible();

  const archive = runTasksForDisplay(artifactRun).find((task) => task.taskKey === "archive")!;
  view.rerender(<RunArtifacts run={artifactRun} selectedTask={archive} loadArtifacts={load} />);
  expect(await screen.findByRole("link", { name: "Download demo-bundle" })).toBeVisible();
  expect(screen.queryByRole("link", { name: "Download demo-report" })).not.toBeInTheDocument();

  view.rerender(<RunArtifacts run={artifactRun} loadArtifacts={load} />);
  expect(await screen.findByRole("link", { name: "Download demo-report" })).toBeVisible();
  expect(await screen.findByRole("link", { name: "Download demo-bundle" })).toBeVisible();
});

it("follows pagination, retains listed rows during refresh failure, and recovers on retry", async () => {
  const load = vi.fn<LoadRunArtifacts>().mockResolvedValueOnce({ data: [mockArtifacts[0]], nextCursor: 1 });
  render(<RunArtifacts run={artifactRun} loadArtifacts={load} />);
  expect(await screen.findByRole("link", { name: "Download demo-report" })).toBeVisible();

  load
    .mockResolvedValueOnce({ data: [mockArtifacts[0]], nextCursor: 1 })
    .mockResolvedValueOnce({ data: [mockArtifacts[1]] });
  fireEvent.click(screen.getByRole("button", { name: "Load more artifacts" }));
  expect(await screen.findByRole("link", { name: "Download demo-bundle" })).toBeVisible();
  expect(load).toHaveBeenLastCalledWith(artifactRun.id, expect.objectContaining({ cursor: 1 }));

  load.mockRejectedValueOnce(new Error("Temporarily offline"));
  fireEvent.click(screen.getByRole("button", { name: "Refresh artifacts" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("Temporarily offline");
  expect(screen.getByRole("link", { name: "Download demo-report" })).toBeVisible();
  expect(screen.getByRole("link", { name: "Download demo-bundle" })).toBeVisible();

  load.mockResolvedValue({ data: [mockArtifacts[0]] });
  fireEvent.click(screen.getByRole("button", { name: "Refresh artifacts" }));
  await waitFor(() => expect(screen.queryByRole("alert")).not.toBeInTheDocument());
});

it("ignores late responses after changing runs", async () => {
  let resolve!: (page: ArtifactPage) => void;
  const load = vi
    .fn<LoadRunArtifacts>()
    .mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = done;
        })
    )
    .mockResolvedValue({ data: [] });

  const view = render(<RunArtifacts run={artifactRun} loadArtifacts={load} />);
  expect(screen.getByRole("status")).toHaveTextContent("Loading artifacts");

  const signal = load.mock.calls[0][1]!.signal!;
  view.rerender(<RunArtifacts run={{ ...artifactRun, id: "other-run" }} loadArtifacts={load} />);
  expect(signal.aborted).toBe(true);

  await act(async () => resolve({ data: [mockArtifacts[0]] }));
  expect(await screen.findByText("No artifacts published for this run.")).toBeVisible();
  expect(screen.queryByRole("link", { name: "Download demo-report" })).not.toBeInTheDocument();
});

it("ignores late artifact responses after changing only the shared attempt", async () => {
  let resolve!: (page: ArtifactPage) => void;
  const load = vi
    .fn<LoadRunArtifacts>()
    .mockImplementationOnce(
      () =>
        new Promise((done) => {
          resolve = done;
        })
    )
    .mockResolvedValue({ data: [mockArtifacts[1]] });

  const view = render(<RunArtifacts run={artifactRun} attempt={1} loadArtifacts={load} />);
  const signal = load.mock.calls[0][1]!.signal!;
  view.rerender(<RunArtifacts run={artifactRun} attempt={2} loadArtifacts={load} />);
  expect(signal.aborted).toBe(true);
  expect(await screen.findByRole("link", { name: "Download demo-bundle" })).toBeVisible();

  await act(async () => resolve({ data: [mockArtifacts[0]] }));
  expect(screen.queryByRole("link", { name: "Download demo-report" })).not.toBeInTheDocument();
  expect(load).toHaveBeenLastCalledWith(artifactRun.id, expect.objectContaining({ attempt: 2 }));
});

it("refreshes live artifacts without overlapping requests and stops after bounded terminal catch-up", async () => {
  vi.useFakeTimers();

  try {
    let resolve!: (page: ArtifactPage) => void;
    const load = vi
      .fn<LoadRunArtifacts>()
      .mockImplementationOnce(
        () =>
          new Promise((done) => {
            resolve = done;
          })
      )
      .mockResolvedValue({ data: mockArtifacts });

    const view = render(<RunArtifacts run={{ ...artifactRun, status: "running" }} loadArtifacts={load} />);
    await act(async () => vi.advanceTimersByTimeAsync(15000));
    expect(load).toHaveBeenCalledOnce();

    await act(async () => resolve({ data: [mockArtifacts[0]] }));
    await act(async () => vi.advanceTimersByTimeAsync(5000));
    expect(screen.getByRole("link", { name: "Download demo-bundle" })).toBeVisible();

    view.rerender(<RunArtifacts run={artifactRun} loadArtifacts={load} />);
    await act(async () => vi.advanceTimersByTimeAsync(6000));
    const calls = load.mock.calls.length;
    await act(async () => vi.advanceTimersByTimeAsync(30000));
    expect(load).toHaveBeenCalledTimes(calls);
    expect(calls).toBe(5);
  } finally {
    vi.useRealTimers();
  }
});

it("keeps existing rows and controls steady while a background refresh is pending", async () => {
  vi.useFakeTimers();

  try {
    let resolve!: (page: ArtifactPage) => void;
    const load = vi
      .fn<LoadRunArtifacts>()
      .mockResolvedValueOnce({ data: [mockArtifacts[0]] })
      .mockImplementationOnce(
        () =>
          new Promise((done) => {
            resolve = done;
          })
      );

    render(<RunArtifacts run={{ ...artifactRun, status: "running" }} loadArtifacts={load} />);
    await act(async () => vi.advanceTimersByTimeAsync(0));
    await act(async () => vi.advanceTimersByTimeAsync(5000));
    expect(screen.getByRole("button", { name: "Refresh artifacts" })).toBeEnabled();
    expect(screen.getByRole("link", { name: "Download demo-report" })).toBeVisible();
    expect(screen.queryByText("Loading artifacts…")).not.toBeInTheDocument();

    await act(async () => resolve({ data: mockArtifacts }));
    expect(screen.getByRole("link", { name: "Download demo-bundle" })).toBeVisible();
  } finally {
    vi.useRealTimers();
  }
});
