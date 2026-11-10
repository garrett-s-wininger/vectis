import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { RunDefinition } from "./RunDefinition";
import type { RunListItem } from "../components";
const run: RunListItem = {
  id: "run-1",
  jobName: "build",
  runNumber: 1,
  commit: "v2",
  status: "succeeded",
  duration: "1s"
};

const snapshot = {
  run_id: "run-1",
  definition_version: 2,
  definition: { root: { id: "root", uses: "builtins/script", with: { script: "echo captured" } } },
  source: {
    repository_id: "mono",
    requested_ref: "main",
    resolved_commit: "captured-commit",
    path: ".vectis/build.json"
  }
};

it("loads the run snapshot on expansion instead of showing the current job definition and caches it across toggles", async () => {
  const load = vi.fn().mockResolvedValue(snapshot);
  const { rerender } = render(
    <RunDefinition run={{ ...run, definition: '{"root":{"id":"current-job"}}' }} load={load} />
  );

  expect(load).not.toHaveBeenCalled();

  await userEvent.click(screen.getByText("Job Definition"));
  await screen.findByText(/echo captured/);
  expect(load).toHaveBeenCalledExactlyOnceWith("run-1");
  expect(screen.queryByText(/current-job/)).not.toBeInTheDocument();
  expect(screen.getByText(/captured-commit/)).toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Copy run definition" })).toBeInTheDocument();

  await userEvent.click(screen.getByText("Job Definition"));
  await userEvent.click(screen.getByText("Job Definition"));
  rerender(<RunDefinition run={{ ...run, status: "running" }} load={load} />);

  await waitFor(() => expect(load).toHaveBeenCalledTimes(1));
});

it("shows definition failures with retry instead of generating metadata", async () => {
  const load = vi.fn().mockRejectedValueOnce(new Error("Snapshot unavailable")).mockResolvedValue(snapshot);
  render(<RunDefinition run={run} load={load} />);

  await userEvent.click(screen.getByText("Job Definition"));
  await screen.findByText("Snapshot unavailable");
  expect(screen.queryByText(/"run_id"/)).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Retry definition" }));
  await screen.findByText(/echo captured/);
  expect(load).toHaveBeenCalledTimes(2);
});

it("rejects a snapshot for another run", async () => {
  const load = vi.fn().mockResolvedValue({ ...snapshot, run_id: "another-run" });
  render(<RunDefinition run={run} load={load} />);

  await userEvent.click(screen.getByText("Job Definition"));
  await screen.findByText("The response did not contain this run's captured definition.");
  expect(screen.queryByText(/echo captured/)).not.toBeInTheDocument();
});

it("states when a captured definition is unavailable without a loader", async () => {
  render(<RunDefinition run={run} />);

  await userEvent.click(screen.getByText("Job Definition"));
  await screen.findByText(/captured definition is unavailable/);
  expect(screen.queryByText(/"run_id"/)).not.toBeInTheDocument();
});

it("does not repeat the run reason and actor inside the captured definition", async () => {
  const hotfix = {
    one_off: true,
    actor: "mira",
    reason: "Repair this execution",
    commit: "hotfix",
    base_commit: "1234567890abcdef",
    base_ref: "main",
    base_path: ".vectis/build.json",
    created_at: 1
  };

  const load = vi.fn().mockResolvedValue({ ...snapshot, source: { ...snapshot.source, hotfix } });
  render(<RunDefinition run={{ ...run, hotfix }} load={load} />);

  await userEvent.click(screen.getByText("Job Definition"));
  await screen.findByText(/echo captured/);
  expect(screen.queryByText("Repair this execution")).not.toBeInTheDocument();
  expect(screen.queryByText(/mira/)).not.toBeInTheDocument();
  expect(screen.getByRole("button", { name: "Copy Hotfix base commit" })).toBeInTheDocument();
});
