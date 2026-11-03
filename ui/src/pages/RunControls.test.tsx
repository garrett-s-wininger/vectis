import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { RunListItem } from "../components";
import { RunControls } from "./RunControls";

const run: RunListItem = {
  id: "run-1",
  jobName: "demo",
  runNumber: 1,
  status: "running",
  duration: "1s",
  commit: "v1"
};

describe("RunControls", () => {
  it("disarms confirmation when focus leaves the button", () => {
    const cancel = vi.fn();
    render(<RunControls run={run} onCancel={cancel} />);

    const button = screen.getByRole("button", { name: "Cancel run" });
    fireEvent.click(button);
    fireEvent.blur(button);

    expect(button).toHaveTextContent("Cancel run");
    expect(cancel).not.toHaveBeenCalled();
  });

  it("requires confirmation and leaves cancellation pending until the worker finishes", async () => {
    let resolve!: () => void;
    const cancel = vi.fn(
      () =>
        new Promise<void>((done) => {
          resolve = done;
        })
    );

    const replay = vi.fn();
    const view = render(<RunControls run={run} onCancel={cancel} onReplay={replay} />);
    expect(screen.queryByRole("button", { name: "Run again" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Cancel run" }));
    expect(cancel).not.toHaveBeenCalled();

    const button = screen.getByRole("button", { name: "Confirm cancel" });
    fireEvent.keyDown(button, { key: "Escape" });
    expect(screen.queryByRole("button", { name: "Confirm cancel" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Cancel run" }));
    expect(screen.getByRole("button", { name: "Confirm cancel" })).toBe(button);

    fireEvent.click(button);
    expect(screen.getByRole("button", { name: "Requesting…" })).toBeDisabled();
    expect(cancel).toHaveBeenCalledOnce();

    await act(async () => resolve());
    expect(screen.getByRole("button", { name: "Cancel requested" })).toBeDisabled();

    view.rerender(<RunControls run={{ ...run, status: "cancelled" }} onCancel={cancel} onReplay={replay} />);
    expect(screen.getByRole("button", { name: "Run again" })).toBeEnabled();
  });

  it("reuses the replay idempotency key after an uncertain failure", async () => {
    const replay = vi.fn().mockRejectedValueOnce(new Error("Connection lost")).mockResolvedValue(undefined);
    render(<RunControls run={{ ...run, status: "failed" }} onReplay={replay} />);

    fireEvent.click(screen.getByRole("button", { name: "Run again" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Connection lost");

    fireEvent.click(screen.getByRole("button", { name: "Run again" }));
    await waitFor(() => expect(replay).toHaveBeenCalledTimes(2));
    expect(replay.mock.calls[0]).toEqual(["run-1", expect.any(String)]);
    expect(replay.mock.calls[1][1]).toBe(replay.mock.calls[0][1]);
  });

  it("keeps cancellation retryable when delivery fails", async () => {
    const cancel = vi.fn().mockRejectedValue(new Error("Cell unavailable"));
    render(<RunControls run={{ ...run, status: "queued" }} onCancel={cancel} />);
    fireEvent.click(screen.getByRole("button", { name: "Cancel run" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm cancel" }));

    expect(await screen.findByRole("alert")).toHaveTextContent("Cell unavailable");
    expect(screen.getByRole("button", { name: "Confirm cancel" })).toBeEnabled();
    expect(screen.queryByRole("button", { name: "Cancel requested" })).not.toBeInTheDocument();
  });
});

it("retries Run again with the same secure key in HTTP demo contexts", async () => {
  const getRandomValues = vi.fn((bytes: Uint8Array) => {
    bytes.fill(255);
    return bytes;
  });

  vi.stubGlobal("crypto", { getRandomValues });

  try {
    const replay = vi.fn().mockRejectedValueOnce(new Error("Connection lost")).mockResolvedValue(undefined);
    render(<RunControls run={{ ...run, status: "succeeded" }} onReplay={replay} />);

    fireEvent.click(screen.getByRole("button", { name: "Run again" }));
    expect(await screen.findByRole("alert")).toHaveTextContent("Connection lost");

    fireEvent.click(screen.getByRole("button", { name: "Run again" }));
    await waitFor(() => expect(replay).toHaveBeenCalledTimes(2));
    expect(replay.mock.calls[0]).toEqual(["run-1", "ffffffff-ffff-4fff-bfff-ffffffffffff"]);
    expect(replay.mock.calls[1]).toEqual(replay.mock.calls[0]);
    expect(getRandomValues).toHaveBeenCalledOnce();
  } finally {
    vi.unstubAllGlobals();
  }
});
