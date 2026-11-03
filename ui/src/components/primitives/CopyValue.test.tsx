import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import { CopyValue } from "./CopyValue";

afterEach(() => {
  vi.unstubAllGlobals();
  delete (document as unknown as { execCommand?: unknown }).execCommand;
});

it("copies the full value rather than its shortened display and reports success", async () => {
  const writeText = vi.fn().mockResolvedValue(undefined);
  vi.stubGlobal("navigator", { clipboard: { writeText } });

  render(
    <CopyValue label="Execution ID" value="full-execution-id">
      short-id
    </CopyValue>
  );

  fireEvent.click(screen.getByRole("button", { name: "Copy Execution ID" }));
  await waitFor(() => expect(screen.getByText("Execution ID copied.")).toBeInTheDocument());
  expect(writeText).toHaveBeenCalledWith("full-execution-id");
  expect(screen.getByText("short-id")).toBeVisible();
});

it("supports HTTP demo copying and restores keyboard focus", async () => {
  vi.stubGlobal("navigator", {});
  const command = vi.fn().mockReturnValue(true);
  Object.defineProperty(document, "execCommand", { configurable: true, value: command });

  render(<CopyValue label="Run ID" value="run-1" />);
  const button = screen.getByRole("button", { name: "Copy Run ID" });

  button.focus();
  fireEvent.click(button);
  await waitFor(() => expect(screen.getByText("Run ID copied.")).toBeInTheDocument());
  expect(command).toHaveBeenCalledWith("copy");
  expect(button).toHaveFocus();
  expect(document.querySelector("textarea")).toBeNull();
});

it("reports denied copying without claiming success", async () => {
  vi.stubGlobal("navigator", { clipboard: { writeText: vi.fn().mockRejectedValue(new Error("denied")) } });
  Object.defineProperty(document, "execCommand", { configurable: true, value: () => false });

  render(<CopyValue label="Job ID" value="job-1" />);

  fireEvent.click(screen.getByRole("button", { name: "Copy Job ID" }));
  await waitFor(() => expect(screen.getByText(/Could not copy job id/)).toBeInTheDocument());
  expect(screen.getByText("job-1")).toBeVisible();
});
