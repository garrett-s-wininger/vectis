import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { defaultRunDefinition } from "../domain/consoleOptions";
import { RunsPage } from "./RunsPage";

function openRunOnce(submit = vi.fn()) {
  render(
    <RunsPage
      namespaces={[]}
      namespacePath="/"
      onSelectNamespace={vi.fn()}
      onSelectRun={vi.fn()}
      onSubmitEphemeralRun={submit}
      runs={[]}
    />
  );

  fireEvent.click(screen.getByRole("button", { name: "Run Once" }));
  return {
    submit,
    input: screen.getByLabelText("Choose definition JSON file"),
    editor: screen.getByLabelText("Definition JSON")
  };
}

afterEach(() => vi.restoreAllMocks());

it("imports a JSON file for review and submits subsequent edits only on request", async () => {
  const { submit, input, editor } = openRunOnce();
  const definition = '{"root":{"uses":"builtins/script","with":{"script":"echo demo"}}}';
  const file = new File(["\uFEFF" + definition], "log-viewer.json", { type: "application/json" });
  await userEvent.upload(input, file);
  await screen.findByText("Imported log-viewer.json");
  expect(editor).toHaveValue(definition);
  expect(submit).not.toHaveBeenCalled();

  fireEvent.change(editor, {
    target: { value: '{"root":{"uses":"builtins/script","with":{"script":"echo edited"}}}' }
  });

  await userEvent.upload(input, file);
  await waitFor(() => expect(editor).toHaveValue(definition));
  fireEvent.change(editor, {
    target: { value: '{"root":{"uses":"builtins/script","with":{"script":"echo reviewed"}}}' }
  });

  fireEvent.click(screen.getByRole("button", { name: "Submit run" }));
  expect(submit).toHaveBeenCalledOnce();
  expect(submit).toHaveBeenCalledWith('{"root":{"uses":"builtins/script","with":{"script":"echo reviewed"}}}');

  await waitFor(() => expect(screen.queryByLabelText("Definition JSON")).not.toBeInTheDocument());
});

it.each(["{bad", "[]", "null"])("keeps the draft when an imported file is not a JSON object: %s", async (contents) => {
  const { input, editor, submit } = openRunOnce();
  await userEvent.upload(input, new File([contents], "invalid.json", { type: "application/json" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("The current definition has been kept.");
  expect(editor).toHaveValue(defaultRunDefinition);
  expect(submit).not.toHaveBeenCalled();
});

it("rejects oversized files before reading them", async () => {
  const { input, editor } = openRunOnce();
  const read = vi.spyOn(FileReader.prototype, "readAsText");
  const file = new File(["{}"], "large.json", { type: "application/json" });
  Object.defineProperty(file, "size", { value: 10 * 1024 * 1024 + 1 });

  await userEvent.upload(input, file);
  expect(screen.getByRole("alert")).toHaveTextContent("10 MiB");
  expect(read).not.toHaveBeenCalled();
  expect(editor).toHaveValue(defaultRunDefinition);
});

it("keeps the draft when reading a file fails", async () => {
  const { input, editor } = openRunOnce();
  vi.spyOn(FileReader.prototype, "readAsText").mockImplementation(function (this: FileReader) {
    this.dispatchEvent(new ProgressEvent("error"));
  });

  await userEvent.upload(input, new File(["{}"], "unreadable.json", { type: "application/json" }));
  expect(screen.getByRole("alert")).toHaveTextContent("Unable to read that file.");
  expect(editor).toHaveValue(defaultRunDefinition);
  expect(screen.getByRole("button", { name: "Submit run" })).toBeEnabled();
});

it("blocks submission while loading and ignores a read completed after closing the editor", async () => {
  const { input, editor, submit } = openRunOnce();
  const pending: FileReader[] = [];
  vi.spyOn(FileReader.prototype, "readAsText").mockImplementation(function (this: FileReader) {
    pending.push(this);
  });

  await userEvent.upload(input, new File(["{}"], "pending.json", { type: "application/json" }));
  expect(editor).toBeEnabled();
  expect(editor).toHaveAttribute("readonly");
  expect(screen.getByLabelText("Definition JSON")).toBe(editor);
  expect(screen.getByRole("button", { name: "Load JSON file" })).toBeEnabled();
  expect(screen.getByRole("status")).toHaveTextContent("Load a file or edit the JSON below, then submit.");

  const submitButton = screen.getByRole("button", { name: "Submit run" });
  expect(submitButton).toBeEnabled();
  expect(submitButton).toHaveAttribute("aria-disabled", "true");

  fireEvent.click(submitButton);
  expect(submit).not.toHaveBeenCalled();

  fireEvent.submit(editor.closest("form")!);
  expect(submit).not.toHaveBeenCalled();

  fireEvent.click(screen.getByRole("button", { name: "Close" }));
  Object.defineProperty(pending[0], "result", { value: "{}" });
  pending[0].dispatchEvent(new ProgressEvent("load"));
  fireEvent.click(screen.getByRole("button", { name: "Run Once" }));

  expect(screen.getByLabelText("Definition JSON")).toHaveValue(defaultRunDefinition);
  expect(screen.queryByText("Imported pending.json")).not.toBeInTheDocument();
});

it("keeps the draft and page in place until submission succeeds, and prevents duplicate submits", async () => {
  let finish!: () => void;
  const submit = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      })
  );

  const { editor } = openRunOnce(submit);
  fireEvent.change(editor, { target: { value: '{"root":{"uses":"builtins/script"}}' } });

  const button = screen.getByRole("button", { name: "Submit run" });
  fireEvent.click(button);
  expect(screen.getByLabelText("Definition JSON")).toBe(editor);
  expect(editor).toHaveValue('{"root":{"uses":"builtins/script"}}');
  expect(editor.closest("form")).toHaveAttribute("aria-busy", "true");

  fireEvent.click(button);
  fireEvent.submit(editor.closest("form")!);
  fireEvent.click(screen.getByRole("button", { name: "Close" }));
  expect(screen.getByLabelText("Definition JSON")).toBe(editor);
  expect(submit).toHaveBeenCalledOnce();

  await act(async () => finish());
  expect(screen.queryByLabelText("Definition JSON")).not.toBeInTheDocument();
});

it("keeps the draft open with a useful error when submission fails", async () => {
  const submit = vi.fn().mockRejectedValue(new Error("Queue unavailable"));
  const { editor } = openRunOnce(submit);

  fireEvent.click(screen.getByRole("button", { name: "Submit run" }));
  expect(await screen.findByRole("alert")).toHaveTextContent("Queue unavailable");
  expect(screen.getByLabelText("Definition JSON")).toBe(editor);
  expect(editor).toHaveValue(defaultRunDefinition);
  expect(editor).not.toHaveAttribute("readonly");
  expect(screen.getByRole("button", { name: "Submit run" })).toHaveAttribute("aria-disabled", "false");
});
