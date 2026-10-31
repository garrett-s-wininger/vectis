import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { JobEditor } from "./JobEditor";
import { emptyJobForm } from "./JobEditorModel";

describe("JobEditor", () => {
  it("submits a source job in the selected repository namespace", async () => {
    const createJob = vi.fn();
    const cancel = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        repositories={[{ id: "vectis-local", namespacePath: "/", canAuthor: true }]}
        namespacePath="/platform"
        onCancel={cancel}
        onCreateJob={createJob}
        onError={() => undefined}
        onUpdateJob={() => undefined}
        onValuesChange={() => undefined}
        values={{
          ...emptyJobForm,
          repository: "vectis-local",
          name: "cache-warmup"
        }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    await waitFor(() => expect(cancel).toHaveBeenCalled());

    expect(createJob).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "cache-warmup",
        namespacePath: "/"
      })
    );
  });

  it("requires a source repository before creating a job", () => {
    const createJob = vi.fn();
    const cancel = vi.fn();
    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        repositories={[{ id: "vectis-local", namespacePath: "/", canAuthor: true }]}
        namespacePath="/"
        onCancel={cancel}
        onCreateJob={createJob}
        onError={() => undefined}
        onUpdateJob={() => undefined}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, name: "missing-source" }}
      />
    );
    fireEvent.submit(screen.getByRole("button", { name: "Create" }).closest("form")!);
    expect(screen.getByLabelText("Repository ID")).toHaveAccessibleDescription(
      "Enter a registered source repository ID."
    );
    expect(createJob).not.toHaveBeenCalled();
    expect(cancel).not.toHaveBeenCalled();
  });

  it("offers registered repositories and marks unavailable authoring choices", () => {
    const valuesChange = vi.fn();
    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        namespacePath="/"
        repositories={[
          { id: "writable", namespacePath: "/team", canAuthor: true },
          { id: "read-only", namespacePath: "/", canAuthor: false }
        ]}
        onCancel={() => undefined}
        onCreateJob={() => undefined}
        onError={() => undefined}
        onUpdateJob={() => undefined}
        onValuesChange={valuesChange}
        values={emptyJobForm}
      />
    );
    expect(screen.getByLabelText("Repository ID").tagName).toBe("SELECT");
    expect(screen.getByLabelText("Repository ID").querySelector('option[value="read-only"]')).toBeDisabled();
    fireEvent.change(screen.getByLabelText("Repository ID"), { target: { value: "writable" } });
    expect(valuesChange).toHaveBeenCalledWith(expect.objectContaining({ repository: "writable" }));
    expect(screen.queryByLabelText("Description")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("State")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Cadence")).not.toBeInTheDocument();
  });

  it("waits for a successful update before closing", async () => {
    let resolveUpdate: (() => void) | undefined;
    const updateJob = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveUpdate = resolve;
        })
    );
    const cancel = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "edit", jobID: "worker-image" }}
        repositories={[{ id: "vectis-local", namespacePath: "/", canAuthor: true }]}
        namespacePath="/"
        onCancel={cancel}
        onCreateJob={() => undefined}
        onError={() => undefined}
        onUpdateJob={updateJob}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, repository: "vectis-local", name: "worker-image" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(updateJob).toHaveBeenCalledWith("worker-image", expect.objectContaining({ name: "worker-image" }));
    expect(cancel).not.toHaveBeenCalled();
    expect(screen.getByRole("button", { name: "Save" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    expect(updateJob).toHaveBeenCalledTimes(1);

    await act(async () => {
      resolveUpdate?.();
    });

    await waitFor(() => expect(cancel).toHaveBeenCalled());
  });

  it("keeps the editor open when update fails", async () => {
    const cancel = vi.fn();
    const onError = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "edit", jobID: "worker-image" }}
        repositories={[{ id: "vectis-local", namespacePath: "/", canAuthor: true }]}
        namespacePath="/"
        onCancel={cancel}
        onCreateJob={() => undefined}
        onError={onError}
        onUpdateJob={() => Promise.reject(new Error("invalid stored definition"))}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, repository: "vectis-local", name: "worker-image" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() => expect(onError).toHaveBeenCalledWith("invalid stored definition"));
    expect(cancel).not.toHaveBeenCalled();
  });

  it("reports invalid JSON without closing", () => {
    const cancel = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        repositories={[{ id: "vectis-local", namespacePath: "/", canAuthor: true }]}
        namespacePath="/"
        onCancel={cancel}
        onCreateJob={() => undefined}
        onError={() => undefined}
        onUpdateJob={() => undefined}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, repository: "vectis-local", definition: "{", name: "broken" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(screen.getByLabelText("Payload")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Payload")).toHaveAccessibleDescription("Definition must be valid JSON.");
    expect(cancel).not.toHaveBeenCalled();
  });

  it("keeps the name read-only while configuring an existing job", async () => {
    const updateJob = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "edit", jobID: "worker-image" }}
        repositories={[{ id: "vectis-local", namespacePath: "/", canAuthor: true }]}
        namespacePath="/"
        onCancel={() => undefined}
        onCreateJob={() => undefined}
        onError={() => undefined}
        onUpdateJob={updateJob}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, repository: "vectis-local", name: "worker-image" }}
      />
    );

    expect(screen.getByLabelText("Name")).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    await waitFor(() =>
      expect(updateJob).toHaveBeenCalledWith("worker-image", expect.objectContaining({ name: "worker-image" }))
    );
  });
});
