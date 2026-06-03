import { fireEvent, render, screen } from "@testing-library/react";
import { JobEditor, emptyJobForm, jobInputFromValues } from "./JobEditor";

describe("JobEditor", () => {
  it("submits a valid create input", () => {
    const createJob = vi.fn();
    const cancel = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        namespacePath="/platform"
        onCancel={cancel}
        onCreateJob={createJob}
        onError={() => undefined}
        onUpdateJob={() => undefined}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, name: "cache-warmup" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(createJob).toHaveBeenCalledWith(expect.objectContaining({ name: "cache-warmup", namespacePath: "/platform" }));
    expect(cancel).toHaveBeenCalled();
  });

  it("reports invalid JSON without closing", () => {
    const onError = vi.fn();
    const cancel = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        namespacePath="/"
        onCancel={cancel}
        onCreateJob={() => undefined}
        onError={onError}
        onUpdateJob={() => undefined}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, definition: "{", name: "broken" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(onError).toHaveBeenLastCalledWith("Definition must be valid JSON.");
    expect(cancel).not.toHaveBeenCalled();
  });

  it("keeps the name read-only while configuring an existing job", () => {
    const updateJob = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "edit", jobID: "worker-image" }}
        namespacePath="/"
        onCancel={() => undefined}
        onCreateJob={() => undefined}
        onError={() => undefined}
        onUpdateJob={updateJob}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, name: "worker-image" }}
      />
    );

    expect(screen.getByLabelText("Name")).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(updateJob).toHaveBeenCalledWith("worker-image", expect.objectContaining({ name: "worker-image" }));
  });

  it("serializes manual-only jobs for the API", () => {
    expect(jobInputFromValues({ ...emptyJobForm, manualEnabled: true, schedule: "None" })).toEqual(
      expect.objectContaining({ schedule: "Manual" })
    );
  });
});
