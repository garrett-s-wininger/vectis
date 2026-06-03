import { act, fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
import { JobEditor, emptyJobForm, jobInputFromValues } from "./JobEditor";
import type { JobFormValues } from "./JobEditor";

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
    const cancel = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        namespacePath="/"
        onCancel={cancel}
        onCreateJob={() => undefined}
        onError={() => undefined}
        onUpdateJob={() => undefined}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, definition: "{", name: "broken" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(screen.getByLabelText("JSON")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("JSON")).toHaveAccessibleDescription("Definition must be valid JSON.");
    expect(cancel).not.toHaveBeenCalled();
  });

  it("reports invalid custom cron specs without closing", () => {
    const cancel = vi.fn();

    render(
      <JobEditor
        error=""
        mode={{ kind: "create" }}
        namespacePath="/"
        onCancel={cancel}
        onCreateJob={() => undefined}
        onError={() => undefined}
        onUpdateJob={() => undefined}
        onValuesChange={() => undefined}
        values={{ ...emptyJobForm, cronSpec: "60 * * * *", name: "broken-schedule", schedule: "Custom" }}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(screen.getByLabelText("Cron Spec")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Cron Spec")).toHaveAccessibleDescription("Minute must be between 0 and 59.");
    expect(cancel).not.toHaveBeenCalled();
  });

  it("reports invalid custom cron specs after typing settles", () => {
    vi.useFakeTimers();

    function EditorHarness() {
      const [values, setValues] = useState<JobFormValues>({
        ...emptyJobForm,
        name: "debounced-schedule",
        schedule: "Custom"
      });

      return (
        <JobEditor
          error=""
          mode={{ kind: "create" }}
          namespacePath="/"
          onCancel={() => undefined}
          onCreateJob={() => undefined}
          onError={() => undefined}
          onUpdateJob={() => undefined}
          onValuesChange={setValues}
          values={values}
        />
      );
    }

    try {
      render(<EditorHarness />);

      fireEvent.change(screen.getByLabelText("Cron Spec"), { target: { value: "60 * * * *" } });

      expect(screen.getByLabelText("Cron Spec")).not.toHaveAttribute("aria-invalid", "true");

      act(() => {
        vi.advanceTimersByTime(350);
      });

      expect(screen.getByLabelText("Cron Spec")).toHaveAttribute("aria-invalid", "true");
      expect(screen.getByLabelText("Cron Spec")).toHaveAccessibleDescription("Minute must be between 0 and 59.");
    } finally {
      vi.useRealTimers();
    }
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
