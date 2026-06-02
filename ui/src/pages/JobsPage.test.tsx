import { fireEvent, render, screen } from "@testing-library/react";
import { JobsPage } from "./JobsPage";

const namespaces = [
  {
    id: 1,
    name: "/",
    path: "/",
    breakInheritance: false,
    role: "Admin" as const
  }
];

describe("JobsPage", () => {
  it("renders a first-job empty state", () => {
    const createJob = vi.fn();

    render(
      <JobsPage
        jobs={[]}
        namespaces={namespaces}
        namespacePath="/"
        onCreateJob={createJob}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByRole("heading", { name: "Create One Today" })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Create" })).toHaveLength(1);

    fireEvent.click(screen.getAllByRole("button", { name: "Create" })[0]);

    expect(screen.getByRole("region", { name: "Create" })).toBeInTheDocument();
    expect(screen.queryByLabelText("Namespace")).not.toBeInTheDocument();
  });

  it("creates a stored job from the new job workflow", () => {
    const createJob = vi.fn();

    render(
      <JobsPage
        jobs={[]}
        namespaces={namespaces}
        namespacePath="/platform"
        onCreateJob={createJob}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    fireEvent.click(screen.getAllByRole("button", { name: "Create" })[0]);

    expect(screen.getByRole("heading", { name: "Source" })).toBeInTheDocument();
    expect(screen.getByText("Source Control")).toBeInTheDocument();
    expect(screen.getByText("Triggers")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "worker-image" } });
    fireEvent.click(screen.getByLabelText("Manual"));
    fireEvent.change(screen.getByLabelText("Schedule"), { target: { value: "Custom" } });
    fireEvent.change(screen.getByLabelText("Cron Spec"), { target: { value: "*/15 * * * *" } });
    fireEvent.change(screen.getByLabelText("JSON"), {
      target: {
        value: JSON.stringify({
          id: "worker-image",
          root: {
            uses: "builtins/shell",
            with: { command: "echo build" }
          }
        })
      }
    });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(createJob).toHaveBeenCalledWith(
      expect.objectContaining({
        name: "worker-image",
        manualEnabled: false,
        namespacePath: "/platform",
        schedule: "Cron: */15 * * * *",
        status: "enabled"
      })
    );
  });
});
