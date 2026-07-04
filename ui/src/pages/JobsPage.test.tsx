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
    const openCreate = vi.fn();

    render(
      <JobsPage
        jobs={[]}
        namespaces={namespaces}
        namespacePath="/"
        onCloseEditor={() => undefined}
        onCreateJob={createJob}
        onOpenCreate={openCreate}
        onOpenEditor={() => undefined}
        onOpenJob={() => undefined}
        onOpenJobRuns={() => undefined}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByRole("heading", { name: "Create One Today" })).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Jobs location" })).toBeInTheDocument();
    expect(screen.getByText("Root")).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(screen.getAllByRole("button", { name: "Create" })).toHaveLength(1);

    fireEvent.click(screen.getAllByRole("button", { name: "Create" })[0]);

    expect(openCreate).toHaveBeenCalled();
  });

  it("creates a stored job from the new job workflow", () => {
    const createJob = vi.fn();

    render(
      <JobsPage
        editorMode={{ kind: "create" }}
        jobs={[]}
        namespaces={namespaces}
        namespacePath="/platform"
        onCloseEditor={() => undefined}
        onCreateJob={createJob}
        onOpenCreate={() => undefined}
        onOpenEditor={() => undefined}
        onOpenJob={() => undefined}
        onOpenJobRuns={() => undefined}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByRole("heading", { name: "Source" })).toBeInTheDocument();
    expect(screen.getByText("Source Control")).toBeInTheDocument();
    expect(screen.getByText("Triggers")).toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "worker-image" } });
    fireEvent.click(screen.getByLabelText("Manual"));
    fireEvent.change(screen.getByLabelText("Cadence"), { target: { value: "Custom" } });
    fireEvent.change(screen.getByLabelText("Cron Spec"), { target: { value: "*/15 * * * *" } });
    fireEvent.change(screen.getByLabelText("Payload"), {
      target: {
        value: JSON.stringify({
          id: "worker-image",
          root: {
            uses: "builtins/script",
            with: { script: "echo build" }
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

  it("shows an explicit no-runs state for jobs without run history", () => {
    const job = {
      id: "test-run",
      name: "test-run",
      repository: "",
      branch: "",
      sourceDetail: "Stored in Vectis",
      sourceKind: "db" as const,
      definition: JSON.stringify({ id: "test-run", root: {} }),
      namespacePath: "/",
      schedule: "Manual",
      nextRun: "On demand",
      triggers: [{ kind: "manual" as const, detail: "On demand" }],
      status: "enabled" as const
    };

    render(
      <JobsPage
        jobs={[job]}
        namespaces={namespaces}
        namespacePath="/"
        onCloseEditor={() => undefined}
        onCreateJob={() => undefined}
        onOpenCreate={() => undefined}
        onOpenEditor={() => undefined}
        onOpenJob={() => undefined}
        onOpenJobRuns={() => undefined}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByText("None")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "View latest run for test-run" })).not.toBeInTheDocument();
  });

  it("renders a job detail view", () => {
    const openJob = vi.fn();
    const openEditor = vi.fn();
    const openJobRuns = vi.fn();
    const selectNamespace = vi.fn();
    const triggerRun = vi.fn();
    const job = {
      id: "test-run",
      name: "test-run",
      repository: "",
      branch: "",
      sourceDetail: "Stored in Vectis",
      sourceKind: "db" as const,
      definition: JSON.stringify({ id: "test-run", root: { uses: "builtins/script" } }),
      namespacePath: "/",
      schedule: "Manual",
      nextRun: "On demand",
      triggers: [{ kind: "manual" as const, detail: "On demand" }],
      status: "enabled" as const
    };

    render(
      <JobsPage
        detailJobID="test-run"
        jobs={[job]}
        namespaces={namespaces}
        namespacePath="/"
        onCloseEditor={() => undefined}
        onCreateJob={() => undefined}
        onOpenCreate={() => undefined}
        onOpenEditor={openEditor}
        onOpenJob={openJob}
        onOpenJobRuns={openJobRuns}
        onSelectNamespace={selectNamespace}
        onSelectRun={() => undefined}
        onTriggerRun={triggerRun}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByRole("heading", { name: "test-run" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Definition" })).toBeInTheDocument();
    expect(screen.getByText("Root")).toBeInTheDocument();
    expect(screen.getByText("Reusable job definition stored in Vectis.")).toBeInTheDocument();
    expect(screen.getByText(/builtins\/script/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Jobs" }));
    expect(openJob).toHaveBeenCalledWith("");

    expect(screen.queryByRole("button", { name: "View Root namespace jobs" })).not.toBeInTheDocument();
    expect(selectNamespace).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Run test-run" }));
    expect(triggerRun).toHaveBeenCalledWith("test-run");

    fireEvent.click(screen.getByRole("button", { name: "Edit test-run" }));
    expect(openEditor).toHaveBeenCalledWith("test-run");

    fireEvent.click(screen.getByRole("button", { name: "View all runs for test-run" }));
    expect(openJobRuns).toHaveBeenCalledWith("test-run");
  });

  it("renders a shared missing state for unknown job detail routes", () => {
    const openJob = vi.fn();

    render(
      <JobsPage
        detailJobID="missing-job"
        jobs={[]}
        namespaces={namespaces}
        namespacePath="/"
        onCloseEditor={() => undefined}
        onCreateJob={() => undefined}
        onOpenCreate={() => undefined}
        onOpenEditor={() => undefined}
        onOpenJob={openJob}
        onOpenJobRuns={() => undefined}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByRole("heading", { name: "Job Not Found" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "No Job Found" })).toBeInTheDocument();
    expect(screen.getByText("Missing Job")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "View Jobs" }));
    expect(openJob).toHaveBeenCalledWith("");
  });

  it("uses all jobs for routed detail views while preserving scoped list jobs", () => {
    const scopedJob = {
      id: "scoped-job",
      name: "scoped-job",
      repository: "",
      branch: "",
      sourceDetail: "Stored in Vectis",
      sourceKind: "db" as const,
      definition: JSON.stringify({ id: "scoped-job", root: {} }),
      namespacePath: "/",
      schedule: "Manual",
      nextRun: "On demand",
      triggers: [{ kind: "manual" as const, detail: "On demand" }],
      status: "enabled" as const
    };

    const routedJob = {
      ...scopedJob,
      definition: JSON.stringify({ id: "routed-job", root: {} }),
      id: "routed-job",
      name: "routed-job",
      namespacePath: "/team-a"
    };

    render(
      <JobsPage
        allJobs={[scopedJob, routedJob]}
        detailJobID="routed-job"
        jobs={[scopedJob]}
        namespaces={namespaces}
        namespacePath="/"
        onCloseEditor={() => undefined}
        onCreateJob={() => undefined}
        onOpenCreate={() => undefined}
        onOpenEditor={() => undefined}
        onOpenJob={() => undefined}
        onOpenJobRuns={() => undefined}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByRole("heading", { name: "routed-job" })).toBeInTheDocument();
    expect(screen.getByText("/team-a")).toBeInTheDocument();
  });

  it("routes configure breadcrumbs to the job detail and jobs index separately", () => {
    const closeEditor = vi.fn();
    const openJob = vi.fn();
    const job = {
      id: "test-run",
      name: "test-run",
      repository: "",
      branch: "",
      sourceDetail: "Stored in Vectis",
      sourceKind: "db" as const,
      definition: JSON.stringify({ id: "test-run", root: {} }),
      namespacePath: "/",
      schedule: "Manual",
      nextRun: "On demand",
      triggers: [{ kind: "manual" as const, detail: "On demand" }],
      status: "enabled" as const
    };

    render(
      <JobsPage
        editorMode={{ kind: "edit", jobID: "test-run" }}
        jobs={[job]}
        namespaces={namespaces}
        namespacePath="/"
        onCloseEditor={closeEditor}
        onCreateJob={() => undefined}
        onOpenCreate={() => undefined}
        onOpenEditor={() => undefined}
        onOpenJob={openJob}
        onOpenJobRuns={() => undefined}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "test-run" }));
    expect(openJob).toHaveBeenCalledWith("test-run");
    expect(closeEditor).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole("button", { name: "Jobs" }));
    expect(closeEditor).toHaveBeenCalled();
  });

  it("uses the routed job id in configure breadcrumbs while job data loads", () => {
    render(
      <JobsPage
        editorMode={{ kind: "edit", jobID: "pending-job" }}
        jobs={[]}
        namespaces={namespaces}
        namespacePath="/"
        onCloseEditor={() => undefined}
        onCreateJob={() => undefined}
        onOpenCreate={() => undefined}
        onOpenEditor={() => undefined}
        onOpenJob={() => undefined}
        onOpenJobRuns={() => undefined}
        onSelectNamespace={() => undefined}
        onSelectRun={() => undefined}
        onTriggerRun={() => undefined}
        onUpdateJob={() => undefined}
        runs={[]}
      />
    );

    expect(screen.getByRole("button", { name: "pending-job" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Create" })).not.toBeInTheDocument();
  });
});
