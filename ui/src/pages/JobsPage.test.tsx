import { fireEvent, render, screen } from "@testing-library/react";
import { JobsPage } from "./JobsPage";

describe("JobsPage", () => {
  it("renders a first-job empty state", () => {
    const createJob = vi.fn();

    render(
      <JobsPage
        jobs={[]}
        namespaces={[
          {
            id: 1,
            name: "/",
            path: "/",
            breakInheritance: false,
            role: "Admin"
          }
        ]}
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

    fireEvent.click(screen.getAllByRole("button", { name: "New" })[0]);

    expect(screen.getByRole("region", { name: "New job" })).toBeInTheDocument();
  });
});
