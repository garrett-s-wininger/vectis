import { render, screen } from "@testing-library/react";
import { Button } from "../primitives/Button";
import { EmptyStatePanel } from "./EmptyStatePanel";

describe("EmptyStatePanel", () => {
  it("renders in-page empty state copy and actions", () => {
    render(
      <EmptyStatePanel
        actions={<Button>Create</Button>}
        description="Stored jobs are reusable definitions."
        eyebrow="No stored jobs"
        title="Create One Today"
        titleID="jobs-empty-title"
      />
    );

    expect(screen.getByRole("region", { name: "Create One Today" })).toBeInTheDocument();
    expect(screen.getByText("No stored jobs")).toBeInTheDocument();
    expect(screen.getByText("Stored jobs are reusable definitions.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Create" })).toBeInTheDocument();
  });
});
