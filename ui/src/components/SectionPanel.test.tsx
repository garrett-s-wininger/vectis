import { render, screen } from "@testing-library/react";
import { Button } from "./Button";
import { SectionPanel } from "./SectionPanel";

describe("SectionPanel", () => {
  it("renders heading, description, actions, and content", () => {
    render(
      <SectionPanel
        actions={<Button>View all</Button>}
        description="Currently executing or queued work."
        title="Compute workload"
      >
        <p>Active runs</p>
      </SectionPanel>
    );

    expect(screen.getByRole("heading", { name: "Compute workload" })).toBeInTheDocument();

    expect(screen.getByText("Currently executing or queued work.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "View all" })).toBeInTheDocument();
    expect(screen.getByText("Active runs")).toBeInTheDocument();
  });
});
