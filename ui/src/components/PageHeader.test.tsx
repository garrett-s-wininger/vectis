import { render, screen } from "@testing-library/react";
import { Button } from "./Button";
import { PageHeader } from "./PageHeader";

describe("PageHeader", () => {
  it("renders heading copy and actions", () => {
    render(
      <PageHeader
        eyebrow="Operator Console"
        title="Cluster dashboard"
        description="Current workload and service health."
        actions={<Button>Refresh</Button>}
      />
    );

    expect(
      screen.getByRole("heading", { name: "Cluster dashboard" })
    ).toBeInTheDocument();

    expect(screen.getByText("Operator Console")).toBeInTheDocument();
    expect(screen.getByText("Current workload and service health.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument();
  });

  it("omits optional regions when not provided", () => {
    render(<PageHeader title="Runs" />);

    expect(screen.getByRole("heading", { name: "Runs" })).toBeInTheDocument();
    expect(screen.queryByText("Operator Console")).not.toBeInTheDocument();
  });
});
