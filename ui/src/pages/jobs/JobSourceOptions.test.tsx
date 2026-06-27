import { render, screen } from "@testing-library/react";
import { JobSourceOptions } from "./JobSourceOptions";

describe("JobSourceOptions", () => {
  it("shows inline source as selected and source control as planned", () => {
    render(<JobSourceOptions />);

    expect(screen.getByText("Inline")).toBeInTheDocument();
    expect(screen.getByText("Selected")).toBeInTheDocument();
    expect(screen.getByText("Source Control")).toBeInTheDocument();
    expect(screen.getByText("Planned")).toBeInTheDocument();
    expect(screen.getByText("Source Control").closest("[role='listitem']")).toHaveAttribute("aria-disabled", "true");
  });
});
