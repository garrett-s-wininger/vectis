import { render, screen } from "@testing-library/react";
import { JobSourceOptions } from "./JobSourceOptions";

describe("JobSourceOptions", () => {
  it("shows inline source as available and source control as unavailable", () => {
    render(<JobSourceOptions />);

    expect(screen.getByText("Inline")).toBeInTheDocument();
    expect(screen.getByText("Available")).toBeInTheDocument();
    expect(screen.getByText("Source Control")).toBeInTheDocument();
    expect(screen.getByText("Coming soon")).toBeInTheDocument();
    expect(screen.getByText("Source Control").closest("[role='listitem']")).toHaveAttribute("aria-disabled", "true");
  });
});
