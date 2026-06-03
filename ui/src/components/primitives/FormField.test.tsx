import { render, screen } from "@testing-library/react";
import { FormField } from "./FormField";

describe("FormField", () => {
  it("renders a labelled input", () => {
    render(<FormField label="Username" name="username" required />);

    const input = screen.getByLabelText("Username");
    expect(input).toHaveAttribute("name", "username");
    expect(input).toBeRequired();
  });

  it("defaults text inputs", () => {
    render(<FormField label="Project" name="project" />);

    expect(screen.getByLabelText("Project")).toHaveAttribute("type", "text");
  });

  it("renders an accessible field error", () => {
    render(<FormField error="Enter a username." label="Username" name="username" />);

    expect(screen.getByLabelText("Username")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByText("Enter a username.")).toBeInTheDocument();
    expect(screen.getByLabelText("Username")).toHaveAccessibleDescription("Enter a username.");
  });
});
