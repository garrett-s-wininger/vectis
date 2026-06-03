import { render, screen } from "@testing-library/react";
import { TextAreaField } from "./TextAreaField";

describe("TextAreaField", () => {
  it("renders a labeled textarea", () => {
    render(<TextAreaField label="Definition JSON" name="definition" value="{}" readOnly />);

    expect(screen.getByLabelText("Definition JSON")).toHaveValue("{}");
  });

  it("renders an accessible field error", () => {
    render(<TextAreaField error="Definition must be valid JSON." label="Definition JSON" name="definition" />);

    expect(screen.getByLabelText("Definition JSON")).toHaveAttribute("aria-invalid", "true");
    expect(screen.getByLabelText("Definition JSON")).toHaveAccessibleDescription("Definition must be valid JSON.");
  });
});
