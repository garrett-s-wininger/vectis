import { render, screen } from "@testing-library/react";
import { TextAreaField } from "./TextAreaField";

describe("TextAreaField", () => {
  it("renders a labeled textarea", () => {
    render(<TextAreaField label="Definition JSON" name="definition" value="{}" readOnly />);

    expect(screen.getByLabelText("Definition JSON")).toHaveValue("{}");
  });
});
