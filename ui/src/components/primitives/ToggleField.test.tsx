import { fireEvent, render, screen } from "@testing-library/react";
import { ToggleField } from "./ToggleField";

describe("ToggleField", () => {
  it("renders the active value and reports changes", () => {
    const onChange = vi.fn();

    render(<ToggleField checked label="Manual" name="manual" offText="Off" onChange={onChange} onText="Allowed" />);

    expect(screen.getByLabelText("Manual")).toBeChecked();
    expect(screen.getByText("Allowed")).toBeInTheDocument();

    fireEvent.click(screen.getByLabelText("Manual"));

    expect(onChange).toHaveBeenCalledWith(false);
  });
});
