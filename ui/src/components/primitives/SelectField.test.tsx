import { render, screen } from "@testing-library/react";
import { SelectField } from "./SelectField";

describe("SelectField", () => {
  it("renders a labelled select", () => {
    render(
      <SelectField
        label="Status"
        name="status"
        options={[
          { label: "All", value: "all" },
          { label: "Running", value: "running" }
        ]}
        value="all"
      />
    );

    const select = screen.getByLabelText("Status");
    expect(select).toHaveAttribute("name", "status");
    expect(screen.getByRole("option", { name: "Running" })).toHaveValue("running");
  });
});
