import { fireEvent, render, screen } from "@testing-library/react";
import { useState } from "react";
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

  it("updates through the custom menu surface", () => {
    function Harness() {
      const [status, setStatus] = useState("all");

      return (
        <SelectField
          label="Status"
          name="status"
          onChange={(event) => setStatus(event.target.value)}
          options={[
            { label: "All", value: "all" },
            { label: "Running", value: "running" }
          ]}
          value={status}
        />
      );
    }

    render(<Harness />);

    fireEvent.click(screen.getByLabelText("Status: All"));
    fireEvent.click(screen.getByRole("button", { name: "Running" }));

    expect(screen.getByLabelText("Status")).toHaveValue("running");
    expect(screen.getByLabelText("Status: Running")).toBeInTheDocument();
  });
});
