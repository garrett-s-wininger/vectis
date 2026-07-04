import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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
    expect(within(select).getByRole("option", { name: "Running" })).toHaveValue("running");
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
    fireEvent.click(within(screen.getByRole("listbox")).getByRole("option", { name: "Running" }));

    expect(screen.getByLabelText("Status")).toHaveValue("running");
    expect(screen.getByLabelText("Status: Running")).toBeInTheDocument();
  });

  it("supports keyboard navigation on the custom menu surface", async () => {
    function Harness() {
      const [status, setStatus] = useState("all");

      return (
        <SelectField
          label="Status"
          name="status"
          onChange={(event) => setStatus(event.target.value)}
          options={[
            { label: "All", value: "all" },
            { disabled: true, label: "Queued", value: "queued" },
            { label: "Running", value: "running" }
          ]}
          value={status}
        />
      );
    }

    render(<Harness />);

    const summary = screen.getByLabelText("Status: All");

    fireEvent.keyDown(summary, { key: "ArrowDown" });

    const listbox = screen.getByRole("listbox");
    const runningOption = within(listbox).getByRole("option", { name: "Running" });

    await waitFor(() => expect(runningOption).toHaveFocus());

    fireEvent.keyDown(listbox, { key: "Escape" });

    expect(summary.closest("details")).not.toHaveAttribute("open");
    expect(summary).toHaveFocus();
  });
});
