import { fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { useState } from "react";
import { SelectField } from "./SelectField";

describe("SelectField", () => {
  it("searches labels, full references and provider descriptions with keyboard selection", async () => {
    const onChange = vi.fn();
    render(
      <SelectField
        searchable
        label="Credential"
        name="credential"
        value=""
        onChange={onChange}
        options={[
          { value: "", label: "None — Public access" },
          { value: "encryptedfs://git/build", label: "git/build", description: "Encrypted filesystem" },
          { value: "other://deploy", label: "Deploy", description: "Other provider" }
        ]}
      />
    );

    const trigger = screen.getByLabelText("Credential: None — Public access");
    fireEvent.keyDown(trigger, { key: "Enter" });
    const search = screen.getByRole("searchbox", { name: "Search credential" });
    await waitFor(() => expect(search).toHaveFocus());
    fireEvent.change(search, { target: { value: "encryptedfs" } });
    const option = within(screen.getByRole("listbox")).getByRole("option", { name: /git\/build/ });
    expect(within(screen.getByRole("listbox")).queryByRole("option", { name: /Deploy/ })).not.toBeInTheDocument();

    fireEvent.keyDown(search, { key: "ArrowDown" });
    await waitFor(() => expect(option).toHaveFocus());
    fireEvent.click(option);
    expect(onChange).toHaveBeenCalledWith(
      expect.objectContaining({ target: { name: "credential", value: "encryptedfs://git/build" } })
    );

    expect(trigger).toHaveFocus();

    fireEvent.click(trigger);
    fireEvent.change(search, { target: { value: "Other provider" } });
    fireEvent.keyDown(search, { key: "Enter" });
    expect(onChange).toHaveBeenLastCalledWith(
      expect.objectContaining({ target: { name: "credential", value: "other://deploy" } })
    );

    fireEvent.click(trigger);
    fireEvent.change(search, { target: { value: "no-match" } });
    expect(within(screen.getByRole("listbox")).getByRole("option", { name: "No matching options." })).toHaveAttribute(
      "aria-disabled",
      "true"
    );

    fireEvent.keyDown(search, { key: "Escape" });
    expect(trigger.closest("details")).not.toHaveAttribute("open");
    expect(trigger).toHaveFocus();
  });

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
