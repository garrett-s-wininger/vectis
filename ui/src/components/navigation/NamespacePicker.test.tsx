import { fireEvent, render, screen } from "@testing-library/react";
import type { Namespace } from "../../domain/console";
import { NamespacePicker } from "./NamespacePicker";

const namespaces: Namespace[] = [
  {
    id: 1,
    name: "/",
    path: "/",
    breakInheritance: false,
    role: "Admin"
  },
  {
    id: 2,
    name: "team-a",
    parentID: 1,
    path: "/team-a",
    breakInheritance: false,
    role: "Operator"
  }
];

describe("NamespacePicker", () => {
  it("renders namespaces and reports selection changes", () => {
    const onChange = vi.fn();

    render(<NamespacePicker namespaces={namespaces} onChange={onChange} value="/" />);

    fireEvent.change(screen.getByLabelText("Namespace"), {
      target: { value: "/team-a" }
    });

    expect(onChange).toHaveBeenCalledWith("/team-a");
  });

  it("renders the compact namespace menu and reports selection changes", () => {
    const onChange = vi.fn();

    render(<NamespacePicker compact namespaces={namespaces} onChange={onChange} value="/" />);

    fireEvent.click(screen.getByLabelText("Namespace"));
    fireEvent.click(screen.getByRole("button", { name: "/team-a" }));

    expect(onChange).toHaveBeenCalledWith("/team-a");
  });
});
