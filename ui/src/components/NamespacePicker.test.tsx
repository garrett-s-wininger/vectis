import { fireEvent, render, screen } from "@testing-library/react";
import { NamespacePicker } from "./NamespacePicker";
import type { MockNamespace } from "../mocks/consoleData";

const namespaces: MockNamespace[] = [
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
});
