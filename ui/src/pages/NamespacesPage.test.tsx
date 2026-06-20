import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { Namespace } from "../domain/console";
import { NamespacesPage } from "./NamespacesPage";

const namespaces: Namespace[] = [
  {
    breakInheritance: false,
    id: 1,
    name: "/",
    path: "/",
    role: "Admin"
  },
  {
    breakInheritance: false,
    id: 2,
    name: "team-a",
    parentID: 1,
    path: "/team-a",
    role: "Admin"
  }
];

describe("NamespacesPage", () => {
  it("creates namespaces from the form", () => {
    const onCreateNamespace = vi.fn();

    render(
      <NamespacesPage
        canDeleteNamespace={() => false}
        namespaces={namespaces}
        onCreateNamespace={onCreateNamespace}
        onDeleteNamespace={() => undefined}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "sandbox" } });
    fireEvent.change(screen.getByLabelText("Description"), { target: { value: "Temporary test jobs." } });
    fireEvent.change(screen.getByLabelText("Parent"), { target: { value: "2" } });
    fireEvent.click(screen.getByRole("button", { name: "Create Namespace" }));

    expect(onCreateNamespace).toHaveBeenCalledWith({
      description: "Temporary test jobs.",
      name: "sandbox",
      parentID: 2
    });
  });

  it("only enables delete when the namespace can be removed", () => {
    render(
      <NamespacesPage
        canDeleteNamespace={(namespaceID) => namespaceID === 2}
        namespaces={namespaces}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
      />
    );

    expect(screen.getByLabelText("Delete /")).toBeDisabled();
    expect(screen.getByLabelText("Delete /team-a")).toBeEnabled();
  });
});
