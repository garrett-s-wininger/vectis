import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { Namespace } from "../domain/console";
import type { Job } from "../domain/console";
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

const jobs: Job[] = [
  {
    branch: "",
    id: "deploy-api",
    name: "deploy-api",
    namespacePath: "/team-a",
    nextRun: "On demand",
    repository: "",
    schedule: "Manual",
    sourceDetail: "Stored in Vectis",
    sourceKind: "db",
    status: "enabled",
    triggers: [{ detail: "On demand", kind: "manual" }]
  }
];

describe("NamespacesPage", () => {
  it("creates namespaces from the form", () => {
    const onCreateNamespace = vi.fn();

    render(
      <NamespacesPage
        canDeleteNamespace={() => false}
        jobs={jobs}
        namespaces={namespaces}
        onCreateNamespace={onCreateNamespace}
        onDeleteNamespace={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
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
        jobs={jobs}
        namespaces={namespaces}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
      />
    );

    expect(screen.getByLabelText("Delete /")).toBeDisabled();
    expect(screen.getByLabelText("Delete /team-a")).toBeEnabled();
  });

  it("shows namespace details with jobs and children", () => {
    render(
      <NamespacesPage
        canDeleteNamespace={() => true}
        jobs={jobs}
        namespaces={[
          ...namespaces,
          {
            breakInheritance: true,
            id: 3,
            name: "edge",
            parentID: 2,
            path: "/team-a/edge",
            role: "Operator"
          }
        ]}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        selectedNamespaceID={2}
      />
    );

    expect(screen.getByRole("heading", { name: "/team-a" })).toBeInTheDocument();
    expect(screen.getByText("deploy-api")).toBeInTheDocument();
    expect(screen.getByText("/team-a/edge")).toBeInTheDocument();
    expect(screen.getByText("Inherited Access")).toBeInTheDocument();
  });
});
