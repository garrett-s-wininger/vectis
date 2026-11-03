import { ConsoleAccessProvider } from "../domain/access";
import { act, fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { Namespace } from "../domain/console";
import type { Job } from "../domain/console";
import type { User } from "../domain/console";
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

const users: User[] = [
  {
    id: "12",
    username: "mira",
    role: "Operator",
    roleBindings: [
      {
        id: "2:12:Operator",
        namespaceID: 2,
        namespacePath: "/team-a",
        role: "Operator",
        userID: "12",
        username: "mira"
      }
    ],
    status: "active",
    lastSeen: "Created 20 Jun 2026",
    tokens: 0
  },
  {
    id: "13",
    username: "lee",
    role: "Unassigned",
    roleBindings: [],
    status: "active",
    lastSeen: "Created 20 Jun 2026",
    tokens: 0
  }
];

describe("NamespacesPage", () => {
  it("creates namespaces from the form", () => {
    const onCreateNamespace = vi.fn();

    render(
      <NamespacesPage
        canDeleteNamespace={() => false}
        editorMode={null}
        jobs={jobs}
        namespaces={namespaces}
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={onCreateNamespace}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateNamespace={() => undefined}
        users={users}
      />
    );

    expect(screen.getByLabelText("Namespaces location")).toHaveTextContent("ManageNamespaces");
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

  it("only offers namespace creation under a parent the account administers", () => {
    const create = vi.fn();

    render(
      <ConsoleAccessProvider
        access={{
          actions: ["admin:*", "job:read"],
          namespaces: { "/": ["job:read"], "/team-a": ["admin:*", "job:read"] }
        }}
      >
        <NamespacesPage
          canDeleteNamespace={() => false}
          editorMode={null}
          jobs={jobs}
          namespaces={namespaces}
          onCloseEditor={() => undefined}
          onConfigureNamespace={() => undefined}
          onCreateNamespace={create}
          onDeleteNamespace={() => undefined}
          onGrantRoleBinding={() => undefined}
          onOpenJobs={() => undefined}
          onOpenNamespace={() => undefined}
          onOpenNamespaces={() => undefined}
          onRevokeRoleBinding={() => undefined}
          onUpdateNamespace={() => undefined}
          users={users}
        />
      </ConsoleAccessProvider>
    );

    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    expect(screen.getByLabelText("Parent")).toHaveValue("2");
    expect(screen.queryByRole("option", { name: "Root" })).not.toBeInTheDocument();

    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "child" } });
    fireEvent.click(screen.getByRole("button", { name: "Create Namespace" }));
    expect(create).toHaveBeenCalledWith({ name: "child", description: "", parentID: 2 });
  });

  it("keeps destructive actions out of the namespace list", () => {
    render(
      <NamespacesPage
        canDeleteNamespace={(namespaceID) => namespaceID === 2}
        editorMode={null}
        jobs={jobs}
        namespaces={namespaces}
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateNamespace={() => undefined}
        users={users}
      />
    );

    expect(screen.getByRole("button", { name: "View /" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "View /team-a" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: /Delete/ })).not.toBeInTheDocument();
  });

  it("only enables delete on namespace detail when the namespace can be removed", () => {
    const { rerender } = render(
      <NamespacesPage
        canDeleteNamespace={() => false}
        editorMode={null}
        jobs={jobs}
        namespaces={namespaces}
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateNamespace={() => undefined}
        selectedNamespaceID={1}
        users={users}
      />
    );

    expect(screen.getByRole("button", { name: "Delete" })).toBeDisabled();

    rerender(
      <NamespacesPage
        canDeleteNamespace={(namespaceID) => namespaceID === 2}
        editorMode={null}
        jobs={jobs}
        namespaces={namespaces}
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateNamespace={() => undefined}
        selectedNamespaceID={2}
        users={users}
      />
    );

    expect(screen.getByRole("button", { name: "Delete" })).toBeEnabled();
  });

  it("shows namespace details with jobs and children", () => {
    render(
      <NamespacesPage
        canDeleteNamespace={() => true}
        editorMode={null}
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
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateNamespace={() => undefined}
        selectedNamespaceID={2}
        users={users}
      />
    );

    expect(screen.getByLabelText("Namespace location")).toHaveTextContent("ManageNamespaces/team-a");
    expect(screen.getByRole("heading", { name: "/team-a" })).toBeInTheDocument();
    expect(screen.getByText("deploy-api")).toBeInTheDocument();
    expect(screen.getByText("/team-a/edge")).toBeInTheDocument();
    expect(screen.getByText("Inherited Access")).toBeInTheDocument();
  });

  it("manages direct role bindings from namespace detail", async () => {
    const onGrantRoleBinding = vi.fn();
    const onRevokeRoleBinding = vi.fn();

    render(
      <NamespacesPage
        canDeleteNamespace={() => true}
        editorMode={null}
        jobs={jobs}
        namespaces={namespaces}
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={onGrantRoleBinding}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        onRevokeRoleBinding={onRevokeRoleBinding}
        onUpdateNamespace={() => undefined}
        selectedNamespaceID={2}
        users={users}
      />
    );

    expect(screen.getByLabelText("Namespace role bindings")).toHaveTextContent("mira");
    expect(screen.getByLabelText("Namespace role bindings")).toHaveTextContent("Operator");

    fireEvent.change(screen.getByLabelText("User"), { target: { value: "13" } });
    fireEvent.change(screen.getByLabelText("Role"), { target: { value: "Viewer" } });
    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Grant" })));
    expect(onGrantRoleBinding).toHaveBeenCalledWith("13", 2, "Viewer", undefined);

    await act(async () => fireEvent.click(screen.getByRole("button", { name: "Revoke" })));
    expect(onRevokeRoleBinding).toHaveBeenCalledWith("12", 2, "Operator");
  });

  it("renders a namespace-specific not found state", () => {
    const onOpenNamespaces = vi.fn();

    render(
      <NamespacesPage
        canDeleteNamespace={() => true}
        editorMode={null}
        jobs={jobs}
        namespaces={namespaces}
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={onOpenNamespaces}
        onRevokeRoleBinding={() => undefined}
        onUpdateNamespace={() => undefined}
        selectedNamespaceID={99}
        users={users}
      />
    );

    expect(screen.getByRole("heading", { name: "Namespace Not Found" })).toBeInTheDocument();
    expect(screen.getByLabelText("Namespace location")).toHaveTextContent("ManageNamespacesMissing");
    expect(screen.getByRole("region", { name: "No Namespace Found" })).toBeInTheDocument();
    expect(screen.queryByText("Create One Today")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "View Namespaces" }));
    expect(onOpenNamespaces).toHaveBeenCalledOnce();
  });

  it("updates namespace descriptions from configure mode", () => {
    const onUpdateNamespace = vi.fn();

    render(
      <NamespacesPage
        canDeleteNamespace={() => true}
        editorMode={{ kind: "edit", namespaceID: 2 }}
        jobs={jobs}
        namespaces={namespaces}
        onCloseEditor={() => undefined}
        onConfigureNamespace={() => undefined}
        onCreateNamespace={() => undefined}
        onDeleteNamespace={() => undefined}
        onGrantRoleBinding={() => undefined}
        onOpenJobs={() => undefined}
        onOpenNamespace={() => undefined}
        onOpenNamespaces={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateNamespace={onUpdateNamespace}
        selectedNamespaceID={2}
        users={users}
      />
    );

    expect(screen.getByLabelText("Namespace configure location")).toHaveTextContent("ManageNamespaces/team-aConfigure");
    fireEvent.change(screen.getByLabelText("Description"), { target: { value: "Updated namespace detail." } });
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect(onUpdateNamespace).toHaveBeenCalledWith(2, { description: "Updated namespace detail." });
  });
});
