import { act, fireEvent, render, screen, within } from "@testing-library/react";
import type { Namespace, User } from "../../domain/console";
import { UserDetailPage } from "./UserDetailPage";

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
    role: "Admin"
  }
];

const user: User = {
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
};

describe("UserDetailPage", () => {
  it("does not carry removal confirmation to another account", () => {
    const remove = vi.fn();
    const props = {
      namespaces,
      onBack: () => undefined,
      onGrantRoleBinding: () => undefined,
      onRemoveUser: remove,
      onRevokeRoleBinding: () => undefined,
      onUpdateUserStatus: () => undefined
    };

    const view = render(<UserDetailPage {...props} user={user} />);
    fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
    expect(screen.getByRole("button", { name: "Confirm removal" })).toBeInTheDocument();

    view.rerender(<UserDetailPage {...props} user={{ ...user, id: "13", username: "lee", roleBindings: [] }} />);
    expect(screen.queryByRole("button", { name: "Confirm removal" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
    expect(remove).not.toHaveBeenCalled();
  });

  it("updates an existing namespace role without revoking it", () => {
    const onGrantRoleBinding = vi.fn();
    render(
      <UserDetailPage
        namespaces={namespaces}
        onBack={() => undefined}
        onGrantRoleBinding={onGrantRoleBinding}
        onRemoveUser={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateUserStatus={() => undefined}
        user={user}
      />
    );

    expect(screen.getByLabelText("User location")).toHaveTextContent("ManageUsersmira");
    fireEvent.change(screen.getByLabelText("Namespace"), { target: { value: "2" } });

    expect(screen.getByRole("option", { name: "/team-a" })).toBeEnabled();
    expect(screen.getByRole("option", { name: "Root" })).toBeEnabled();
    expect(within(screen.getByLabelText("Role")).getByRole("option", { name: "Operator" })).toBeInTheDocument();
    expect(within(screen.getByLabelText("Role")).queryByRole("option", { name: "Admin" })).toBeInTheDocument();
    expect(within(screen.getByLabelText("Role")).queryByRole("option", { name: "Viewer" })).toBeInTheDocument();
    expect(within(screen.getByLabelText("Role")).queryByRole("option", { name: "Trigger" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Update Role" })).toBeDisabled();

    fireEvent.change(screen.getByLabelText("Role"), { target: { value: "Viewer" } });
    fireEvent.click(screen.getByRole("button", { name: "Update Role" }));
    expect(onGrantRoleBinding).toHaveBeenCalledWith("12", 2, "Viewer", "Operator");
  });

  it("disables granting when every namespace already has a binding", () => {
    render(
      <UserDetailPage
        namespaces={namespaces}
        onBack={() => undefined}
        onGrantRoleBinding={() => undefined}
        onRemoveUser={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateUserStatus={() => undefined}
        user={{
          ...user,
          role: "Admin",
          roleBindings: [
            ...(user.roleBindings ?? []),
            {
              id: "1:12:Admin",
              namespaceID: 1,
              namespacePath: "/",
              role: "Admin",
              userID: "12",
              username: "mira"
            }
          ]
        }}
      />
    );

    expect(screen.getByRole("option", { name: "Root" })).toBeEnabled();
    expect(screen.getByRole("option", { name: "/team-a" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Update Role" })).toBeDisabled();
  });
});

it("locks account and role actions during a status change and allows retry after failure", async () => {
  let reject!: (error: Error) => void;
  const update = vi.fn(
    () =>
      new Promise<void>((_, fail) => {
        reject = fail;
      })
  );

  render(
    <UserDetailPage
      user={user}
      namespaces={namespaces}
      onBack={() => undefined}
      onGrantRoleBinding={vi.fn()}
      onRevokeRoleBinding={vi.fn()}
      onRemoveUser={vi.fn()}
      onUpdateUserStatus={update}
    />
  );

  fireEvent.click(screen.getByRole("button", { name: "Disable" }));
  const pending = screen.getByRole("button", { name: "Disabling…" });
  expect(pending).toBeDisabled();

  fireEvent.click(pending);
  expect(update).toHaveBeenCalledExactlyOnceWith("12", "disabled");
  expect(screen.getByRole("button", { name: "Revoke" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Remove User" })).toBeDisabled();
  expect(screen.getByLabelText("Role bindings")).toHaveTextContent("/team-a");

  await act(async () => reject(new Error("Failed")));
  expect(screen.getByRole("button", { name: "Disable" })).toBeEnabled();
  expect(screen.getByRole("button", { name: "Revoke" })).toBeEnabled();
});

it("prevents duplicate role updates and conflicting account actions until the update completes", async () => {
  let finish!: () => void;
  const grant = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      })
  );

  render(
    <UserDetailPage
      user={user}
      namespaces={namespaces}
      onBack={() => undefined}
      onGrantRoleBinding={grant}
      onRevokeRoleBinding={vi.fn()}
      onRemoveUser={vi.fn()}
      onUpdateUserStatus={vi.fn()}
    />
  );

  fireEvent.change(screen.getByLabelText("Role"), { target: { value: "Viewer" } });
  fireEvent.click(screen.getByRole("button", { name: "Grant" }));
  const pending = screen.getByRole("button", { name: "Granting…" });
  expect(pending).toBeDisabled();

  fireEvent.click(pending);
  expect(grant).toHaveBeenCalledExactlyOnceWith("12", 1, "Viewer", undefined);
  expect(screen.getByRole("button", { name: "Disable" })).toBeDisabled();
  expect(screen.getByRole("button", { name: "Revoke" })).toBeDisabled();

  await act(async () => finish());
  expect(screen.getByRole("button", { name: "Disable" })).toBeEnabled();
});
