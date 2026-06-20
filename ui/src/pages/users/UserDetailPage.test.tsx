import { fireEvent, render, screen, within } from "@testing-library/react";
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
  it("keeps bound namespaces visible but hides other role choices", () => {
    render(
      <UserDetailPage
        namespaces={namespaces}
        onBack={() => undefined}
        onGrantRoleBinding={() => undefined}
        onRemoveUser={() => undefined}
        onRevokeRoleBinding={() => undefined}
        onUpdateUserStatus={() => undefined}
        user={user}
      />
    );

    fireEvent.change(screen.getByLabelText("Namespace"), { target: { value: "2" } });

    expect(screen.getByRole("option", { name: "/team-a" })).toBeEnabled();
    expect(screen.getByRole("option", { name: "Root" })).toBeEnabled();
    expect(within(screen.getByLabelText("Role")).getByRole("option", { name: "Operator" })).toBeInTheDocument();
    expect(within(screen.getByLabelText("Role")).queryByRole("option", { name: "Admin" })).not.toBeInTheDocument();
    expect(within(screen.getByLabelText("Role")).queryByRole("option", { name: "Viewer" })).not.toBeInTheDocument();
    expect(within(screen.getByLabelText("Role")).queryByRole("option", { name: "Trigger" })).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Grant" })).toBeDisabled();
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
    expect(screen.getByRole("button", { name: "Grant" })).toBeDisabled();
  });
});
