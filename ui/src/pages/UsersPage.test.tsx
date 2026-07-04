import { render, screen } from "@testing-library/react";
import { vi } from "vitest";
import type { Namespace, User } from "../domain/console";
import { UsersPage } from "./UsersPage";

const namespaces: Namespace[] = [
  {
    breakInheritance: false,
    id: 1,
    name: "/",
    path: "/",
    role: "Admin"
  }
];

const users: User[] = [
  {
    id: "12",
    username: "mira",
    role: "Operator",
    roleBindings: [],
    status: "active",
    lastSeen: "Created 20 Jun 2026",
    tokens: 0
  }
];

const pageProps = {
  namespaces,
  onCreateUser: vi.fn(),
  onDeleteUser: vi.fn(),
  onGrantRoleBinding: vi.fn(),
  onOpenUser: vi.fn(),
  onOpenUsers: vi.fn(),
  onRevokeRoleBinding: vi.fn(),
  onUpdateUserStatus: vi.fn(),
  users
};

describe("UsersPage", () => {
  it("anchors the users index in admin navigation", () => {
    render(<UsersPage {...pageProps} />);

    expect(screen.getByLabelText("Users location")).toHaveTextContent("AdminUsers");
  });

  it("anchors missing users in admin navigation", () => {
    render(<UsersPage {...pageProps} selectedUserID="missing" />);

    expect(screen.getByLabelText("User location")).toHaveTextContent("AdminUsersMissing");
    expect(screen.getByRole("heading", { name: "User Not Found" })).toBeInTheDocument();
  });
});
