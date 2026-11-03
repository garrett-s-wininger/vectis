import { fireEvent, render, screen } from "@testing-library/react";
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
  it("uses the shared copy control for the complete one-time password", async () => {
    const password = "synthetic-initial-password-".repeat(5);
    const writeText = vi.fn().mockResolvedValue(undefined);
    const clipboard = Object.getOwnPropertyDescriptor(navigator, "clipboard");
    Object.defineProperty(navigator, "clipboard", { configurable: true, value: { writeText } });

    try {
      render(<UsersPage {...pageProps} onCreateUser={async () => ({ username: "new-user", password })} />);
      fireEvent.click(screen.getByRole("button", { name: "Create" }));
      fireEvent.change(screen.getByLabelText("Username"), { target: { value: "new-user" } });
      fireEvent.click(screen.getByRole("button", { name: "Add User" }));

      const copy = await screen.findByRole("button", { name: "Copy initial password for new-user" });
      expect(copy.querySelector("svg")).not.toBeNull();
      expect(screen.getByLabelText("Initial password for new-user")).toHaveTextContent(password);

      fireEvent.click(copy);
      expect(await screen.findByText("initial password for new-user copied.")).toBeInTheDocument();
      expect(writeText).toHaveBeenCalledWith(password);

      fireEvent.click(screen.getByRole("button", { name: "Dismiss" }));
      expect(screen.queryByText(password)).not.toBeInTheDocument();
    } finally {
      if (clipboard) Object.defineProperty(navigator, "clipboard", clipboard);
      else Reflect.deleteProperty(navigator, "clipboard");
    }
  });

  it("anchors the users index in admin navigation", () => {
    render(<UsersPage {...pageProps} />);

    expect(screen.getByLabelText("Users location")).toHaveTextContent("ManageUsers");
  });

  it("anchors missing users in admin navigation", () => {
    render(<UsersPage {...pageProps} selectedUserID="missing" />);

    expect(screen.getByLabelText("User location")).toHaveTextContent("ManageUsersMissing");
    expect(screen.getByRole("heading", { name: "User Not Found" })).toBeInTheDocument();
  });
});
