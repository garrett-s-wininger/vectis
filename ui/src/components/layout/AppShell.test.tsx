import { fireEvent, render, screen } from "@testing-library/react";
import { AppShell, type NavEntry } from "./AppShell";
import { Button } from "../primitives/Button";
import { NamespacePicker } from "../navigation/NamespacePicker";
import type { Namespace } from "../../domain/console";

const navItems: NavEntry[] = [
  { href: "/runs", label: "Runs" },
  { href: "/jobs", label: "Jobs" }
];

const utilityNavItems: NavEntry[] = [
  {
    label: "Admin",
    items: [
      { href: "/health", label: "Health" },
      { href: "/users", label: "Users" }
    ]
  }
];

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

describe("AppShell", () => {
  it("renders brand, primary navigation, actions, and page content", () => {
    render(
      <AppShell activeHref="/jobs" actions={<Button>Refresh</Button>} brand="Vectis" navItems={navItems}>
        <h1>Jobs</h1>
      </AppShell>
    );

    expect(screen.getByLabelText("Vectis home")).toHaveAttribute("href", "/jobs");
    expect(screen.getByRole("navigation", { name: "Primary" })).toBeInTheDocument();

    expect(screen.getByRole("link", { name: "Jobs" })).toHaveAttribute("aria-current", "page");

    expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Jobs" })).toBeInTheDocument();
  });

  it("renders utility navigation and account menu", () => {
    render(
      <AppShell
        accountName="admin"
        activeHref="/users"
        brand="Vectis"
        navItems={navItems}
        utilityNavItems={utilityNavItems}
      >
        <h1>Jobs</h1>
      </AppShell>
    );

    expect(screen.getByRole("navigation", { name: "Utility" })).toBeInTheDocument();
    expect(screen.getByText("Admin")).toBeInTheDocument();
    expect(screen.getByText("admin")).toBeInTheDocument();

    fireEvent.click(screen.getByText("admin"));

    expect(screen.getByRole("link", { name: "Profile" })).toHaveAttribute("href", "/profile");
  });

  it("can delegate navigation", () => {
    const onNavigate = vi.fn((_, event) => event.preventDefault());

    render(
      <AppShell activeHref="/jobs" brand="Vectis" navItems={navItems} onNavigate={onNavigate}>
        <h1>Jobs</h1>
      </AppShell>
    );

    fireEvent.click(screen.getByRole("link", { name: "Runs" }));

    expect(onNavigate).toHaveBeenCalledWith("/runs", expect.any(Object));
  });

  it("can delegate grouped navigation", () => {
    const onNavigate = vi.fn((_, event) => event.preventDefault());

    render(
      <AppShell
        activeHref="/users"
        brand="Vectis"
        navItems={navItems}
        onNavigate={onNavigate}
        utilityNavItems={utilityNavItems}
      >
        <h1>Users</h1>
      </AppShell>
    );

    fireEvent.click(screen.getByText("Admin"));
    const adminMenu = screen.getByText("Admin").closest("details");

    expect(adminMenu).toHaveAttribute("open");

    fireEvent.click(screen.getByRole("link", { name: "Health" }));

    expect(onNavigate).toHaveBeenCalledWith("/health", expect.any(Object));
    expect(adminMenu).not.toHaveAttribute("open");
  });

  it("can sign out from the account menu", () => {
    const onSignOut = vi.fn();

    render(
      <AppShell accountName="admin" activeHref="/jobs" brand="Vectis" navItems={navItems} onSignOut={onSignOut}>
        <h1>Jobs</h1>
      </AppShell>
    );

    fireEvent.click(screen.getByText("admin"));
    const accountMenu = screen.getByText("admin").closest("details");

    expect(accountMenu).toHaveAttribute("open");

    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));

    expect(onSignOut).toHaveBeenCalled();
    expect(accountMenu).not.toHaveAttribute("open");
  });

  it("can render an auth-disabled account indicator without session actions", () => {
    render(
      <AppShell
        accountDetail="Auth disabled"
        accountName="Anonymous"
        activeHref="/jobs"
        brand="Vectis"
        navItems={navItems}
        showProfile={false}
      >
        <h1>Jobs</h1>
      </AppShell>
    );

    expect(screen.getByText("Anonymous")).toBeInTheDocument();
    expect(screen.queryByRole("link", { name: "Profile" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Sign out" })).not.toBeInTheDocument();
    expect(screen.queryByText("Auth disabled")).not.toBeInTheDocument();
  });

  it("omits sign out when no sign out handler is provided", () => {
    render(
      <AppShell accountName="admin" activeHref="/jobs" brand="Vectis" navItems={navItems}>
        <h1>Jobs</h1>
      </AppShell>
    );

    fireEvent.click(screen.getByText("admin"));

    expect(screen.getByRole("link", { name: "Profile" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Sign out" })).not.toBeInTheDocument();
  });

  it("closes other dropdowns when a dropdown opens", () => {
    render(
      <AppShell
        accountName="admin"
        activeHref="/jobs"
        brand="Vectis"
        navItems={navItems}
        utilityNavItems={utilityNavItems}
      >
        <h1>Jobs</h1>
      </AppShell>
    );

    fireEvent.click(screen.getByText("Admin"));
    const adminMenu = screen.getByText("Admin").closest("details");

    expect(adminMenu).toHaveAttribute("open");

    fireEvent.click(screen.getByText("admin"));
    const accountMenu = screen.getByText("admin").closest("details");

    expect(accountMenu).toHaveAttribute("open");
    expect(adminMenu).not.toHaveAttribute("open");
  });

  it("coordinates shell dropdowns with page dropdowns", () => {
    render(
      <AppShell
        accountName="admin"
        activeHref="/jobs"
        brand="Vectis"
        navItems={navItems}
        utilityNavItems={utilityNavItems}
      >
        <NamespacePicker compact namespaces={namespaces} onChange={() => undefined} value="/" />
      </AppShell>
    );

    fireEvent.click(screen.getByLabelText("Namespace"));
    const namespaceMenu = screen.getByLabelText("Namespace").closest("details");

    expect(namespaceMenu).toHaveAttribute("open");

    fireEvent.click(screen.getByText("Admin"));
    const adminMenu = screen.getByText("Admin").closest("details");

    expect(adminMenu).toHaveAttribute("open");
    expect(namespaceMenu).not.toHaveAttribute("open");

    fireEvent.click(screen.getByLabelText("Namespace"));

    expect(namespaceMenu).toHaveAttribute("open");
    expect(adminMenu).not.toHaveAttribute("open");
  });

  it("dismisses open dropdowns on outside click and Escape", () => {
    render(
      <AppShell accountName="admin" activeHref="/jobs" brand="Vectis" navItems={navItems}>
        <button type="button">Page action</button>
      </AppShell>
    );

    fireEvent.click(screen.getByText("admin"));
    const accountMenu = screen.getByText("admin").closest("details");

    expect(accountMenu).toHaveAttribute("open");

    fireEvent.pointerDown(screen.getByRole("button", { name: "Page action" }));

    expect(accountMenu).not.toHaveAttribute("open");

    fireEvent.click(screen.getByText("admin"));

    expect(accountMenu).toHaveAttribute("open");

    fireEvent.keyDown(document, { key: "Escape" });

    expect(accountMenu).not.toHaveAttribute("open");
  });
});
