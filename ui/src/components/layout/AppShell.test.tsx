import { fireEvent, render, screen } from "@testing-library/react";
import { AppShell, type NavEntry } from "./AppShell";
import { Button } from "../primitives/Button";

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
});
