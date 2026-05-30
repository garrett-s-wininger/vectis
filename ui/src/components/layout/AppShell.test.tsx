import { fireEvent, render, screen } from "@testing-library/react";
import { AppShell, type NavItem } from "./AppShell";
import { Button } from "../primitives/Button";

const navItems: NavItem[] = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/jobs", label: "Jobs" },
  { href: "/runs", label: "Runs" }
];

describe("AppShell", () => {
  it("renders brand, primary navigation, actions, and page content", () => {
    render(
      <AppShell activeHref="/jobs" actions={<Button>Refresh</Button>} brand="Vectis" navItems={navItems}>
        <h1>Jobs</h1>
      </AppShell>
    );

    expect(screen.getByLabelText("Vectis home")).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Primary" })).toBeInTheDocument();

    expect(screen.getByRole("link", { name: "Jobs" })).toHaveAttribute("aria-current", "page");

    expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Jobs" })).toBeInTheDocument();
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
});
