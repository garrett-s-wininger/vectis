import { render, screen } from "@testing-library/react";
import { AppShell, type NavItem } from "./AppShell";
import { Button } from "./Button";

const navItems: NavItem[] = [
  { href: "/dashboard", label: "Dashboard" },
  { href: "/jobs", label: "Jobs" },
  { href: "/runs", label: "Runs" }
];

describe("AppShell", () => {
  it("renders brand, primary navigation, actions, and page content", () => {
    render(
      <AppShell
        activeHref="/jobs"
        actions={<Button>Refresh</Button>}
        brand="Vectis"
        navItems={navItems}
      >
        <h1>Jobs</h1>
      </AppShell>
    );

    expect(screen.getByLabelText("Vectis home")).toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Primary" })).toBeInTheDocument();

    expect(screen.getByRole("link", { name: "Jobs" })).toHaveAttribute(
      "aria-current",
      "page"
    );

    expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Jobs" })).toBeInTheDocument();
  });
});
