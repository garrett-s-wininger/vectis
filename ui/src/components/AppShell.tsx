import type { MouseEvent, ReactNode } from "react";

export type NavItem = {
  href: string;
  label: string;
};

type AppShellProps = {
  brand: string;
  navItems: NavItem[];
  activeHref: string;
  actions?: ReactNode;
  children: ReactNode;
  onNavigate?: (href: string, event: MouseEvent<HTMLAnchorElement>) => void;
};

export function AppShell({
  brand,
  navItems,
  activeHref,
  actions,
  children,
  onNavigate
}: AppShellProps) {
  return (
    <div className="console-shell">
      <header className="console-shell__topbar">
        <a
          className="console-shell__brand"
          href="/"
          aria-label={`${brand} home`}
          onClick={(event) => onNavigate?.("/", event)}
        >
          {brand}
        </a>
        <nav className="console-shell__nav" aria-label="Primary">
          {navItems.map((item) => (
            <a
              aria-current={item.href === activeHref ? "page" : undefined}
              className="console-shell__nav-link"
              href={item.href}
              key={item.href}
              onClick={(event) => onNavigate?.(item.href, event)}
            >
              {item.label}
            </a>
          ))}
        </nav>
        {actions ? <div className="console-shell__actions">{actions}</div> : null}
      </header>
      <main className="console-shell__main">{children}</main>
    </div>
  );
}
