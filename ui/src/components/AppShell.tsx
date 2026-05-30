import type { MouseEvent, ReactNode } from "react";
import styles from "./AppShell.module.css";

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
    <div className={styles.root}>
      <header className={styles.topbar}>
        <a
          className={styles.brand}
          href="/"
          aria-label={`${brand} home`}
          onClick={(event) => onNavigate?.("/", event)}
        >
          {brand}
        </a>
        <nav className={styles.nav} aria-label="Primary">
          {navItems.map((item) => (
            <a
              aria-current={item.href === activeHref ? "page" : undefined}
              className={styles.navLink}
              href={item.href}
              key={item.href}
              onClick={(event) => onNavigate?.(item.href, event)}
            >
              {item.label}
            </a>
          ))}
        </nav>
        {actions ? <div className={styles.actions}>{actions}</div> : null}
      </header>
      <main className={styles.main}>{children}</main>
    </div>
  );
}
