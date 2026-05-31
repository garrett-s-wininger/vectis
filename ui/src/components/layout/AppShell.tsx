import type { MouseEvent, ReactNode } from "react";
import styles from "./AppShell.module.css";

export type NavItem = {
  href: string;
  label: string;
};

export type NavGroup = {
  items: NavItem[];
  label: string;
};

export type NavEntry = NavItem | NavGroup;

type AppShellProps = {
  brand: string;
  navItems: NavEntry[];
  activeHref: string;
  actions?: ReactNode;
  accountName?: string;
  children: ReactNode;
  onSignOut?: () => void;
  utilityNavItems?: NavEntry[];
  onNavigate?: (href: string, event: MouseEvent<HTMLAnchorElement>) => void;
};

export function AppShell({
  accountName,
  brand,
  navItems,
  activeHref,
  actions,
  children,
  onNavigate,
  onSignOut,
  utilityNavItems = []
}: AppShellProps) {
  const isAccountActive = activeHref === "/profile";

  return (
    <div className={styles.root}>
      <header className={styles.topbar}>
        <a
          className={styles.brand}
          href="/jobs"
          aria-label={`${brand} home`}
          onClick={(event) => onNavigate?.("/jobs", event)}
        >
          <img className={styles.brandLogo} src="/img/vectis.png" alt="" aria-hidden="true" />
          <span className={styles.brandText}>
            <strong>{brand}</strong>
            <small>local console</small>
          </span>
        </a>
        <nav className={styles.nav} aria-label="Primary">
          <NavEntries activeHref={activeHref} items={navItems} onNavigate={onNavigate} />
        </nav>
        <div className={styles.actions}>
          {actions}
          {utilityNavItems.length > 0 ? (
            <nav className={styles.utilityNav} aria-label="Utility">
              <NavEntries activeHref={activeHref} items={utilityNavItems} onNavigate={onNavigate} />
            </nav>
          ) : null}
          {accountName ? (
            <details className={`${styles.accountMenu} ${isAccountActive ? styles.accountMenuActive : ""}`}>
              <summary className={styles.accountSummary}>
                <span className={styles.accountAvatar}>{accountName.slice(0, 1).toUpperCase()}</span>
                <span className={styles.accountName}>{accountName}</span>
              </summary>
              <div className={styles.accountPanel}>
                <a
                  aria-current={isAccountActive ? "page" : undefined}
                  className={styles.navMenuLink}
                  href="/profile"
                  onClick={(event) => {
                    onNavigate?.("/profile", event);
                    closeParentDetails(event.currentTarget);
                  }}
                >
                  Profile
                </a>
                <button
                  className={styles.accountAction}
                  onClick={(event) => {
                    closeParentDetails(event.currentTarget);
                    onSignOut?.();
                  }}
                  type="button"
                >
                  Sign out
                </button>
              </div>
            </details>
          ) : null}
        </div>
      </header>
      <main className={styles.main}>{children}</main>
    </div>
  );
}

function NavEntries({
  activeHref,
  items,
  onNavigate
}: {
  activeHref: string;
  items: NavEntry[];
  onNavigate?: (href: string, event: MouseEvent<HTMLAnchorElement>) => void;
}) {
  return items.map((item) => {
    if ("items" in item) {
      const isActive = item.items.some((child) => child.href === activeHref);

      return (
        <details className={`${styles.navGroup} ${isActive ? styles.navGroupActive : ""}`} key={item.label}>
          <summary className={styles.navGroupSummary}>
            <span className={styles.navGroupLabel}>{item.label}</span>
          </summary>
          <div className={styles.navMenu}>
            {item.items.map((child) => (
              <a
                aria-current={child.href === activeHref ? "page" : undefined}
                className={styles.navMenuLink}
                href={child.href}
                key={child.href}
                onClick={(event) => {
                  onNavigate?.(child.href, event);
                  closeParentDetails(event.currentTarget);
                }}
              >
                {child.label}
              </a>
            ))}
          </div>
        </details>
      );
    }

    return (
      <a
        aria-current={item.href === activeHref ? "page" : undefined}
        className={styles.navLink}
        href={item.href}
        key={item.href}
        onClick={(event) => onNavigate?.(item.href, event)}
      >
        {item.label}
      </a>
    );
  });
}

function closeParentDetails(element: HTMLElement) {
  const details = element.closest("details");

  if (details) {
    details.open = false;
  }
}
