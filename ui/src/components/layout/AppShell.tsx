import type { MouseEvent, ReactNode } from "react";
import { ExternalLink, LogOut, UserRound, type LucideIcon } from "lucide-react";
import { helpLinks } from "../navigation/helpLinks";
import { useEffect } from "react";
import {
  closeCoordinatedDropdowns,
  closeOtherDropdowns,
  closeOtherDropdownsFromTrigger,
  closeParentDropdown,
  coordinatedDropdownProps,
  isInsideCoordinatedDropdown
} from "../navigation/dropdownCoordination";
import styles from "./AppShell.module.css";
import { useResolvedTheme } from "../../theme";

export type NavItem = {
  icon?: LucideIcon;
  external?: boolean;
  href: string;
  label: string;
};

export type NavGroup = {
  icon?: LucideIcon;
  items: NavItem[];
  label: string;
};

export type NavEntry = NavItem | NavGroup;

type AppShellProps = {
  brand: string;
  docsURL?: string;
  navItems: NavEntry[];
  activeHref: string;
  actions?: ReactNode;
  accountDetail?: string;
  accountName?: string;
  children: ReactNode;
  showProfile?: boolean;
  onSignOut?: () => void;
  utilityNavItems?: NavEntry[];
  onNavigate?: (href: string, event: MouseEvent<HTMLAnchorElement>) => void;
};

export function AppShell({
  accountName,
  accountDetail,
  brand,
  docsURL,
  navItems,
  activeHref,
  actions,
  children,
  onNavigate,
  onSignOut,
  showProfile = true,
  utilityNavItems = []
}: AppShellProps) {
  const isAccountActive = activeHref === "/profile";
  const theme = useResolvedTheme();
  const hasAccountMenu = showProfile || onSignOut;

  useEffect(() => {
    function handlePointerDown(event: PointerEvent) {
      if (isInsideCoordinatedDropdown(event.target)) {
        return;
      }

      closeCoordinatedDropdowns(document);
    }

    function handleKeyDown(event: KeyboardEvent) {
      if (event.key === "Escape") {
        closeCoordinatedDropdowns(document);
      }
    }

    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleKeyDown);

    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleKeyDown);
    };
  }, []);

  return (
    <div className={styles.root}>
      <header className={styles.topbar}>
        <a
          className={styles.brand}
          href="/jobs"
          aria-label={`${brand} home`}
          onClick={(event) => onNavigate?.("/jobs", event)}
        >
          <img
            className={styles.brandLogo}
            src={theme === "dark" ? "/img/vectis-dark.png" : "/img/vectis.png"}
            alt=""
            aria-hidden="true"
          />
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
          <nav className={styles.utilityNav} aria-label="Utility">
            <NavEntries
              activeHref={activeHref}
              items={[...utilityNavItems, helpLinks(docsURL)]}
              onNavigate={onNavigate}
            />
          </nav>
          {accountName && !hasAccountMenu ? (
            <div className={`${styles.accountSummary} ${styles.accountIndicator}`} title={accountDetail}>
              <span className={styles.accountAvatar}>{accountName.slice(0, 1).toUpperCase()}</span>
              <span className={styles.accountName}>{accountName}</span>
            </div>
          ) : null}
          {accountName && hasAccountMenu ? (
            <details
              {...coordinatedDropdownProps}
              className={`${styles.accountMenu} ${isAccountActive ? styles.accountMenuActive : ""}`}
              onToggle={(event) => closeOtherDropdowns(event.currentTarget)}
            >
              <summary
                className={styles.accountSummary}
                aria-label={`Account menu for ${accountName}`}
                onClick={(event) => closeOtherDropdownsFromTrigger(event.currentTarget)}
              >
                <span className={styles.accountAvatar}>{accountName.slice(0, 1).toUpperCase()}</span>
                <span className={styles.accountName}>{accountName}</span>
              </summary>
              <div className={styles.accountPanel}>
                {accountDetail ? <span className={styles.accountDetail}>{accountDetail}</span> : null}
                {showProfile ? (
                  <a
                    aria-current={isAccountActive ? "page" : undefined}
                    className={styles.navMenuLink}
                    href="/profile"
                    onClick={(event) => {
                      onNavigate?.("/profile", event);
                      closeParentDropdown(event.currentTarget);
                    }}
                  >
                    <UserRound aria-hidden="true" /> Profile
                  </a>
                ) : null}
                {onSignOut ? (
                  <button
                    className={styles.accountAction}
                    onClick={(event) => {
                      closeParentDropdown(event.currentTarget);
                      onSignOut();
                    }}
                    type="button"
                  >
                    <LogOut aria-hidden="true" /> Sign out
                  </button>
                ) : null}
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
        <details
          {...coordinatedDropdownProps}
          className={`${styles.navGroup} ${isActive ? styles.navGroupActive : ""}`}
          key={item.label}
          onToggle={(event) => closeOtherDropdowns(event.currentTarget)}
        >
          <summary
            className={styles.navGroupSummary}
            onClick={(event) => closeOtherDropdownsFromTrigger(event.currentTarget)}
          >
            {item.icon ? <item.icon aria-hidden="true" /> : null}
            <span className={styles.navGroupLabel}>{item.label}</span>
          </summary>
          <div className={styles.navMenu}>
            {item.items.map((child) => (
              <a
                aria-current={!child.external && child.href === activeHref ? "page" : undefined}
                className={styles.navMenuLink}
                href={child.href}
                key={child.href}
                target={child.external ? "_blank" : undefined}
                rel={child.external ? "noopener noreferrer" : undefined}
                aria-label={child.external ? `${child.label} (opens in a new tab)` : undefined}
                onClick={(event) => {
                  if (!child.external) onNavigate?.(child.href, event);
                  closeParentDropdown(event.currentTarget);
                }}
              >
                {child.icon ? <child.icon aria-hidden="true" /> : null}
                {child.label}
                {child.external ? <ExternalLink className={styles.externalIcon} aria-hidden="true" /> : null}
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
        {item.icon ? <item.icon aria-hidden="true" /> : null}
        {item.label}
      </a>
    );
  });
}
