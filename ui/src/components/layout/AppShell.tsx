import type { MouseEvent, ReactNode } from "react";
import { useEffect, useLayoutEffect, useRef, useState } from "react";
import {
  CircleSmall,
  ExternalLink,
  LogOut,
  Menu,
  PanelLeftClose,
  PanelLeftOpen,
  UserRound,
  X,
  type LucideIcon
} from "lucide-react";
import { helpLinks } from "../navigation/helpLinks";
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

export type NavItem = { icon?: LucideIcon; external?: boolean; href: string; label: string; shortLabel?: string };
export type NavGroup = { icon?: LucideIcon; items: NavItem[]; label: string };
export type NavEntry = NavItem | NavGroup;
type AppShellProps = {
  brand: string;
  workspaceControl?: ReactNode;
  contextLabel?: string;
  homeHref?: string;
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

const collapseKey = "vectis.sidebar.collapsed";
const mobileQuery = "(max-width: 900px)";

function savedCollapsed() {
  try {
    return localStorage.getItem(collapseKey) === "true";
  } catch {
    return false;
  }
}

export function AppShell({
  accountName,
  workspaceControl,
  contextLabel,
  homeHref = "/jobs",
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
  const [collapsed, setCollapsed] = useState(savedCollapsed);
  const [mobile, setMobile] = useState(() => window.matchMedia?.(mobileQuery).matches ?? false);
  const navigationKey = `${activeHref}|${homeHref}`;
  const [drawerAt, setDrawerAt] = useState<string | null>(null);
  const drawerOpen = drawerAt === navigationKey;
  const drawer = useRef<HTMLElement>(null);
  const toggle = useRef<HTMLButtonElement>(null);
  const wasOpen = useRef(false);
  const modalOpen = mobile && drawerOpen;
  const logo = theme === "dark" ? "/img/vectis-dark.png" : "/img/vectis.png";
  const contextPath = contextLabel?.startsWith("/") && contextLabel !== "/" ? contextLabel : undefined;
  const contextParent = contextPath?.slice(0, contextPath.lastIndexOf("/"));
  const contextLeaf = contextPath
    ? contextPath.slice(contextPath.lastIndexOf("/") + 1)
    : contextLabel === "/"
      ? "Root"
      : (contextLabel ?? brand);

  const instanceItems = utilityNavItems.flatMap((item) => ("items" in item ? item.items : [item]));
  useEffect(() => {
    const media = window.matchMedia?.(mobileQuery);
    if (!media) return;

    const update = () => {
      setMobile(media.matches);
      setDrawerAt(null);
      closeCoordinatedDropdowns(document);
    };

    media.addEventListener("change", update);
    return () => media.removeEventListener("change", update);
  }, []);

  useLayoutEffect(() => {
    if (modalOpen) drawer.current?.querySelector<HTMLButtonElement>("button")?.focus();
    else if (wasOpen.current) toggle.current?.focus();

    wasOpen.current = modalOpen;
  }, [modalOpen]);

  useEffect(() => {
    function pointer(event: PointerEvent) {
      if (!isInsideCoordinatedDropdown(event.target)) closeCoordinatedDropdowns(document);
    }

    function key(event: KeyboardEvent) {
      if (event.key === "Escape" && !event.defaultPrevented) {
        closeCoordinatedDropdowns(document);
        setDrawerAt(null);
      }

      if (event.key !== "Tab" || !modalOpen || !drawer.current) return;
      const targets = Array.from(
        drawer.current.querySelectorAll<HTMLElement>('a[href], button, input, select, summary, [tabindex="0"]')
      ).filter((node) => node.tabIndex >= 0 && !node.matches(":disabled") && node.getClientRects().length > 0);

      const first = targets[0],
        last = targets.at(-1);

      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last?.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first?.focus();
      }
    }

    document.addEventListener("pointerdown", pointer);
    document.addEventListener("keydown", key);
    return () => {
      document.removeEventListener("pointerdown", pointer);
      document.removeEventListener("keydown", key);
    };
  }, [modalOpen]);

  function toggleNavigation() {
    if (mobile) {
      setDrawerAt((value) => (value === navigationKey ? null : navigationKey));
      return;
    }

    closeCoordinatedDropdowns(document);
    setCollapsed((value) => {
      try {
        localStorage.setItem(collapseKey, String(!value));
      } catch {
        /* NOTE(garrett): Session-only when storage is unavailable. */
      }

      return !value;
    });
  }

  function navigate(href: string, event: MouseEvent<HTMLAnchorElement>) {
    onNavigate?.(href, event);
    if (event.button === 0 && !event.metaKey && !event.ctrlKey && !event.shiftKey && !event.altKey) setDrawerAt(null);
  }

  return (
    <div className={styles.root} data-sidebar-collapsed={!mobile && collapsed ? "true" : undefined}>
      <a className={styles.skipLink} href="#vectis-main-content">
        Skip to content
      </a>
      {modalOpen ? <div className={styles.scrim} aria-hidden="true" onClick={() => setDrawerAt(null)} /> : null}
      <aside
        id="vectis-navigation"
        ref={drawer}
        className={styles.sidebar}
        hidden={mobile && !drawerOpen}
        role={mobile ? "dialog" : undefined}
        aria-modal={mobile ? true : undefined}
        aria-label={mobile ? "Navigation" : "Application navigation"}
      >
        <div className={styles.sidebarBrand}>
          <a
            className={styles.brand}
            href={homeHref}
            aria-label={`${brand} home`}
            onClick={(event) => navigate(homeHref, event)}
          >
            <img className={styles.brandLogo} src={logo} alt="" aria-hidden="true" />
            <span className={styles.brandText}>
              <strong>{brand}</strong>
              <small>Console</small>
            </span>
          </a>
          {mobile ? (
            <button
              type="button"
              className={styles.iconButton}
              aria-label="Close navigation"
              onClick={() => setDrawerAt(null)}
            >
              <X aria-hidden="true" />
            </button>
          ) : null}
        </div>
        {workspaceControl ? <div className={styles.workspace}>{workspaceControl}</div> : null}
        <div className={styles.sidebarScroll}>
          <nav className={styles.nav} aria-label="Primary">
            <SidebarLinks
              activeHref={activeHref}
              items={navItems.flatMap((item) => ("items" in item ? item.items : [item]))}
              onNavigate={navigate}
            />
          </nav>
          {instanceItems.length ? (
            <section className={styles.instance} aria-label="Instance administration">
              <h2 className={styles.sectionLabel}>Instance</h2>
              <nav className={styles.nav} aria-label="Administration">
                <SidebarLinks activeHref={activeHref} items={instanceItems} onNavigate={navigate} />
              </nav>
            </section>
          ) : null}
        </div>
        {!mobile ? (
          <div className={styles.sidebarFooter}>
            <button
              type="button"
              className={styles.collapseButton}
              onClick={toggleNavigation}
              aria-controls="vectis-navigation"
              aria-expanded={!collapsed}
              aria-label={collapsed ? "Expand navigation" : "Collapse navigation"}
              title={collapsed ? "Expand navigation" : "Collapse navigation"}
            >
              {collapsed ? <PanelLeftOpen aria-hidden="true" /> : <PanelLeftClose aria-hidden="true" />}
              <span className={styles.linkText}>Collapse sidebar</span>
            </button>
          </div>
        ) : null}
      </aside>
      <div className={styles.content} inert={modalOpen}>
        <header className={styles.topbar}>
          {mobile ? (
            <button
              ref={toggle}
              type="button"
              className={styles.iconButton}
              onClick={toggleNavigation}
              aria-controls="vectis-navigation"
              aria-expanded={drawerOpen}
              aria-label="Open navigation"
              title="Open navigation"
            >
              <Menu aria-hidden="true" />
            </button>
          ) : null}
          <img className={styles.mobileLogo} src={logo} alt="" aria-hidden="true" />
          <div className={styles.context} title={contextLabel}>
            <span className={styles.contextPath}>
              {contextParent ? <span className={styles.contextParent}>{contextParent}</span> : null}
              {contextParent ? (
                <span className={styles.contextSeparator} aria-hidden="true">
                  /
                </span>
              ) : null}
              <span className={styles.contextLeaf}>{contextLeaf}</span>
            </span>
            <span className={styles.contextCaption}>{contextLabel?.startsWith("/") ? "Namespace" : "Console"}</span>
          </div>
          <div className={styles.actions}>
            {actions}
            <nav className={styles.utilityNav} aria-label="Utility">
              <NavEntries activeHref={activeHref} items={[helpLinks(docsURL)]} onNavigate={navigate} />
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
                className={styles.accountMenu}
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
                        navigate("/profile", event);
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
        <main id="vectis-main-content" tabIndex={-1} className={styles.main}>
          {children}
        </main>
      </div>
    </div>
  );
}

function SidebarLinks({
  items,
  activeHref,
  onNavigate
}: {
  items: NavItem[];
  activeHref: string;
  onNavigate: NonNullable<AppShellProps["onNavigate"]>;
}) {
  return items.map((item) => {
    const Icon = item.icon ?? CircleSmall;
    return (
      <a
        key={item.href}
        className={styles.navLink}
        href={item.href}
        aria-label={item.label}
        aria-current={item.href.split("?")[0] === activeHref ? "page" : undefined}
        title={item.label}
        onClick={(event) => onNavigate(item.href, event)}
      >
        <Icon aria-hidden="true" />
        <span className={styles.linkText}>{item.shortLabel ?? item.label}</span>
      </a>
    );
  });
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
      return (
        <details
          {...coordinatedDropdownProps}
          className={styles.navGroup}
          key={item.label}
          onToggle={(event) => closeOtherDropdowns(event.currentTarget)}
        >
          <summary
            className={styles.navGroupSummary}
            aria-label={item.label}
            title={item.label}
            onClick={(event) => closeOtherDropdownsFromTrigger(event.currentTarget)}
          >
            {item.icon ? <item.icon aria-hidden="true" /> : null}
            <span className={styles.navGroupLabel}>{item.label}</span>
          </summary>
          <div className={styles.navMenu}>
            {item.items.map((child) => (
              <a
                aria-current={!child.external && child.href.split("?")[0] === activeHref ? "page" : undefined}
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
        aria-current={item.href.split("?")[0] === activeHref ? "page" : undefined}
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
