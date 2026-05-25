import type { NavItem } from "../components/AppShell";

export type AppRouteKind =
  | "dashboard"
  | "runs"
  | "jobs"
  | "users"
  | "namespaces"
  | "setup"
  | "login"
  | "notFound";

export type AppRoute = {
  kind: AppRouteKind;
  activeHref: string;
  pathname: string;
};

export const primaryNavItems: NavItem[] = [
  { href: "/", label: "Dashboard" },
  { href: "/runs", label: "Runs" },
  { href: "/jobs", label: "Jobs" },
  { href: "/users", label: "Users" },
  { href: "/namespaces", label: "Namespaces" }
];

export function routeFromPath(pathname: string): AppRoute {
  if (pathname === "/setup") {
    return { kind: "setup", activeHref: "", pathname };
  }

  if (pathname === "/login") {
    return { kind: "login", activeHref: "", pathname };
  }

  if (pathname === "/" || pathname === "") {
    return { kind: "dashboard", activeHref: "/", pathname: "/" };
  }

  if (pathname === "/runs" || pathname.startsWith("/runs/")) {
    return { kind: "runs", activeHref: "/runs", pathname };
  }

  if (pathname === "/jobs" || pathname.startsWith("/jobs/")) {
    return { kind: "jobs", activeHref: "/jobs", pathname };
  }

  if (pathname === "/users" || pathname.startsWith("/users/")) {
    return { kind: "users", activeHref: "/users", pathname };
  }

  if (pathname === "/namespaces" || pathname.startsWith("/namespaces/")) {
    return { kind: "namespaces", activeHref: "/namespaces", pathname };
  }

  return { kind: "notFound", activeHref: "", pathname };
}

export function navigateTo(path: string) {
  window.history.pushState(null, "", path);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

export function safeNextPath(
  search = window.location.search,
  origin = window.location.origin
) {
  const next = new URLSearchParams(search).get("next");
  if (!next) {
    return null;
  }

  try {
    const url = new URL(next, origin);
    if (url.origin !== origin) {
      return null;
    }

    if (url.pathname === "/login" || url.pathname === "/setup") {
      return null;
    }

    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return null;
  }
}
