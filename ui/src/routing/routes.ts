import type { NavEntry } from "../components";

export type AppRouteKind =
  | "dashboard"
  | "health"
  | "runs"
  | "jobs"
  | "users"
  | "namespaces"
  | "profile"
  | "setup"
  | "login"
  | "notFound";

export type AppRoute = {
  cellID?: string;
  kind: AppRouteKind;
  activeHref: string;
  jobEditor?: { kind: "create" } | { kind: "edit"; jobID: string };
  jobID?: string;
  namespaceEditor?: { kind: "edit"; namespaceID: number };
  namespaceID?: number;
  namespaceMissing?: boolean;
  pathname: string;
  runJobName?: string;
  runID?: string;
  userID?: string;
};

export const primaryNavItems: NavEntry[] = [
  { href: "/jobs", label: "Jobs" },
  { href: "/runs", label: "Runs" }
];

export const adminNavItems: NavEntry[] = [
  {
    label: "Admin",
    items: [
      { href: "/health", label: "Health" },
      { href: "/users", label: "Users" },
      { href: "/namespaces", label: "Namespaces" }
    ]
  }
];

export function routeFromPath(pathname: string, search = ""): AppRoute {
  if (pathname === "/setup") {
    return { kind: "setup", activeHref: "", pathname };
  }

  if (pathname === "/login") {
    return { kind: "login", activeHref: "", pathname };
  }

  if (pathname === "/" || pathname === "" || pathname === "/health") {
    return { kind: "health", activeHref: "/health", pathname };
  }

  if (pathname.startsWith("/health/")) {
    return {
      kind: "health",
      activeHref: "/health",
      cellID: pathname.slice("/health/".length),
      pathname
    };
  }

  if (pathname === "/runs" || pathname === "/runs/") {
    return { kind: "runs", activeHref: "/runs", pathname, runJobName: jobFilterFromSearch(search) };
  }

  if (pathname.startsWith("/runs/")) {
    return {
      kind: "runs",
      activeHref: "/runs",
      pathname,
      runID: pathname.slice("/runs/".length)
    };
  }

  if (pathname === "/jobs/create") {
    return { kind: "jobs", activeHref: "/jobs", jobEditor: { kind: "create" }, pathname };
  }

  if (pathname.startsWith("/jobs/") && pathname.endsWith("/config")) {
    const jobID = pathname.slice("/jobs/".length, -"/config".length);

    if (jobID) {
      return { kind: "jobs", activeHref: "/jobs", jobEditor: { kind: "edit", jobID }, pathname };
    }
  }

  if (pathname.startsWith("/jobs/")) {
    const jobID = pathname.slice("/jobs/".length);

    if (jobID) {
      return { kind: "jobs", activeHref: "/jobs", jobID, pathname };
    }
  }

  if (pathname === "/jobs") {
    return { kind: "jobs", activeHref: "/jobs", pathname };
  }

  if (pathname.startsWith("/users/")) {
    const userID = pathname.slice("/users/".length);

    if (userID) {
      return { kind: "users", activeHref: "/users", pathname, userID };
    }
  }

  if (pathname === "/users") {
    return { kind: "users", activeHref: "/users", pathname };
  }

  if (pathname.startsWith("/namespaces/")) {
    const namespaceConfigMatch = pathname.match(/^\/namespaces\/(\d+)\/config$/);
    if (namespaceConfigMatch?.[1]) {
      const namespaceID = Number(namespaceConfigMatch[1]);
      return {
        kind: "namespaces",
        activeHref: "/namespaces",
        namespaceEditor: { kind: "edit", namespaceID },
        namespaceID,
        pathname
      };
    }

    const namespaceID = Number(pathname.slice("/namespaces/".length));

    if (Number.isInteger(namespaceID) && namespaceID > 0) {
      return { kind: "namespaces", activeHref: "/namespaces", namespaceID, pathname };
    }

    return { kind: "namespaces", activeHref: "/namespaces", namespaceMissing: true, pathname };
  }

  if (pathname === "/namespaces") {
    return { kind: "namespaces", activeHref: "/namespaces", pathname };
  }

  if (pathname === "/profile" || pathname.startsWith("/profile/")) {
    return { kind: "profile", activeHref: "/profile", pathname };
  }

  return { kind: "notFound", activeHref: "", pathname };
}

function jobFilterFromSearch(search: string) {
  const job = new URLSearchParams(search).get("job")?.trim();
  return job || undefined;
}

export function navigateTo(path: string) {
  window.history.pushState(null, "", path);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

export function safeNextPath(search = window.location.search, origin = window.location.origin) {
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

export function safeReturnPath(search = window.location.search, origin = window.location.origin) {
  const returnTo = new URLSearchParams(search).get("returnTo");
  if (!returnTo) {
    return null;
  }

  try {
    const url = new URL(returnTo, origin);
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
