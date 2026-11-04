import { Briefcase, GitBranch, HeartPulse, Play, Plug, Settings2, Users } from "lucide-react";
import { runListFiltersFromSearch, type RunListFilters } from "./runListFilters";
import { runInvestigationFromSearch, type RunInvestigationContext } from "./runInvestigation";
import type { NavEntry } from "../components";

export { navigateTo } from "./navigation";

export type AppRouteKind =
  | "repositories"
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
  workspaceNamespace?: string;
  repositoryManagement?: boolean;
  repositoryID?: string;
  repositoryNamespace?: string;
  jobRepository?: string;
  jobNamespace?: string;
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
  runFilters?: RunListFilters;
  runID?: string;
  runInvestigation?: RunInvestigationContext;
  userID?: string;
};

export const primaryNavItems: NavEntry[] = [
  { href: "/jobs", label: "Jobs", icon: Briefcase },
  { href: "/runs", label: "Runs", icon: Play },
  { href: "/repositories", label: "Repositories", icon: GitBranch }
];

export const managementNavItems: NavEntry[] = [
  {
    label: "Manage",
    icon: Settings2,
    items: [
      { href: "/health", label: "Health", icon: HeartPulse },
      { href: "/manage/repositories", label: "Repository connections", shortLabel: "Connections", icon: Plug },
      { href: "/users", label: "Users", icon: Users }
    ]
  }
];

export function routeFromPath(pathname: string, search = ""): AppRoute {
  return {
    ...parseRoute(pathname, search),
    workspaceNamespace: new URLSearchParams(search).get("namespace") || undefined
  };
}

function parseRoute(pathname: string, search = ""): AppRoute {
  if (pathname === "/setup") {
    return { kind: "setup", activeHref: "", pathname };
  }

  if (pathname === "/login") {
    return { kind: "login", activeHref: "", pathname };
  }

  if (pathname === "/" || pathname === "") return { kind: "jobs", activeHref: "/jobs", pathname };

  if (pathname === "/manage/repositories" || pathname.startsWith("/manage/repositories/"))
    return {
      kind: "repositories",
      repositoryManagement: true,
      activeHref: "/manage/repositories",
      pathname,
      repositoryID:
        pathname === "/manage/repositories"
          ? undefined
          : decodeRouteSegment(pathname.slice("/manage/repositories/".length))
    };

  if (pathname === "/repositories" || pathname.startsWith("/repositories/")) {
    return {
      kind: "repositories",
      activeHref: "/repositories",
      pathname,
      repositoryID:
        pathname === "/repositories" ? undefined : decodeRouteSegment(pathname.slice("/repositories/".length)),
      repositoryNamespace: new URLSearchParams(search).get("namespace") || undefined
    };
  }

  const jobContext = {
    jobRepository: new URLSearchParams(search).get("repository") || undefined,
    jobNamespace: new URLSearchParams(search).get("namespace") || undefined
  };

  if (pathname === "/health") {
    return { kind: "health", activeHref: "/health", pathname };
  }

  if (pathname.startsWith("/health/")) {
    return {
      kind: "health",
      activeHref: "/health",
      cellID: decodeRouteSegment(pathname.slice("/health/".length)),
      pathname
    };
  }

  if (pathname === "/runs" || pathname === "/runs/") {
    const runFilters = runListFiltersFromSearch(search);
    return { kind: "runs", activeHref: "/runs", pathname, runJobName: runFilters.job.trim() || undefined, runFilters };
  }

  if (pathname.startsWith("/runs/")) {
    return {
      kind: "runs",
      activeHref: "/runs",
      pathname,
      runID: pathname.slice("/runs/".length),
      runInvestigation: runInvestigationFromSearch(search)
    };
  }

  if (pathname === "/jobs/create") {
    return { kind: "jobs", activeHref: "/jobs", jobEditor: { kind: "create" }, pathname };
  }

  if (pathname.startsWith("/jobs/") && pathname.endsWith("/config")) {
    const jobID = pathname.slice("/jobs/".length, -"/config".length);

    if (jobID) {
      return {
        ...jobContext,
        kind: "jobs",
        activeHref: "/jobs",
        jobEditor: { kind: "edit", jobID: decodeRouteSegment(jobID) },
        pathname
      };
    }
  }

  if (pathname.startsWith("/jobs/")) {
    const jobID = pathname.slice("/jobs/".length);

    if (jobID) {
      return { ...jobContext, kind: "jobs", activeHref: "/jobs", jobID: decodeRouteSegment(jobID), pathname };
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

function decodeRouteSegment(value: string) {
  try {
    return decodeURIComponent(value);
  } catch {
    return value;
  }
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
