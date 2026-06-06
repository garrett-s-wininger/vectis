import type { DataTableColumn, RunListItem, SignalItem } from "../components";
import type { Namespace } from "../domain/console";

export type JobTableFixtureRow = {
  id: string;
  name: string;
  repository: string;
  status: string;
};

export const storyRunListItems: RunListItem[] = [
  {
    id: "run-184",
    jobName: "linux-ci",
    runNumber: 184,
    cellName: "local",
    commit: "6f4c2d7a",
    duration: "4m 12s",
    namespacePath: "/team-a",
    source: "stored",
    submittedBy: "mira",
    trigger: "manual",
    status: "running"
  },
  {
    id: "run-812",
    jobName: "api-smoke",
    runNumber: 812,
    cellName: "edge",
    commit: "b82e19f4",
    duration: "6m 03s",
    namespacePath: "/team-a/edge",
    source: "ephemeral",
    submittedBy: "admin",
    trigger: "manual",
    status: "running"
  },
  {
    id: "run-88",
    jobName: "nightly-load",
    runNumber: 88,
    cellName: "prod-west",
    commit: "a19f03de",
    duration: "14m 02s",
    namespacePath: "/prod",
    source: "stored",
    submittedBy: "cron",
    trigger: "schedule",
    status: "failed"
  }
];

export const storySignals: SignalItem[] = [
  {
    id: "cron",
    label: "Cron",
    detail: "schedules evaluated 34s ago",
    state: "healthy"
  },
  {
    id: "registry",
    label: "Registry",
    detail: "one peer lagging",
    state: "degraded"
  },
  {
    id: "logs",
    label: "Logs",
    detail: "ingest connected",
    state: "healthy"
  },
  {
    id: "worker-pool",
    label: "Worker pool",
    detail: "one worker drained",
    state: "unknown"
  }
];

export const storyJobRows: JobTableFixtureRow[] = [
  {
    id: "api",
    name: "api-test-suite",
    repository: "github.com/vectis/api",
    status: "Enabled"
  },
  {
    id: "docs",
    name: "docs-publish",
    repository: "github.com/vectis/docs",
    status: "Paused"
  }
];

export const storyJobColumns: DataTableColumn<JobTableFixtureRow>[] = [
  { header: "Name", cell: (row) => row.name },
  { header: "Repository", cell: (row) => row.repository },
  { header: "Status", cell: (row) => row.status, align: "end" }
];

export const storyNamespaces: Namespace[] = [
  {
    id: 1,
    name: "/",
    path: "/",
    breakInheritance: false,
    role: "Admin"
  },
  {
    id: 2,
    name: "team-a",
    parentID: 1,
    path: "/team-a",
    breakInheritance: false,
    role: "Operator"
  }
];
