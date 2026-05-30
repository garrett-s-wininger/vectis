import type { RunListItem, RunStatus } from "../components";
import type { CellStatus, JobStatus, UserRole } from "./console";

export type RunFilter = RunStatus | "all";
export type SourceFilter = NonNullable<RunListItem["source"]> | "all";

export const jobStatusOptions: { label: string; value: JobStatus }[] = [
  { label: "Enabled", value: "enabled" },
  { label: "Paused", value: "paused" }
];

export const jobScheduleOptions = [
  { label: "Manual", value: "Manual" },
  { label: "On push", value: "On push" },
  { label: "Hourly", value: "Hourly" },
  { label: "Nightly", value: "Nightly" }
];

export const defaultJobDefinition = JSON.stringify(
  {
    id: "stored-job",
    root: {
      id: "root",
      uses: "builtins/shell",
      with: {
        command: "echo 'Hello from Vectis'"
      }
    }
  },
  null,
  2
);

export const defaultRunDefinition = JSON.stringify(
  {
    id: "ad-hoc-check",
    root: {
      id: "root",
      uses: "builtins/shell",
      with: {
        command: "echo 'Hello from Vectis'"
      }
    }
  },
  null,
  2
);

export const runStatusLabels: Record<RunFilter, string> = {
  all: "All",
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
  abandoned: "Abandoned"
};

export const runStatusOptions = Object.entries(runStatusLabels).map(([value, label]) => ({
  label,
  value
}));

export const runSourceLabels: Record<SourceFilter, string> = {
  all: "All",
  stored: "Stored jobs",
  ephemeral: "Ephemeral"
};

export const runSourceTitleLabels: Record<SourceFilter, string> = {
  all: "All",
  stored: "Stored job",
  ephemeral: "Ephemeral"
};

export const runSourceOptions = Object.entries(runSourceLabels).map(([value, label]) => ({
  label,
  value
}));

export const userRoleOptions: { label: string; value: UserRole }[] = [
  { label: "Admin", value: "Admin" },
  { label: "Operator", value: "Operator" },
  { label: "Viewer", value: "Viewer" }
];

export function cellStatusLabel(status: CellStatus) {
  switch (status) {
    case "healthy":
      return "Healthy";
    case "degraded":
      return "Degraded";
    case "offline":
      return "Offline";
  }
}

export function cellStatusTone(status: CellStatus) {
  switch (status) {
    case "healthy":
      return "active";
    case "degraded":
      return "paused";
    case "offline":
      return "disabled";
  }
}
