import { ResourceStatus, type ResourceStatusTone } from "./ResourceStatus";

const runStatusLabels = {
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
  abandoned: "Abandoned",
  orphaned: "Orphaned",
  aborted: "Aborted"
} as const;

const statusLabels = {
  ...runStatusLabels,
  empty: "None"
} as const;

export type RunStatus = keyof typeof runStatusLabels;
export type StatusBadgeTone = keyof typeof statusLabels;

type StatusBadgeProps = {
  status: StatusBadgeTone;
};

export function StatusBadge({ status }: StatusBadgeProps) {
  return <ResourceStatus tone={statusBadgeTone(status)}>{statusLabels[status]}</ResourceStatus>;
}

export function statusBadgeLabel(status: StatusBadgeTone) {
  return statusLabels[status];
}

function statusBadgeTone(status: StatusBadgeTone): ResourceStatusTone {
  switch (status) {
    case "running":
      return "info";
    case "succeeded":
      return "success";
    case "failed":
      return "danger";
    case "cancelled":
    case "abandoned":
    case "orphaned":
    case "aborted":
      return "warning";
    case "empty":
    case "queued":
      return "neutral";
  }
}
