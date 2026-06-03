import styles from "./StatusBadge.module.css";

const runStatusLabels = {
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
  abandoned: "Abandoned"
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
  return <span className={`${styles.root} ${styles[status]}`}>{statusLabels[status]}</span>;
}
