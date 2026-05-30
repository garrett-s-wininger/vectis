import styles from "./StatusBadge.module.css";

const statusLabels = {
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
  abandoned: "Abandoned"
} as const;

export type RunStatus = keyof typeof statusLabels;

type StatusBadgeProps = {
  status: RunStatus;
};

export function StatusBadge({ status }: StatusBadgeProps) {
  return (
    <span className={`${styles.root} ${styles[status]}`}>
      {statusLabels[status]}
    </span>
  );
}
