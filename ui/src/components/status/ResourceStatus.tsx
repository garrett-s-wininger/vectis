import type { ReactNode } from "react";
import styles from "./ResourceStatus.module.css";

export type ResourceStatusTone =
  | "active"
  | "danger"
  | "disabled"
  | "enabled"
  | "info"
  | "neutral"
  | "paused"
  | "success"
  | "warning";

type ResourceStatusProps = {
  children: ReactNode;
  className?: string;
  tone: ResourceStatusTone;
};

export function ResourceStatus({ children, className, tone }: ResourceStatusProps) {
  return <span className={`${styles.root} ${styles[tone]} ${className ?? ""}`.trim()}>{children}</span>;
}
