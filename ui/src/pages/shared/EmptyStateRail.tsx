import type { ReactNode } from "react";
import styles from "./EmptyStateRail.module.css";

export function EmptyStateRail({ children }: { children: ReactNode }) {
  return <div className={styles.root}>{children}</div>;
}
