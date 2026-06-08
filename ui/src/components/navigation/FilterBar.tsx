import type { ReactNode } from "react";
import styles from "./FilterBar.module.css";

type FilterBarProps = {
  filters: ReactNode;
  actions?: ReactNode;
};

export function FilterBar({ filters, actions }: FilterBarProps) {
  return (
    <div className={`${styles.root} polished-panel`}>
      <div className={styles.filters}>{filters}</div>
      {actions ? <div className={styles.actions}>{actions}</div> : null}
    </div>
  );
}
