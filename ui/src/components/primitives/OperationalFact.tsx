import type { LucideIcon } from "lucide-react";
import styles from "./OperationalFact.module.css";

type OperationalFactProps = {
  emphasis?: boolean;
  icon: LucideIcon;
  label: string;
  value: string;
};

export function OperationalFact({ emphasis, icon: Icon, label, value }: OperationalFactProps) {
  return (
    <div className={emphasis ? `${styles.root} ${styles.emphasis}` : styles.root}>
      <dt className={styles.label}>
        <span className={styles.icon} aria-hidden="true">
          <Icon />
        </span>
        {label}
      </dt>
      <dd className={styles.value}>{value}</dd>
    </div>
  );
}
