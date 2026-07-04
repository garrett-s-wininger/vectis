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
      <span className={styles.icon} aria-hidden="true">
        <Icon />
      </span>
      <div className={styles.body}>
        <dt className={styles.label}>{label}</dt>
        <dd className={styles.value}>{value}</dd>
      </div>
    </div>
  );
}
