import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import styles from "./OperationalFact.module.css";

type OperationalFactProps = {
  emphasis?: boolean;
  icon: LucideIcon;
  label: string;
  value: ReactNode;
  wrap?: boolean;
};

export function OperationalFact({ emphasis, icon: Icon, label, value, wrap }: OperationalFactProps) {
  return (
    <div className={[styles.root, emphasis ? styles.emphasis : "", wrap ? styles.wrap : ""].filter(Boolean).join(" ")}>
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
