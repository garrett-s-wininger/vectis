import type { LucideIcon } from "lucide-react";
import type { ReactNode } from "react";
import styles from "./OperationalFact.module.css";

type OperationalFactProps = {
  className?: string;
  layout?: "metric" | "detail";
  emphasis?: boolean;
  icon: LucideIcon;
  label: string;
  value: ReactNode;
  wrap?: boolean;
};

export function OperationalFact({
  className,
  layout = "metric",
  emphasis,
  icon: Icon,
  label,
  value,
  wrap
}: OperationalFactProps) {
  return (
    <div
      className={[
        styles.root,
        className,
        layout === "detail" ? styles.detail : "",
        emphasis ? styles.emphasis : "",
        wrap ? styles.wrap : ""
      ]
        .filter(Boolean)
        .join(" ")}
    >
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
