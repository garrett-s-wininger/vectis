import type { ReactNode } from "react";
import styles from "./MetricCard.module.css";

export type MetricTone = "neutral" | "attention" | "success";

export type MetricCardProps = {
  label: string;
  value: ReactNode;
  detail?: string;
  tone?: MetricTone;
};

export function MetricCard({ label, value, detail, tone = "neutral" }: MetricCardProps) {
  const className = tone === "neutral" ? styles.root : `${styles.root} ${styles[tone]}`;

  return (
    <article className={className}>
      <span className={styles.label}>{label}</span>
      <strong className={styles.value}>{value}</strong>
      {detail ? <small className={styles.detail}>{detail}</small> : null}
    </article>
  );
}
