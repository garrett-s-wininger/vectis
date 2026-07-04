import type { ReactNode } from "react";
import styles from "./MetricCard.module.css";

export type MetricTone = "neutral" | "attention" | "success";
export type MetricVariant = "default" | "plain";

export type MetricCardProps = {
  label: string;
  value: ReactNode;
  detail?: string;
  tone?: MetricTone;
  variant?: MetricVariant;
};

export function MetricCard({ label, value, detail, tone = "neutral", variant = "default" }: MetricCardProps) {
  const className = [styles.root, variant === "plain" ? styles.plain : "", tone === "neutral" ? "" : styles[tone]]
    .filter(Boolean)
    .join(" ");

  return (
    <article className={className}>
      <span className={styles.label}>{label}</span>
      <strong className={styles.value}>{value}</strong>
      {detail ? <small className={styles.detail}>{detail}</small> : null}
    </article>
  );
}
