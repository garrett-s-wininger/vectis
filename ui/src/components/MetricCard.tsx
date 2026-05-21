import type { ReactNode } from "react";

type MetricTone = "neutral" | "attention" | "success";

type MetricCardProps = {
  label: string;
  value: ReactNode;
  detail?: string;
  tone?: MetricTone;
};

export function MetricCard({
  label,
  value,
  detail,
  tone = "neutral"
}: MetricCardProps) {
  return (
    <article className={`metric-card metric-card--${tone}`}>
      <span className="metric-card__label">{label}</span>
      <strong className="metric-card__value">{value}</strong>
      {detail ? <small className="metric-card__detail">{detail}</small> : null}
    </article>
  );
}
