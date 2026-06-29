import styles from "./ProgressMeter.module.css";

export type ProgressTone = "neutral" | "warning" | "critical";

export type ProgressMeterProps = {
  label: string;
  value: number;
  detail?: string;
  tone?: ProgressTone;
  variant?: "default" | "card";
};

function clampPercent(value: number) {
  return Math.min(100, Math.max(0, value));
}

export function ProgressMeter({ label, value, detail, tone = "neutral", variant = "default" }: ProgressMeterProps) {
  const percent = clampPercent(value);
  const className = [styles.root, tone === "neutral" ? "" : styles[tone], variant === "card" ? styles.card : ""]
    .filter(Boolean)
    .join(" ");

  return (
    <div className={className}>
      <div className={styles.header}>
        <span>{label}</span>
        <strong>{percent}%</strong>
      </div>
      <div
        aria-label={label}
        aria-valuemax={100}
        aria-valuemin={0}
        aria-valuenow={percent}
        className={styles.track}
        role="progressbar"
      >
        <span className={styles.bar} style={{ width: `${percent}%` }} />
      </div>
      {detail ? <small className={styles.detail}>{detail}</small> : null}
    </div>
  );
}
