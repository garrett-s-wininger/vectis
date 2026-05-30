import styles from "./ProgressMeter.module.css";

export type ProgressTone = "neutral" | "warning" | "critical";

export type ProgressMeterProps = {
  label: string;
  value: number;
  detail?: string;
  tone?: ProgressTone;
};

function clampPercent(value: number) {
  return Math.min(100, Math.max(0, value));
}

export function ProgressMeter({ label, value, detail, tone = "neutral" }: ProgressMeterProps) {
  const percent = clampPercent(value);
  const className = tone === "neutral" ? styles.root : `${styles.root} ${styles[tone]}`;

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
