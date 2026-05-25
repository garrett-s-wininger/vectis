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

export function ProgressMeter({
  label,
  value,
  detail,
  tone = "neutral"
}: ProgressMeterProps) {
  const percent = clampPercent(value);

  return (
    <div className={`progress-meter progress-meter--${tone}`}>
      <div className="progress-meter__header">
        <span>{label}</span>
        <strong>{percent}%</strong>
      </div>
      <div
        aria-label={label}
        aria-valuemax={100}
        aria-valuemin={0}
        aria-valuenow={percent}
        className="progress-meter__track"
        role="progressbar"
      >
        <span className="progress-meter__bar" style={{ width: `${percent}%` }} />
      </div>
      {detail ? <small className="progress-meter__detail">{detail}</small> : null}
    </div>
  );
}
