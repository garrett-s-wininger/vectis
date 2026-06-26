import { ResourceStatus, type ResourceStatusTone } from "../status/ResourceStatus";
import styles from "./SignalList.module.css";

const stateLabels = {
  healthy: "Healthy",
  degraded: "Degraded",
  offline: "Offline",
  unknown: "Unknown"
} as const;

export type SignalState = keyof typeof stateLabels;

export type SignalItem = {
  id: string;
  label: string;
  detail?: string;
  state: SignalState;
};

type SignalListProps = {
  signals: SignalItem[];
  variant?: "default" | "stretch";
};

export function SignalList({ signals, variant = "default" }: SignalListProps) {
  if (signals.length === 0) {
    return <p className={styles.empty}>No signals to show.</p>;
  }

  const className = variant === "stretch" ? `${styles.root} ${styles.stretch}` : styles.root;

  return (
    <ul className={className}>
      {signals.map((signal) => (
        <li className={styles.item} key={signal.id}>
          <span className={`${styles.marker} ${styles[`${signal.state}Marker`]}`} />
          <div className={styles.copy}>
            <strong>{signal.label}</strong>
            {signal.detail ? <small>{signal.detail}</small> : null}
          </div>
          <ResourceStatus tone={signalStateTone(signal.state)}>{stateLabels[signal.state]}</ResourceStatus>
        </li>
      ))}
    </ul>
  );
}

function signalStateTone(state: SignalState): ResourceStatusTone {
  switch (state) {
    case "healthy":
      return "success";
    case "degraded":
      return "warning";
    case "offline":
      return "danger";
    case "unknown":
      return "neutral";
  }
}
