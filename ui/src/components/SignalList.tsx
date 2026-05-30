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
};

export function SignalList({ signals }: SignalListProps) {
  if (signals.length === 0) {
    return <p className={styles.empty}>No signals to show.</p>;
  }

  return (
    <ul className={styles.root}>
      {signals.map((signal) => (
        <li className={styles.item} key={signal.id}>
          <span className={`${styles.marker} ${styles[`${signal.state}Marker`]}`} />
          <div className={styles.copy}>
            <strong>{signal.label}</strong>
            {signal.detail ? <small>{signal.detail}</small> : null}
          </div>
          <span className={`${styles.state} ${styles[`${signal.state}State`]}`}>{stateLabels[signal.state]}</span>
        </li>
      ))}
    </ul>
  );
}
