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
    return <p className="signal-list__empty">No signals to show.</p>;
  }

  return (
    <ul className="signal-list">
      {signals.map((signal) => (
        <li className="signal-list__item" key={signal.id}>
          <span className={`signal-list__marker signal-list__marker--${signal.state}`} />
          <div className="signal-list__copy">
            <strong>{signal.label}</strong>
            {signal.detail ? <small>{signal.detail}</small> : null}
          </div>
          <span className={`signal-list__state signal-list__state--${signal.state}`}>
            {stateLabels[signal.state]}
          </span>
        </li>
      ))}
    </ul>
  );
}
