type ResourceStatusTone = "active" | "disabled" | "enabled" | "paused";

type ResourceStatusProps = {
  children: string;
  tone: ResourceStatusTone;
};

export function ResourceStatus({ children, tone }: ResourceStatusProps) {
  return <span className={`resource-status resource-status--${tone}`}>{children}</span>;
}
