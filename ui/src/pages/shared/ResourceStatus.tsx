type ResourceStatusTone = "active" | "disabled" | "enabled" | "paused";

type ResourceStatusProps = {
  children: string;
  className?: string;
  tone: ResourceStatusTone;
};

export function ResourceStatus({ children, className, tone }: ResourceStatusProps) {
  return <span className={`resource-status resource-status--${tone} ${className ?? ""}`.trim()}>{children}</span>;
}
