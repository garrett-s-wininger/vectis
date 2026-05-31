type ResourceTitleProps = {
  className?: string;
  id?: string;
  subtitle?: string;
  title: string;
};

export function ResourceTitle({ className, id, subtitle, title }: ResourceTitleProps) {
  return (
    <div className={`resource-title ${className ?? ""}`.trim()}>
      <strong id={id}>{title}</strong>
      {subtitle ? <small>{subtitle}</small> : null}
    </div>
  );
}
