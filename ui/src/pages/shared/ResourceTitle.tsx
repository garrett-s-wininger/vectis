type ResourceTitleProps = {
  id?: string;
  subtitle?: string;
  title: string;
};

export function ResourceTitle({ id, subtitle, title }: ResourceTitleProps) {
  return (
    <div className="resource-title">
      <strong id={id}>{title}</strong>
      {subtitle ? <small>{subtitle}</small> : null}
    </div>
  );
}
