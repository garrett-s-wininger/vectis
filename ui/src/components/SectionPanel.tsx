import type { ReactNode } from "react";

type SectionPanelProps = {
  title: string;
  description?: string;
  actions?: ReactNode;
  children: ReactNode;
};

export function SectionPanel({
  title,
  description,
  actions,
  children
}: SectionPanelProps) {
  return (
    <section className="section-panel" aria-labelledby="section-panel-title">
      <div className="section-panel__header">
        <div className="section-panel__copy">
          <h2 id="section-panel-title">{title}</h2>
          {description ? (
            <p className="section-panel__description">{description}</p>
          ) : null}
        </div>
        {actions ? <div className="section-panel__actions">{actions}</div> : null}
      </div>
      <div className="section-panel__body">{children}</div>
    </section>
  );
}
