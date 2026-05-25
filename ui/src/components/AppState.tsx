import type { ReactNode } from "react";

type AppStateTone = "neutral" | "loading" | "empty" | "error";

type AppStateProps = {
  actions?: ReactNode;
  description?: string;
  title: string;
  tone?: AppStateTone;
};

export function AppState({
  actions,
  description,
  title,
  tone = "neutral"
}: AppStateProps) {
  return (
    <section className={`app-state app-state--${tone}`} aria-label={title}>
      <div className="app-state__copy">
        <h2>{title}</h2>
        {description ? <p>{description}</p> : null}
      </div>
      {actions ? <div className="app-state__actions">{actions}</div> : null}
    </section>
  );
}
