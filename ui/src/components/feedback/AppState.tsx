import type { ReactNode } from "react";
import styles from "./AppState.module.css";

type AppStateTone = "neutral" | "loading" | "empty" | "error";

type AppStateProps = {
  actions?: ReactNode;
  description?: string;
  title: string;
  headingLevel?: 1 | 2;
  tone?: AppStateTone;
};

export function AppState({ actions, description, title, tone = "neutral", headingLevel = 2 }: AppStateProps) {
  const Heading = headingLevel === 1 ? "h1" : "h2";
  const className =
    tone === "neutral" || tone === "loading"
      ? `${styles.root} polished-panel polished-panel--accent-top`
      : `${styles.root} ${styles[tone]} polished-panel polished-panel--accent-top`;

  return (
    <section className={className} aria-label={title}>
      <div className={styles.copy}>
        <Heading>{title}</Heading>
        {description ? <p>{description}</p> : null}
      </div>
      {actions ? <div className={styles.actions}>{actions}</div> : null}
    </section>
  );
}
