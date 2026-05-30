import type { ReactNode } from "react";
import styles from "./AppState.module.css";

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
  const className = tone === "neutral" || tone === "loading"
    ? styles.root
    : `${styles.root} ${styles[tone]}`;

  return (
    <section className={className} aria-label={title}>
      <div className={styles.copy}>
        <h2>{title}</h2>
        {description ? <p>{description}</p> : null}
      </div>
      {actions ? <div className={styles.actions}>{actions}</div> : null}
    </section>
  );
}
