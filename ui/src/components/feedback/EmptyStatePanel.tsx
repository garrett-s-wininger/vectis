import type { ReactNode } from "react";
import styles from "./EmptyStatePanel.module.css";

type EmptyStatePanelProps = {
  actions?: ReactNode;
  description: string;
  eyebrow?: string;
  title: string;
  titleID?: string;
};

export function EmptyStatePanel({ actions, description, eyebrow, title, titleID }: EmptyStatePanelProps) {
  return (
    <section
      className={`${styles.root} polished-panel polished-panel--accent-top`}
      aria-labelledby={titleID}
      aria-label={titleID ? undefined : title}
    >
      <div>
        {eyebrow ? <p className="eyebrow">{eyebrow}</p> : null}
        <h2 id={titleID}>{title}</h2>
        <p>{description}</p>
      </div>
      {actions ? <div className={styles.actions}>{actions}</div> : null}
    </section>
  );
}
