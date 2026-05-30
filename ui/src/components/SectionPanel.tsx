import type { ReactNode } from "react";
import styles from "./SectionPanel.module.css";

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
    <section className={styles.root} aria-labelledby="section-panel-title">
      <div className={styles.header}>
        <div className={styles.copy}>
          <h2 className={styles.title} id="section-panel-title">{title}</h2>
          {description ? (
            <p className={styles.description}>{description}</p>
          ) : null}
        </div>
        {actions ? <div className={styles.actions}>{actions}</div> : null}
      </div>
      <div className={styles.body}>{children}</div>
    </section>
  );
}
