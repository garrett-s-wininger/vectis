import type { ReactNode } from "react";
import styles from "./PageHeader.module.css";

type PageHeaderProps = {
  eyebrow?: string;
  navigation?: ReactNode;
  titleID?: string;
  title: string;
  description?: string;
  actions?: ReactNode;
};

export function PageHeader({ eyebrow, navigation, titleID, title, description, actions }: PageHeaderProps) {
  return (
    <header className={styles.root}>
      <div className={styles.copy}>
        {navigation ? <div className={styles.navigation}>{navigation}</div> : null}
        {eyebrow ? <p className={styles.eyebrow}>{eyebrow}</p> : null}
        <h1 className={styles.title} id={titleID}>
          {title}
        </h1>
        {description ? <p className={styles.description}>{description}</p> : null}
      </div>
      {actions ? <div className={styles.actions}>{actions}</div> : null}
    </header>
  );
}
