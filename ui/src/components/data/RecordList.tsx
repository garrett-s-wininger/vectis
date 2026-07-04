import type { ReactNode } from "react";
import { useId } from "react";
import styles from "./RecordList.module.css";

export type RecordListRailTone = "neutral" | "info" | "success" | "warning" | "warningMuted" | "danger";

export type RecordListItem = {
  actions?: ReactNode;
  ariaLabel?: string;
  content: ReactNode;
  key: string;
  onSelect?: () => void;
  railTone?: RecordListRailTone;
};

type RecordListProps = {
  countLabel: string;
  description: string;
  emptyMessage: string;
  items: RecordListItem[];
  title: string;
};

type RecordListSummaryProps = {
  children: ReactNode;
};

type RecordListIdentityProps = {
  subtitle?: string;
  title: string;
};

type RecordListMetaProps = {
  children: ReactNode;
  featuredFirst?: boolean;
};

export function RecordList({ countLabel, description, emptyMessage, items, title }: RecordListProps) {
  const titleID = useId();

  return (
    <section className={`${styles.root} polished-panel polished-panel--accent-top`} aria-labelledby={titleID}>
      <div className={styles.header}>
        <div className={styles.headingGroup}>
          <h2 id={titleID}>{title}</h2>
          <p>{description}</p>
        </div>
        <span className={styles.count}>{countLabel}</span>
      </div>
      {items.length > 0 ? (
        <ul className={styles.items}>
          {items.map((item) => (
            <li className={`${styles.item} ${styles[railToneClass(item.railTone)]}`} key={item.key}>
              <RecordRowAction label={item.ariaLabel} onSelect={item.onSelect}>
                {item.content}
              </RecordRowAction>
              {item.actions ? <div className={styles.actions}>{item.actions}</div> : null}
            </li>
          ))}
        </ul>
      ) : (
        <p className={styles.empty}>{emptyMessage}</p>
      )}
    </section>
  );
}

export function RecordListSummary({ children }: RecordListSummaryProps) {
  return <div className={styles.summary}>{children}</div>;
}

export function RecordListIdentity({ subtitle, title }: RecordListIdentityProps) {
  return (
    <div className={styles.identity}>
      <strong>{title}</strong>
      {subtitle ? <span>{subtitle}</span> : null}
    </div>
  );
}

export function RecordListMeta({ children, featuredFirst }: RecordListMetaProps) {
  return <dl className={featuredFirst ? `${styles.meta} ${styles.featuredMeta}` : styles.meta}>{children}</dl>;
}

function RecordRowAction({
  children,
  label,
  onSelect
}: {
  children: ReactNode;
  label?: string;
  onSelect?: () => void;
}) {
  if (!onSelect) {
    return <div className={styles.rowAction}>{children}</div>;
  }

  return (
    <button aria-label={label} className={styles.rowAction} onClick={onSelect} type="button">
      {children}
    </button>
  );
}

function railToneClass(tone: RecordListRailTone = "neutral") {
  switch (tone) {
    case "info":
      return "railInfo";
    case "success":
      return "railSuccess";
    case "warning":
      return "railWarning";
    case "warningMuted":
      return "railWarningMuted";
    case "danger":
      return "railDanger";
    case "neutral":
      return "railNeutral";
  }
}
