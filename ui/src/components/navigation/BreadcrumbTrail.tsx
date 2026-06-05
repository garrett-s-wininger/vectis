import styles from "./BreadcrumbTrail.module.css";

export type BreadcrumbItem = {
  ariaLabel?: string;
  label: string;
  current?: boolean;
  onClick?: () => void;
};

type BreadcrumbTrailProps = {
  items: BreadcrumbItem[];
  label: string;
};

export function BreadcrumbTrail({ items, label }: BreadcrumbTrailProps) {
  return (
    <nav aria-label={label} className={styles.root}>
      {items.map((item) =>
        item.onClick ? (
          <button
            aria-label={item.ariaLabel}
            aria-current={item.current ? "page" : undefined}
            className={styles.crumb}
            key={item.label}
            onClick={item.onClick}
            type="button"
          >
            {item.label}
          </button>
        ) : (
          <span aria-current={item.current ? "page" : undefined} className={styles.crumb} key={item.label}>
            {item.label}
          </span>
        )
      )}
    </nav>
  );
}
