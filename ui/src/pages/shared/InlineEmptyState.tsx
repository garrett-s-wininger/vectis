import styles from "./InlineEmptyState.module.css";

type InlineEmptyStateProps = {
  children: string;
};

export function InlineEmptyState({ children }: InlineEmptyStateProps) {
  return <div className={styles.root}>{children}</div>;
}
