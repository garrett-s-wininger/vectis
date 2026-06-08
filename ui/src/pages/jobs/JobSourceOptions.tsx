import styles from "./JobSourceOptions.module.css";

const sourceOptions = [
  {
    description:
      "Store the job definition in Vectis. Best for local workflows, quick jobs, and definitions managed through the API.",
    label: "Inline",
    status: "Available"
  },
  {
    description:
      "Use a repository path as the source of truth. Vectis will read definitions from the configured repo browser once enabled.",
    label: "Source Control",
    status: "Coming soon"
  }
];

export function JobSourceOptions() {
  return (
    <div className={styles.sourceChoices} role="list" aria-label="Definition source options">
      {sourceOptions.map((option, index) => (
        <div
          aria-disabled={index === 0 ? undefined : true}
          className={index === 0 ? `${styles.sourceChoice} ${styles.sourceChoiceSelected}` : styles.sourceChoice}
          key={option.label}
          role="listitem"
        >
          <div>
            <strong>{option.label}</strong>
            <small>{option.description}</small>
          </div>
          <span>{option.status}</span>
        </div>
      ))}
    </div>
  );
}
