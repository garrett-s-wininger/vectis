import styles from "./JobSourceOptions.module.css";

const sourceOptions = [
  {
    description: "Best for quick setup or local definitions. Vectis stores the JSON and owns changes from this screen.",
    label: "Inline",
    status: "Available"
  },
  {
    description: "Best for reviewed definitions. Vectis reads job files from a repository; edits happen through code review.",
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
