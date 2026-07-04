import styles from "./JobSourceOptions.module.css";

const sourceOptions = [
  {
    description: "Store JSON directly in Vectis for quick jobs and API-managed workflows.",
    label: "Inline",
    status: "Selected"
  },
  {
    description: "Point to a reviewed repo path once the repo browser is configured.",
    label: "Source Control",
    status: "Planned"
  }
];

export function JobSourceOptions() {
  return (
    <div className={styles.sourceChoices} role="list" aria-label="Source options">
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
