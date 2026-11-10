import { definitionDiff } from "./DefinitionDiffModel";
import styles from "./DefinitionDiff.module.css";

export function DefinitionDiff({
  before,
  after,
  beforeLabel,
  afterLabel,
  fill = false
}: {
  before: string;
  after: string;
  beforeLabel: string;
  afterLabel: string;
  fill?: boolean;
}) {
  const lines = definitionDiff(before, after);
  const added = lines.filter((line) => line.kind === "added").length;
  const removed = lines.filter((line) => line.kind === "removed").length;
  return (
    <div className={`${styles.root} ${fill ? styles.fill : ""}`}>
      <div className={styles.legend}>
        <span>
          {beforeLabel} → {afterLabel}
        </span>
        <span>
          {added} added · {removed} removed
        </span>
      </div>
      {!added && !removed ? (
        <p className={styles.empty}>No differences between these definitions.</p>
      ) : (
        <pre className={styles.diff} tabIndex={0} aria-label={`${beforeLabel} to ${afterLabel} diff`}>
          {lines.map((line, index) => (
            <span className={`${styles.line} ${styles[line.kind]}`} key={index}>
              <span className={styles.number} aria-hidden="true">
                {line.before ?? ""}
              </span>
              <span className={styles.number} aria-hidden="true">
                {line.after ?? ""}
              </span>
              <span className={styles.marker}>{line.kind === "added" ? "+" : line.kind === "removed" ? "−" : " "}</span>
              <span className={styles.code}>{line.text || " "}</span>
            </span>
          ))}
        </pre>
      )}
    </div>
  );
}
