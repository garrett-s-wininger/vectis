import type { Job } from "../../domain/console";
import styles from "./JobsPage.module.css";
import { triggerKindLabel } from "./jobPresentation";

export function JobTriggers({ job }: { job: Job }) {
  const visibleTriggers = job.triggers.length > 2 ? job.triggers.slice(0, 1) : job.triggers;
  const hiddenTriggerCount = job.triggers.length - visibleTriggers.length;

  return (
    <div className={styles.triggerList} aria-label={`${job.name} triggers`}>
      {visibleTriggers.map((trigger) => (
        <span className={styles.triggerItem} key={`${job.id}-${trigger.kind}`} title={trigger.detail}>
          {triggerKindLabel[trigger.kind]}
        </span>
      ))}
      {hiddenTriggerCount > 0 ? (
        <span
          className={styles.triggerItem}
          title={job.triggers
            .slice(visibleTriggers.length)
            .map((trigger) => trigger.detail)
            .join(", ")}
        >
          +{hiddenTriggerCount}
        </span>
      ) : null}
    </div>
  );
}
