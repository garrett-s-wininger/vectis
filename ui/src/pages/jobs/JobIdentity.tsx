import type { Job } from "../../domain/console";
import { ResourceStatus } from "../shared";
import styles from "./JobsPage.module.css";
import { getJobSourceDetail } from "./JobPresentation";

export function JobIdentity({ job, onSelect, selected }: { job: Job; onSelect: () => void; selected: boolean }) {
  return (
    <button aria-pressed={selected} className={styles.identity} onClick={onSelect} type="button">
      <div className={styles.identityTitle}>
        <strong>{job.name}</strong>
      </div>
      <small>{getJobSourceDetail(job)}</small>
      <span className={styles.identityStatus}>
        <ResourceStatus className={styles.identityStatusPill} tone={job.status}>
          {job.status === "enabled" ? "Enabled" : "Paused"}
        </ResourceStatus>
      </span>
      <span className={styles.identityChevron} aria-hidden="true" />
    </button>
  );
}
