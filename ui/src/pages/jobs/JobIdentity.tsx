import type { Job } from "../../domain/console";
import { ResourceStatus } from "../shared";
import styles from "./JobsPage.module.css";
import { getJobSourceDetail } from "./JobPresentation";

export function JobIdentity({ job }: { job: Job }) {
  return (
    <div className={styles.identity}>
      <div className={styles.identityTitle}>
        <strong>{job.name}</strong>
      </div>
      <small>{getJobSourceDetail(job)}</small>
      <span className={styles.identityStatus}>
        <ResourceStatus className={styles.identityStatusPill} tone={job.status}>
          {job.status === "enabled" ? "Enabled" : "Paused"}
        </ResourceStatus>
      </span>
    </div>
  );
}
