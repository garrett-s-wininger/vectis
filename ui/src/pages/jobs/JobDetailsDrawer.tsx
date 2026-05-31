import { Button } from "../../components";
import type { RunListItem } from "../../components";
import { StatusBadge } from "../../components";
import type { Job } from "../../domain/console";
import styles from "./JobsPage.module.css";
import { JobTriggers } from "./JobTriggers";
import { canTriggerRun, getJobPathDetail, jobConfigurationActionLabel } from "./jobPresentation";

export function JobDetailsDrawer({
  job,
  lastRun,
  onEdit,
  onOpenLastRun,
  onTrigger
}: {
  job: Job;
  lastRun?: RunListItem;
  onEdit: () => void;
  onOpenLastRun: () => void;
  onTrigger: () => void;
}) {
  return (
    <div className={styles.detailsDrawer}>
      <div className={styles.detailsRows}>
        <div className={`${styles.detailRow} ${styles.detailRowLatestRun}`}>
          <span>Latest run</span>
          <div>
            <StatusBadge status={job.lastRunStatus} />
            {lastRun ? (
              <small>
                #{lastRun.runNumber} · {lastRun.duration}
              </small>
            ) : null}
          </div>
          <div>
            {lastRun ? (
              <Button aria-label={`Open latest run for ${job.name}`} onClick={onOpenLastRun}>
                View
              </Button>
            ) : null}
          </div>
        </div>
        <div className={styles.detailRow}>
          <span>Path</span>
          <div>
            <small>{getJobPathDetail(job)}</small>
          </div>
          <div>
            <Button
              className={styles.detailButtonQuiet}
              aria-label={`${jobConfigurationActionLabel} ${job.name}`}
              onClick={onEdit}
            >
              {jobConfigurationActionLabel}
            </Button>
          </div>
        </div>
        <div className={canTriggerRun(job) ? styles.detailRow : `${styles.detailRow} ${styles.detailRowReadOnly}`}>
          <span>Triggers</span>
          <div>
            <JobTriggers job={job} />
          </div>
          {canTriggerRun(job) ? (
            <div>
              <Button className={styles.detailButtonQuiet} aria-label={`Run ${job.name}`} onClick={onTrigger}>
                Run
              </Button>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
