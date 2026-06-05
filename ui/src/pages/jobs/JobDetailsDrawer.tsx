import { Button } from "../../components";
import type { RunListItem } from "../../components";
import { StatusBadge } from "../../components";
import type { Job } from "../../domain/console";
import styles from "./JobsPage.module.css";
import { JobTriggers } from "./JobTriggers";
import { canTriggerRun, getJobPathDetail, jobConfigurationActionLabel } from "./JobPresentation";

export function JobDetailsDrawer({
  job,
  lastRun,
  onEdit,
  onOpen,
  onOpenLastRun,
  onTrigger
}: {
  job: Job;
  lastRun?: RunListItem;
  onEdit: () => void;
  onOpen: () => void;
  onOpenLastRun: () => void;
  onTrigger: () => void;
}) {
  return (
    <div className={styles.detailsDrawer}>
      <div className={styles.detailsRows}>
        <div className={`${styles.detailRow} ${styles.detailRowLatestRun}`}>
          <span>Latest Run</span>
          <div>
            {lastRun ? (
              <>
                <StatusBadge status={lastRun.status} />
                <small>
                  #{lastRun.runNumber} · {lastRun.duration}
                </small>
              </>
            ) : (
              <StatusBadge status="empty" />
            )}
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
            <Button aria-label={`Open ${job.name}`} onClick={onOpen} variant="quiet">
              Open
            </Button>
            <Button
              aria-label={`${jobConfigurationActionLabel} ${job.name}`}
              onClick={onEdit}
              variant="quiet"
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
              <Button aria-label={`Run ${job.name}`} onClick={onTrigger} variant="quiet">
                Run
              </Button>
            </div>
          ) : null}
        </div>
      </div>
    </div>
  );
}
