import { Button } from "../../components";
import type { RunListItem } from "../../components";
import { StatusBadge } from "../../components";
import type { Job } from "../../domain/console";
import { ResourceTitle, TableActions } from "../shared";
import styles from "./JobsPage.module.css";
import { canTriggerRun, getJobActionFacts, jobConfigurationActionLabel } from "./jobPresentation";

export function JobActionPanel({
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
    <section className={styles.actionPanel} aria-labelledby="job-action-title">
      <ResourceTitle
        className={styles.actionTitle}
        id="job-action-title"
        subtitle="Context and actions"
        title={job.name}
      />
      <dl className={styles.actionFacts}>
        {getJobActionFacts(job).map((fact) => (
          <div key={fact.label}>
            <dt>{fact.label}</dt>
            <dd title={fact.value}>{fact.value}</dd>
          </div>
        ))}
      </dl>
      {lastRun ? (
        <div className={styles.latestRunPanel}>
          <span>Latest run</span>
          <div>
            <StatusBadge status={lastRun.status} />
            <Button aria-label={`Open latest run for ${job.name}`} onClick={onOpenLastRun}>
              View
            </Button>
          </div>
        </div>
      ) : null}
      <TableActions className={styles.actionButtons}>
        {canTriggerRun(job) ? (
          <Button aria-label={`Trigger ${job.name}`} onClick={onTrigger}>
            Run
          </Button>
        ) : null}
        <Button aria-label={`${jobConfigurationActionLabel} ${job.name}`} onClick={onEdit}>
          {jobConfigurationActionLabel}
        </Button>
      </TableActions>
    </section>
  );
}
