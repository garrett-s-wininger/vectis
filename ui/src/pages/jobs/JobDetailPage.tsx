import { BreadcrumbTrail, Button, PageHeader, StatusBadge, type RunListItem } from "../../components";
import type { Job } from "../../domain/console";
import { ResourceStatus } from "../shared";
import { JobTriggers } from "./JobTriggers";
import { jobDetailBreadcrumbItems } from "./JobBreadcrumbs";
import {
  canTriggerRun,
  getJobDescription,
  getJobActionFacts,
  getJobPathDetail,
  getJobSourceDetail,
  jobConfigurationActionLabel
} from "./JobPresentation";
import styles from "./JobDetailPage.module.css";

type JobDetailPageProps = {
  job: Job;
  lastRun?: RunListItem;
  onBack: () => void;
  onConfigure: () => void;
  onOpenLastRun: () => void;
  onTrigger: () => void;
};

export function JobDetailPage({
  job,
  lastRun,
  onBack,
  onConfigure,
  onOpenLastRun,
  onTrigger
}: JobDetailPageProps) {
  return (
    <section className={styles.detailPage} aria-labelledby="job-detail-title">
      <PageHeader
        navigation={
          <BreadcrumbTrail
            items={jobDetailBreadcrumbItems({ jobName: job.name, namespacePath: job.namespacePath, onJobs: onBack })}
            label="Job location"
          />
        }
        description={getJobDescription(job)}
        title={job.name}
        titleID="job-detail-title"
        actions={
          <>
            <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>
            {canTriggerRun(job) ? (
              <Button aria-label={`Run ${job.name}`} onClick={onTrigger}>
                Run
              </Button>
            ) : null}
            <Button aria-label={`${jobConfigurationActionLabel} ${job.name}`} onClick={onConfigure}>
              {jobConfigurationActionLabel}
            </Button>
          </>
        }
      />

      <div className={styles.summaryGrid}>
        <section className={styles.panel} aria-labelledby="job-source-title">
          <div className={styles.panelHeader}>
            <h3 id="job-source-title">Source</h3>
            <p>{getJobSourceDetail(job)}</p>
          </div>
          <dl className={styles.factList}>
            {getJobActionFacts(job).map((fact) => (
              <div key={fact.label}>
                <dt>{fact.label}</dt>
                <dd>{fact.value}</dd>
              </div>
            ))}
          </dl>
        </section>

        <section className={styles.panel} aria-labelledby="job-triggers-title">
          <div className={styles.panelHeader}>
            <h3 id="job-triggers-title">Triggers</h3>
            <p>Configured ways this definition can enter the queue.</p>
          </div>
          <JobTriggers job={job} />
        </section>

        <section className={styles.panel} aria-labelledby="job-latest-run-title">
          <div className={styles.panelHeader}>
            <h3 id="job-latest-run-title">Latest Run</h3>
            <p>{lastRun ? `Run #${lastRun.runNumber}` : "No runs have been created for this job."}</p>
          </div>
          {lastRun ? (
            <div className={styles.latestRun}>
              <StatusBadge status={lastRun.status} />
              <span>{lastRun.duration}</span>
              <Button aria-label={`Open latest run for ${job.name}`} onClick={onOpenLastRun}>
                View
              </Button>
            </div>
          ) : (
            <StatusBadge status="empty" />
          )}
        </section>
      </div>

      <section className={`${styles.panel} ${styles.definitionPanel}`} aria-labelledby="job-definition-title">
        <div className={styles.panelHeader}>
          <h3 id="job-definition-title">Definition</h3>
          <p>{getJobPathDetail(job)}</p>
        </div>
        <pre className={styles.definitionPreview}>{formatDefinition(job.definition)}</pre>
      </section>
    </section>
  );
}

function formatDefinition(definition?: string) {
  if (!definition) {
    return "Definition body is not loaded for this source.";
  }

  try {
    return JSON.stringify(JSON.parse(definition) as unknown, null, 2);
  } catch {
    return definition;
  }
}
