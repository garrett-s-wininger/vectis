import { BreadcrumbTrail, Button, PageHeader, StatusBadge, type RunListItem } from "../../components";
import type { Job } from "../../domain/console";
import { ResourceStatus } from "../shared";
import { JobTriggers } from "./JobTriggers";
import { jobDetailBreadcrumbItems } from "./JobBreadcrumbs";
import {
  canTriggerRun,
  getJobDescription,
  getJobPathDetail,
  getManualTriggerDetail,
  getRunHealthSummary,
  getScheduleDetail,
  jobConfigurationActionLabel
} from "./JobPresentation";
import styles from "./JobDetailPage.module.css";

type JobDetailPageProps = {
  job: Job;
  lastRun?: RunListItem;
  runs: RunListItem[];
  onBack: () => void;
  onConfigure: () => void;
  onOpenLastRun: () => void;
  onOpenRuns: () => void;
  onTrigger: () => void;
};

export function JobDetailPage({
  job,
  lastRun,
  runs,
  onBack,
  onConfigure,
  onOpenLastRun,
  onOpenRuns,
  onTrigger
}: JobDetailPageProps) {
  const runHealth = getRunHealthSummary(runs);

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
      />

      <div className={styles.summaryGrid}>
        <section className={`${styles.panel} polished-panel`} aria-labelledby="job-execution-title">
          <div className={styles.panelHeader}>
            <h3 id="job-execution-title">Execution</h3>
            <p>{runHealth.detail}</p>
          </div>
          <div className={styles.executionSummary}>
            <div className={styles.healthSummary}>
              <StatusBadge status={runHealth.tone} />
              <Button aria-label={`Open all runs for ${job.name}`} onClick={onOpenRuns}>
                All Runs
              </Button>
            </div>
            {lastRun ? (
              <div className={styles.latestRun}>
                <span>
                  Latest #{lastRun.runNumber} · {lastRun.duration}
                </span>
                <Button aria-label={`Open latest run for ${job.name}`} onClick={onOpenLastRun} variant="quiet">
                  Latest Run
                </Button>
              </div>
            ) : null}
          </div>
        </section>

        <section
          className={`${styles.panel} ${styles.commandPanel} polished-panel`}
          aria-labelledby="job-readiness-title"
        >
          <div className={styles.panelHeader}>
            <h3 id="job-readiness-title">Run Readiness</h3>
            <p>{getManualTriggerDetail(job)}</p>
          </div>
          <div className={styles.commandStack}>
            <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>
            {canTriggerRun(job) ? (
              <Button aria-label={`Run ${job.name}`} onClick={onTrigger} variant="quiet">
                Run Now
              </Button>
            ) : null}
          </div>
        </section>

        <section className={`${styles.panel} polished-panel`} aria-labelledby="job-triggers-title">
          <div className={styles.panelHeader}>
            <h3 id="job-triggers-title">Triggers</h3>
            <p>{getScheduleDetail(job)}</p>
          </div>
          <JobTriggers job={job} />
        </section>
      </div>

      <section
        className={`${styles.panel} polished-panel polished-panel--accent-top`}
        aria-labelledby="job-definition-title"
      >
        <div className={styles.definitionHeader}>
          <div className={styles.panelHeader}>
            <h3 id="job-definition-title">Definition</h3>
            <p>{getJobPathDetail(job)}</p>
          </div>
          <Button aria-label={`${jobConfigurationActionLabel} ${job.name}`} onClick={onConfigure} variant="quiet">
            {jobConfigurationActionLabel}
          </Button>
        </div>
        <pre className="definition-preview">{formatDefinition(job.definition)}</pre>
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
