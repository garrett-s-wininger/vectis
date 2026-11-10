import type { ReactNode } from "react";
import { useConsoleAccess } from "../../domain/access";
import {
  BreadcrumbTrail,
  Button,
  CopyValue,
  OperationalFact,
  PageHeader,
  ResourceStatus,
  ResourceLink,
  StatusBadge,
  type RunListItem
} from "../../components";
import { repositoryPath } from "../../routing/repositories";
import { FolderGit2, GitBranch, GitCommitHorizontal, FileCode, ArrowRight, Play } from "lucide-react";
import type { Job } from "../../domain/console";
import { SourceJobDefinition } from "./SourceJobDefinition";
import type { SourceHotfixInput, SourceJobChange } from "../../data/sourceJobs";
import { JobTriggers } from "./JobTriggers";
import { jobDetailBreadcrumbItems } from "./JobBreadcrumbs";
import {
  canTriggerRun,
  getJobDescription,
  getJobPathDetail,
  getScheduleDetail,
  jobConfigurationActionLabel
} from "./JobPresentation";
import styles from "./JobDetailPage.module.css";
import { formatDefinitionJSON } from "./DefinitionJSON";

type JobDetailPageProps = {
  job: Job;
  sharedWorkspace?: boolean;
  runs: RunListItem[];
  onBack: () => void;
  onConfigure: () => void;
  onOpenRun: (runID: string) => void;
  onOpenRuns: () => void;
  onTrigger: () => void;
  onRunWithChanges?: (input: SourceHotfixInput) => Promise<void> | void;
  onSourceJobChanged?: (change: SourceJobChange) => void;
};

export function JobDetailPage({
  job,
  sharedWorkspace = false,
  runs,
  onBack,
  onConfigure,
  onOpenRun,
  onOpenRuns,
  onTrigger,
  onRunWithChanges,
  onSourceJobChanged
}: JobDetailPageProps) {
  const { can } = useConsoleAccess();

  function renderWorkspace(definition: ReactNode, provenance?: ReactNode) {
    return (
      <div className={styles.workspaceLayout}>
        <div className={styles.definitionColumn}>{definition}</div>
        <aside className={styles.contextColumn} aria-label="Job context">
          {provenance ? (
            <section className={`${styles.panel} polished-panel`} aria-labelledby="job-provenance-title">
              <h3 className={styles.contextTitle} id="job-provenance-title">
                Provenance
              </h3>
              {provenance}
            </section>
          ) : null}
          <section className={`${styles.panel} polished-panel`} aria-labelledby="job-execution-title">
            <div className={styles.activityHeader}>
              <h3 id="job-execution-title">Recent runs</h3>
              <Button
                className={styles.iconAction}
                aria-label={`View all runs for ${job.name}`}
                onClick={onOpenRuns}
                variant="quiet"
              >
                All <ArrowRight aria-hidden="true" />
              </Button>
            </div>
            {runs.length ? (
              <ol className={styles.recentRuns} aria-label="Recent runs">
                {runs.slice(0, 5).map((run, index) => {
                  const timestamp = run.startedAt || run.createdAt;
                  const date = timestamp ? new Date(timestamp) : undefined;
                  const validDate = date && !Number.isNaN(date.getTime());
                  return (
                    <li key={run.id}>
                      <Button
                        className={styles.runLink}
                        variant="quiet"
                        aria-label={
                          index === 0 ? `View latest run for ${job.name}` : `View run #${run.runNumber} for ${job.name}`
                        }
                        onClick={() => onOpenRun(run.id)}
                      >
                        <span className={styles.runHeading}>
                          <strong>#{run.runNumber}</strong>
                          <StatusBadge status={run.status} />
                          <ArrowRight aria-hidden="true" />
                        </span>
                        <span className={styles.runMetadata}>
                          {validDate ? (
                            <time dateTime={date.toISOString()} title={date.toLocaleString()}>
                              {date.toLocaleString(undefined, {
                                month: "short",
                                day: "numeric",
                                hour: "numeric",
                                minute: "2-digit"
                              })}
                            </time>
                          ) : null}
                          <span className={styles.runDuration}>{run.duration}</span>
                        </span>
                      </Button>
                    </li>
                  );
                })}
              </ol>
            ) : (
              <p className={styles.summaryDetail}>No run history yet.</p>
            )}
          </section>
          <section className={`${styles.panel} polished-panel`} aria-labelledby="job-triggers-title">
            <div className={styles.triggerSection}>
              <div className={styles.triggerHeading}>
                <h3 id="job-triggers-title">Triggers</h3>
                <JobTriggers job={job} />
              </div>
              {job.triggers.some((trigger) => trigger.kind === "schedule") ? (
                <p className={styles.summaryDetail}>{getScheduleDetail(job)}</p>
              ) : null}
            </div>
          </section>
        </aside>
      </div>
    );
  }

  return (
    <section className={styles.detailPage} aria-labelledby="job-detail-title">
      <PageHeader
        navigation={
          <BreadcrumbTrail
            items={jobDetailBreadcrumbItems({
              jobName: job.name,
              namespacePath: job.namespacePath,
              onJobs: onBack,
              hideNamespace: sharedWorkspace
            })}
            label="Job location"
          />
        }
        description={getJobDescription(job)}
        title={job.name}
        titleID="job-detail-title"
        actions={
          <div className={styles.pageActions}>
            <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>
            {can("run:trigger", job.namespacePath) && canTriggerRun(job) ? (
              <Button className={styles.iconAction} aria-label={`Run ${job.name}`} onClick={onTrigger}>
                <Play aria-hidden="true" /> Run Now
              </Button>
            ) : null}
          </div>
        }
      />

      {job.repository ? (
        <SourceJobDefinition
          key={`${job.repository}:${job.id}`}
          job={job}
          renderLayout={renderWorkspace}
          sourceDetails={(ownership) => (
            <div className={styles.sourceContext}>
              <dl aria-label="Repository source" className={styles.sourceFacts}>
                <OperationalFact
                  icon={FolderGit2}
                  label="Repository"
                  layout="detail"
                  wrap
                  value={
                    <ResourceLink href={repositoryPath(job.repository, job.namespacePath)}>
                      {job.repository}
                    </ResourceLink>
                  }
                />
                <OperationalFact icon={GitBranch} label="Ref" value={job.branch || "HEAD"} layout="detail" wrap />
                <OperationalFact icon={FileCode} label="Definition" value={job.sourceDetail} layout="detail" wrap />
                {ownership}
                {job.sourceCommit ? (
                  <OperationalFact
                    className={styles.sourceCommit}
                    icon={GitCommitHorizontal}
                    label="Commit"
                    layout="detail"
                    wrap
                    value={
                      <CopyValue appearance="field" label="Commit" value={job.sourceCommit}>
                        <code title={job.sourceCommit}>{job.sourceCommit.slice(0, 12)}</code>
                      </CopyValue>
                    }
                  />
                ) : null}
              </dl>
              {job.id !== job.name ? (
                <p className={styles.jobIdentifier}>
                  Job ID <code>{job.id}</code>
                </p>
              ) : null}
            </div>
          )}
          onConfigure={onConfigure}
          onRun={onRunWithChanges}
          onChanged={onSourceJobChanged}
        />
      ) : (
        renderWorkspace(
          <section
            className={`${styles.panel} polished-panel polished-panel--accent-top`}
            aria-labelledby="job-definition-title"
          >
            <div className={styles.definitionHeader}>
              <div className={styles.panelHeader}>
                <h3 id="job-definition-title">Definition</h3>
                <p>{getJobPathDetail(job)}</p>
              </div>
              {can("admin:*") && can("job:write", job.namespacePath) ? (
                <Button aria-label={`${jobConfigurationActionLabel} ${job.name}`} onClick={onConfigure} variant="quiet">
                  {jobConfigurationActionLabel}
                </Button>
              ) : null}
            </div>
            <pre className="definition-preview">{formatDefinition(job.definition)}</pre>
          </section>
        )
      )}
    </section>
  );
}

function formatDefinition(definition?: string) {
  if (!definition) {
    return "Definition body is not loaded for this source.";
  }

  return formatDefinitionJSON(definition);
}
