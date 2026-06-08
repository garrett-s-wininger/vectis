import { useState } from "react";
import { DataTable, StatusBadge, type DataTableColumn, type RunListItem } from "../../components";
import type { Job } from "../../domain/console";
import { ResourceStatus } from "../shared";
import { JobActionPanel } from "./JobActionPanel";
import { JobDetailsDrawer } from "./JobDetailsDrawer";
import { JobIdentity } from "./JobIdentity";
import { getLatestRunForJob } from "./JobPresentation";
import { JobTriggers } from "./JobTriggers";
import styles from "./JobsPage.module.css";

type JobWorkspaceProps = {
  jobs: Job[];
  onEdit: (job: Job) => void;
  onOpen: (jobID: string) => void;
  onSelectRun: (runID: string) => void;
  onTrigger: (jobID: string) => void;
  runs: RunListItem[];
};

export function JobWorkspace({ jobs, onEdit, onOpen, onSelectRun, onTrigger, runs }: JobWorkspaceProps) {
  const [selectedJobID, setSelectedJobID] = useState("");
  const selectedJob = jobs.find((job) => job.id === selectedJobID);
  const selectedJobLastRun = selectedJob ? getLatestRunForJob(selectedJob, runs) : undefined;

  function toggleSelectedJob(jobID: string) {
    setSelectedJobID((currentJobID) => (currentJobID === jobID ? "" : jobID));
  }

  const columns: DataTableColumn<Job>[] = [
    {
      header: "Job",
      width: "34%",
      cell: (job) => (
        <JobIdentity job={job} onSelect={() => toggleSelectedJob(job.id)} selected={selectedJob?.id === job.id} />
      )
    },
    {
      header: "Triggers",
      hideOnMobile: true,
      width: "34%",
      cell: (job) => <JobTriggers job={job} />
    },
    {
      header: "Details",
      mobileOnly: true,
      cell: (job) =>
        selectedJob?.id === job.id ? (
          <JobDetailsDrawer
            job={job}
            lastRun={getLatestRunForJob(job, runs)}
            onEdit={() => onEdit(job)}
            onOpen={() => onOpen(job.id)}
            onOpenLastRun={() => {
              const lastRun = getLatestRunForJob(job, runs);
              if (lastRun) {
                onSelectRun(lastRun.id);
              }
            }}
            onTrigger={() => onTrigger(job.id)}
          />
        ) : null
    },
    {
      align: "end",
      header: "State",
      hideOnMobile: true,
      width: "96px",
      cell: (job) => (
        <ResourceStatus tone={job.status}>{job.status === "enabled" ? "Enabled" : "Paused"}</ResourceStatus>
      )
    },
    {
      align: "end",
      header: "Latest run",
      hideOnMobile: true,
      width: "96px",
      cell: (job) => {
        const lastRun = getLatestRunForJob(job, runs);

        return lastRun ? <StatusBadge status={lastRun.status} /> : <StatusBadge status="empty" />;
      }
    }
  ];

  return (
    <div className={selectedJob ? `${styles.workspace} ${styles.workspaceWithPanel}` : styles.workspace}>
      <DataTable
        columns={columns}
        emptyMessage="No jobs loaded."
        getRowKey={(job) => job.id}
        isRowSelected={(job) => selectedJob?.id === job.id}
        rows={jobs}
      />
      {selectedJob ? (
        <JobActionPanel
          job={selectedJob}
          lastRun={selectedJobLastRun}
          onEdit={() => onEdit(selectedJob)}
          onOpen={() => onOpen(selectedJob.id)}
          onOpenLastRun={() => {
            if (selectedJobLastRun) {
              onSelectRun(selectedJobLastRun.id);
            }
          }}
          onTrigger={() => onTrigger(selectedJob.id)}
        />
      ) : null}
    </div>
  );
}
