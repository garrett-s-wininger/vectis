import { DataTable, StatusBadge, type DataTableColumn, type RunListItem } from "../../components";
import type { Job } from "../../domain/console";
import { ResourceStatus } from "../shared";
import { JobIdentity } from "./JobIdentity";
import { getLatestRunForJob } from "./JobPresentation";
import { JobTriggers } from "./JobTriggers";
import styles from "./JobsPage.module.css";

type JobListProps = {
  jobs: Job[];
  onOpen: (jobID: string) => void;
  runs: RunListItem[];
};

export function JobList({ jobs, onOpen, runs }: JobListProps) {
  const columns: DataTableColumn<Job>[] = [
    {
      header: "Job",
      width: "34%",
      cell: (job) => <JobIdentity job={job} />
    },
    {
      header: "Triggers",
      hideOnMobile: true,
      width: "34%",
      cell: (job) => <JobTriggers job={job} />
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
    <div className={styles.workspace}>
      <DataTable
        columns={columns}
        emptyMessage="No jobs loaded."
        getRowActionLabel={(job) => `View ${job.name}`}
        getRowKey={(job) => job.id}
        onRowClick={(job) => onOpen(job.id)}
        rows={jobs}
      />
    </div>
  );
}
