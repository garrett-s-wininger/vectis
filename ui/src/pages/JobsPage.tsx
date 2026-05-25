import { Button } from "../components/Button";
import { DataTable, type DataTableColumn } from "../components/DataTable";
import { PageHeader } from "../components/PageHeader";
import { StatusBadge } from "../components/StatusBadge";
import type { MockJob } from "../mocks/consoleData";

type JobsPageProps = {
  jobs: MockJob[];
  onTriggerRun: (jobID: string) => void;
};

export function JobsPage({ jobs, onTriggerRun }: JobsPageProps) {
  const columns: DataTableColumn<MockJob>[] = [
    {
      header: "Job",
      cell: (job) => (
        <div className="resource-title">
          <strong>{job.name}</strong>
          <small>{job.repository}</small>
        </div>
      )
    },
    {
      header: "Branch",
      cell: (job) => job.branch
    },
    {
      header: "Schedule",
      cell: (job) => (
        <div className="resource-title">
          <strong>{job.schedule}</strong>
          <small>{job.nextRun}</small>
        </div>
      )
    },
    {
      header: "Last run",
      cell: (job) => <StatusBadge status={job.lastRunStatus} />
    },
    {
      align: "end",
      header: "State",
      cell: (job) => (
        <span className={`resource-status resource-status--${job.status}`}>
          {job.status === "enabled" ? "Enabled" : "Paused"}
        </span>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (job) => (
        <Button
          aria-label={`Trigger ${job.name}`}
          disabled={job.status === "paused"}
          onClick={() => onTriggerRun(job.id)}
        >
          Trigger run
        </Button>
      )
    }
  ];

  return (
    <>
      <PageHeader
        description="Configured job definitions for this instance."
        eyebrow="Jobs"
        title="Jobs"
      />
      <DataTable
        columns={columns}
        emptyMessage="No jobs loaded."
        getRowKey={(job) => job.id}
        rows={jobs}
      />
    </>
  );
}
