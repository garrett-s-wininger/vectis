import { useMemo, useState } from "react";
import { Button } from "../components/Button";
import { FilterBar } from "../components/FilterBar";
import { PageHeader } from "../components/PageHeader";
import { RunList, type RunListItem } from "../components/RunList";
import { SelectField } from "../components/SelectField";
import type { RunStatus } from "../components/StatusBadge";

type RunFilter = RunStatus | "all";

const statusLabels: Record<RunFilter, string> = {
  all: "All",
  queued: "Queued",
  running: "Running",
  succeeded: "Succeeded",
  failed: "Failed",
  cancelled: "Cancelled",
  abandoned: "Abandoned"
};

const statusOptions = Object.entries(statusLabels).map(([value, label]) => ({
  label,
  value
}));

type RunsPageProps = {
  namespacePath: string;
  runs: RunListItem[];
};

export function RunsPage({ namespacePath, runs }: RunsPageProps) {
  const [status, setStatus] = useState<RunFilter>("all");
  const filteredRuns = useMemo(() => {
    if (status === "all") {
      return runs;
    }

    return runs.filter((run) => run.status === status);
  }, [runs, status]);

  return (
    <>
      <PageHeader
        description={`Recent queued, running, and completed work under ${namespacePath}.`}
        eyebrow="Runs"
        title="Runs"
      />
      <FilterBar
        actions={
          <Button disabled={status === "all"} onClick={() => setStatus("all")}>
            Clear
          </Button>
        }
        filters={
          <SelectField
            label="Status"
            name="runStatus"
            onChange={(event) => setStatus(event.target.value as RunFilter)}
            options={statusOptions}
            value={status}
          />
        }
      />
      <RunList
        title={status === "all" ? "All runs" : `${statusLabels[status]} runs`}
        runs={filteredRuns}
      />
    </>
  );
}
