import type { FormEvent } from "react";
import { useMemo, useState } from "react";
import { Button } from "../components";
import { FilterBar } from "../components";
import { FormError } from "../components";
import { PageHeader } from "../components";
import { RunList, type RunListItem } from "../components";
import { SelectField } from "../components";
import { TextAreaField } from "../components";
import type { RunStatus } from "../components";
import { ResourceTitle } from "./shared";

type RunFilter = RunStatus | "all";
type SourceFilter = NonNullable<RunListItem["source"]> | "all";

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

const sourceLabels: Record<SourceFilter, string> = {
  all: "All",
  stored: "Stored jobs",
  ephemeral: "Ephemeral"
};

const sourceTitleLabels: Record<SourceFilter, string> = {
  all: "All",
  stored: "Stored job",
  ephemeral: "Ephemeral"
};

const sourceOptions = Object.entries(sourceLabels).map(([value, label]) => ({
  label,
  value
}));

const defaultDefinition = JSON.stringify(
  {
    id: "ad-hoc-check",
    root: {
      id: "root",
      uses: "builtins/shell",
      with: {
        command: "echo 'Hello from Vectis'"
      }
    }
  },
  null,
  2
);

type RunsPageProps = {
  namespacePath: string;
  onSelectRun: (runID: string) => void;
  onSubmitEphemeralRun: (definition: string) => void;
  runs: RunListItem[];
};

export function RunsPage({ namespacePath, onSelectRun, onSubmitEphemeralRun, runs }: RunsPageProps) {
  const [status, setStatus] = useState<RunFilter>("all");
  const [source, setSource] = useState<SourceFilter>("all");
  const [showRunOnce, setShowRunOnce] = useState(false);
  const [definition, setDefinition] = useState(defaultDefinition);
  const [definitionError, setDefinitionError] = useState("");
  const filteredRuns = useMemo(() => {
    return runs.filter((run) => {
      const sourceMatches = source === "all" || (run.source ?? "stored") === source;
      const statusMatches = status === "all" || run.status === status;
      return sourceMatches && statusMatches;
    });
  }, [runs, source, status]);

  function clearFilters() {
    setSource("all");
    setStatus("all");
  }

  function submitRunOnce(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setDefinitionError("");

    try {
      JSON.parse(definition);
    } catch {
      setDefinitionError("Definition must be valid JSON.");
      return;
    }

    onSubmitEphemeralRun(definition);
    setDefinition(defaultDefinition);
    setShowRunOnce(false);
    clearFilters();
  }

  return (
    <>
      <PageHeader
        description={`Recent queued, running, and completed work under ${namespacePath}.`}
        eyebrow="Runs"
        actions={
          <Button aria-expanded={showRunOnce} onClick={() => setShowRunOnce((value) => !value)}>
            {showRunOnce ? "Close" : "Run once"}
          </Button>
        }
        title="Runs"
      />
      {showRunOnce ? (
        <section className="run-once-panel" aria-labelledby="run-once-title">
          <div className="run-once-panel__header">
            <ResourceTitle id="run-once-title" subtitle={`Namespace ${namespacePath}`} title="Run once" />
          </div>
          <form className="run-once-form" onSubmit={submitRunOnce}>
            <TextAreaField
              label="Job definition JSON"
              name="definition"
              onChange={(event) => setDefinition(event.target.value)}
              required
              rows={10}
              value={definition}
              wide
            />
            <FormError message={definitionError} />
            <div className="run-once-form__actions">
              <Button type="submit">Submit run</Button>
            </div>
          </form>
        </section>
      ) : null}
      <FilterBar
        actions={
          <Button disabled={status === "all" && source === "all"} onClick={clearFilters}>
            Clear
          </Button>
        }
        filters={
          <>
            <SelectField
              label="Status"
              name="runStatus"
              onChange={(event) => setStatus(event.target.value as RunFilter)}
              options={statusOptions}
              value={status}
            />
            <SelectField
              label="Source"
              name="runSource"
              onChange={(event) => setSource(event.target.value as SourceFilter)}
              options={sourceOptions}
              value={source}
            />
          </>
        }
      />
      <RunList onSelectRun={onSelectRun} title={runListTitle(status, source)} runs={filteredRuns} />
    </>
  );
}

function runListTitle(status: RunFilter, source: SourceFilter) {
  if (status === "all" && source === "all") {
    return "All runs";
  }

  if (status === "all") {
    return `${sourceTitleLabels[source]} runs`;
  }

  if (source === "all") {
    return `${statusLabels[status]} runs`;
  }

  return `${sourceTitleLabels[source]} ${statusLabels[status].toLowerCase()} runs`;
}
