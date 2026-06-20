import type { FormEvent } from "react";
import { useMemo, useState } from "react";
import { BreadcrumbTrail } from "../components";
import { Button } from "../components";
import { FilterBar } from "../components";
import { FormField } from "../components";
import { NamespacePicker } from "../components";
import { PageHeader } from "../components";
import { RunList, type RunListItem } from "../components";
import { SelectField } from "../components";
import { TextAreaField } from "../components";
import type { Namespace } from "../domain/console";
import {
  defaultRunDefinition,
  runSourceOptions,
  runSourceTitleLabels,
  runStatusLabels,
  runStatusOptions,
  type RunFilter,
  type SourceFilter
} from "../domain/consoleOptions";
import { navigateTo } from "../routing/routes";
import { jsonObject } from "../validation/FormValidation";
import { formatNamespaceCrumb } from "./jobs/JobBreadcrumbs";
import { ResourceTitle } from "./shared";

type RunsPageProps = {
  jobName?: string;
  namespaces: Namespace[];
  namespacePath: string;
  onSelectNamespace: (namespacePath: string) => void;
  onSelectRun: (runID: string) => void;
  onSubmitEphemeralRun: (definition: string) => void;
  runs: RunListItem[];
};

export function RunsPage({
  jobName,
  namespaces,
  namespacePath,
  onSelectNamespace,
  onSelectRun,
  onSubmitEphemeralRun,
  runs
}: RunsPageProps) {
  const jobFilter = jobName?.trim() ?? "";
  const [status, setStatus] = useState<RunFilter>("all");
  const [source, setSource] = useState<SourceFilter>("all");
  const [showRunOnce, setShowRunOnce] = useState(false);
  const [definition, setDefinition] = useState(defaultRunDefinition);
  const [definitionError, setDefinitionError] = useState("");
  const filteredRuns = useMemo(() => {
    const normalizedJobFilter = jobFilter.toLowerCase();

    return runs.filter((run) => {
      const jobMatches = !normalizedJobFilter || run.jobName.toLowerCase().includes(normalizedJobFilter);
      const sourceMatches = source === "all" || (run.source ?? "stored") === source;
      const statusMatches = status === "all" || run.status === status;
      return jobMatches && sourceMatches && statusMatches;
    });
  }, [runs, jobFilter, source, status]);

  function clearFilters() {
    setSource("all");
    setStatus("all");
    if (jobFilter) {
      navigateTo("/runs");
    }
  }

  function updateJobFilter(value: string) {
    const trimmedValue = value.trim();
    navigateTo(trimmedValue ? `/runs?job=${encodeURIComponent(trimmedValue)}` : "/runs");
  }

  function submitRunOnce(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setDefinitionError("");

    const definitionResult = jsonObject(definition);
    if (!definitionResult.valid) {
      setDefinitionError(definitionResult.message);
      return;
    }

    onSubmitEphemeralRun(definition);
    setDefinition(defaultRunDefinition);
    setShowRunOnce(false);
    clearFilters();
  }

  return (
    <>
      <PageHeader
        description="Recent queued, running, and completed work."
        navigation={
          <BreadcrumbTrail
            items={[{ label: formatNamespaceCrumb(namespacePath) }, { label: "Runs", current: true }]}
            label="Runs location"
          />
        }
        actions={
          <>
            <NamespacePicker compact namespaces={namespaces} onChange={onSelectNamespace} value={namespacePath} />
            <Button aria-expanded={showRunOnce} onClick={() => setShowRunOnce((value) => !value)}>
              {showRunOnce ? "Close" : "Run Once"}
            </Button>
          </>
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
              error={definitionError}
              label="Job definition JSON"
              name="definition"
              onChange={(event) => {
                setDefinitionError("");
                setDefinition(event.target.value);
              }}
              required
              rows={10}
              value={definition}
              wide
            />
            <div className="run-once-form__actions">
              <Button type="submit">Submit run</Button>
            </div>
          </form>
        </section>
      ) : null}
      <FilterBar
        actions={
          <Button disabled={status === "all" && source === "all" && !jobFilter} onClick={clearFilters}>
            Clear
          </Button>
        }
        filters={
          <>
            <FormField
              label="Job"
              name="runJob"
              onChange={(event) => updateJobFilter(event.target.value)}
              value={jobFilter}
              wide
            />
            <SelectField
              label="Status"
              name="runStatus"
              onChange={(event) => setStatus(event.target.value as RunFilter)}
              options={runStatusOptions}
              value={status}
            />
            <SelectField
              label="Source"
              name="runSource"
              onChange={(event) => setSource(event.target.value as SourceFilter)}
              options={runSourceOptions}
              value={source}
            />
          </>
        }
      />
      <RunList
        emptyMessage={runEmptyMessage(runs.length, filteredRuns.length)}
        onSelectRun={onSelectRun}
        title={runListTitle(status, source)}
        runs={filteredRuns}
      />
    </>
  );
}

function runEmptyMessage(totalRuns: number, filteredRuns: number) {
  if (filteredRuns > 0) {
    return "";
  }

  return totalRuns > 0 ? "No runs match the current filters." : "No runs yet.";
}

function runListTitle(status: RunFilter, source: SourceFilter) {
  if (status === "all" && source === "all") {
    return "All Runs";
  }

  if (status === "all") {
    return `${runSourceTitleLabels[source]} Runs`;
  }

  if (source === "all") {
    return `${runStatusLabels[status]} Runs`;
  }

  return `${runSourceTitleLabels[source]} ${runStatusLabels[status]} Runs`;
}
