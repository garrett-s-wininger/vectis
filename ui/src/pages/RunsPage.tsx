import { useConsoleAccess } from "../domain/access";
import type { ChangeEvent, FormEvent } from "react";
import { memo, useCallback, useEffect, useMemo, useRef, useState } from "react";
import { BreadcrumbTrail } from "../components";
import { Button } from "../components";
import { FormError } from "../components";
import { FilterBar } from "../components";
import { FormField } from "../components";
import { NamespacePicker } from "../components";
import { PageHeader } from "../components";
import { RunList, type RunListItem } from "../components";
import { SelectField } from "../components";
import { runDisplayName } from "../components/data/RunPresentation";
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
import type { RunListFilters } from "../routing/runListFilters";
import { jsonObject } from "../validation/FormValidation";
import { formatNamespaceCrumb } from "./jobs/JobBreadcrumbs";
import { ResourceTitle } from "./shared";
import styles from "./RunsPage.module.css";

const maxDefinitionFileBytes = 10 * 1024 * 1024;

type RunsPageProps = {
  jobName?: string;
  filters?: RunListFilters;
  onFiltersChange?: (filters: RunListFilters, options?: { replace?: boolean }) => void;
  namespaces: Namespace[];
  namespacePath: string;
  sharedWorkspace?: boolean;
  onSelectNamespace: (namespacePath: string) => void;
  onSelectRun: (runID: string) => void;
  onSubmitEphemeralRun: (definition: string) => Promise<void> | void;
  runs: RunListItem[];
};

export function RunsPage({
  jobName,
  filters,
  onFiltersChange,
  namespaces,
  namespacePath,
  sharedWorkspace = false,
  onSelectNamespace,
  onSelectRun,
  onSubmitEphemeralRun,
  runs
}: RunsPageProps) {
  const { can } = useConsoleAccess();
  const [localFilters, setLocalFilters] = useState<RunListFilters>({ job: "", status: "all", source: "all" });
  const effectiveFilters = useMemo(
    () => filters ?? { ...localFilters, job: jobName ?? localFilters.job },
    [filters, localFilters, jobName]
  );

  const { job: jobFilter, status, source } = effectiveFilters;
  const changeFilters = useCallback(
    (next: RunListFilters, options?: { replace?: boolean }) => {
      if (!filters) setLocalFilters(next);
      onFiltersChange?.(next, options);
    },
    [filters, onFiltersChange]
  );

  const [showRunOnce, setShowRunOnce] = useState(false);
  const [definition, setDefinition] = useState(defaultRunDefinition);
  const [definitionError, setDefinitionError] = useState("");
  const [fileName, setFileName] = useState("");
  const [fileError, setFileError] = useState("");
  const [readingFile, setReadingFile] = useState(false);
  const [submittingRun, setSubmittingRun] = useState(false);
  const [submissionError, setSubmissionError] = useState("");
  const submissionInFlight = useRef(false);
  const fileInput = useRef<HTMLInputElement>(null);
  const fileReader = useRef<FileReader | null>(null);

  useEffect(
    () => () => {
      const reader = fileReader.current;
      fileReader.current = null;
      reader?.abort();
    },
    []
  );

  function cancelFileRead() {
    const reader = fileReader.current;
    fileReader.current = null;
    reader?.abort();
    setReadingFile(false);
  }

  function loadDefinitionFile(event: ChangeEvent<HTMLInputElement>) {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file) return;

    cancelFileRead();
    setFileError("");
    if (file.size > maxDefinitionFileBytes) {
      setFileError("Choose a JSON file no larger than 10 MiB. The current definition has been kept.");
      return;
    }

    const reader = new FileReader();
    fileReader.current = reader;
    setReadingFile(true);

    reader.onload = () => {
      if (fileReader.current !== reader) return;
      fileReader.current = null;
      setReadingFile(false);

      const contents = typeof reader.result === "string" ? reader.result.replace(/^\uFEFF/, "") : "";
      const result = jsonObject(contents);
      if (!result.valid) {
        setFileError(`${result.message} The current definition has been kept.`);
        return;
      }

      setDefinition(contents);
      setDefinitionError("");
      setFileName(file.name);
    };

    reader.onerror = () => {
      if (fileReader.current !== reader) return;
      fileReader.current = null;

      setReadingFile(false);
      setFileError("Unable to read that file. The current definition has been kept.");
    };

    reader.readAsText(file, "UTF-8");
  }

  const filteredRuns = useMemo(() => {
    const normalizedJobFilter = jobFilter.trim().toLowerCase();

    return runs.filter((run) => {
      const jobMatches =
        !normalizedJobFilter ||
        run.jobName.toLowerCase().includes(normalizedJobFilter) ||
        runDisplayName(run).toLowerCase().includes(normalizedJobFilter);
      const sourceMatches = source === "all" || (run.source ?? "stored") === source;
      const statusMatches = status === "all" || run.status === status;
      return jobMatches && sourceMatches && statusMatches;
    });
  }, [runs, jobFilter, source, status]);

  async function submitRunOnce(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (readingFile || submissionInFlight.current) return;
    setDefinitionError("");

    const definitionResult = jsonObject(definition);
    if (!definitionResult.valid) {
      setDefinitionError(definitionResult.message);
      return;
    }

    submissionInFlight.current = true;
    setSubmittingRun(true);
    setSubmissionError("");

    try {
      await onSubmitEphemeralRun(definition);
      setDefinition(defaultRunDefinition);
      setFileName("");
      setFileError("");
      setShowRunOnce(false);
    } catch (error) {
      setSubmissionError(error instanceof Error ? error.message : "Unable to submit run.");
    } finally {
      submissionInFlight.current = false;
      setSubmittingRun(false);
    }
  }

  return (
    <>
      <PageHeader
        description="Recent queued, running, and completed work."
        navigation={
          <BreadcrumbTrail
            items={[
              ...(!sharedWorkspace ? [{ label: formatNamespaceCrumb(namespacePath) }] : []),
              { label: "Runs", current: true }
            ]}
            label="Runs location"
          />
        }
        actions={
          <>
            {!sharedWorkspace ? (
              <NamespacePicker compact namespaces={namespaces} onChange={onSelectNamespace} value={namespacePath} />
            ) : null}
            {can("run:trigger", namespacePath) ? (
              <Button
                aria-disabled={submittingRun}
                aria-expanded={showRunOnce}
                onClick={() => {
                  if (submissionInFlight.current) return;
                  cancelFileRead();
                  setShowRunOnce((value) => !value);
                }}
              >
                {showRunOnce ? "Close" : "Run Once"}
              </Button>
            ) : null}
          </>
        }
        title="Runs"
      />
      {showRunOnce && can("run:trigger", namespacePath) ? (
        <section
          className={`${styles.runOncePanel} polished-panel polished-panel--accent-top`}
          aria-labelledby="run-once-title"
        >
          <div className={styles.runOnceHeader}>
            <ResourceTitle
              id="run-once-title"
              subtitle="Submit an inline definition without saving a job."
              title="Run Once"
            />
          </div>
          <form aria-busy={submittingRun} className={styles.runOnceForm} onSubmit={submitRunOnce}>
            <div className={styles.fileImport}>
              <Button
                aria-disabled={submittingRun}
                onClick={() => {
                  if (!submissionInFlight.current) fileInput.current?.click();
                }}
                variant="quiet"
              >
                Load JSON file
              </Button>
              <input
                accept=".json,application/json"
                aria-label="Choose definition JSON file"
                hidden
                onChange={loadDefinitionFile}
                ref={fileInput}
                type="file"
              />
              <span aria-busy={readingFile} role="status">
                {fileName ? `Imported ${fileName}` : "Load a file or edit the JSON below, then submit."}
              </span>
            </div>
            <FormError message={fileError} title="Unable to load definition" />
            <TextAreaField
              code
              readOnly={readingFile || submittingRun}
              error={definitionError}
              label="Definition JSON"
              name="definition"
              onChange={(event) => {
                setDefinitionError("");
                setFileError("");
                setDefinition(event.target.value);
              }}
              required
              rows={10}
              value={definition}
              wide
            />
            <FormError message={submissionError} title="Unable to submit run" />
            <div className={styles.runOnceActions}>
              <Button aria-disabled={readingFile || submittingRun} type="submit">
                Submit run
              </Button>
            </div>
          </form>
        </section>
      ) : null}
      <RunsFilters filters={effectiveFilters} onChange={changeFilters} />
      <RunList
        emptyMessage={runEmptyMessage(runs.length, filteredRuns.length)}
        onSelectRun={onSelectRun}
        title={runListTitle(status, source)}
        runs={filteredRuns}
      />
    </>
  );
}

const RunsFilters = memo(function RunsFilters({
  filters,
  onChange
}: {
  filters: RunListFilters;
  onChange: (filters: RunListFilters, options?: { replace?: boolean }) => void;
}) {
  const { job: jobFilter, status, source } = filters;

  return (
    <FilterBar
      actions={
        <Button
          disabled={status === "all" && source === "all" && !jobFilter}
          onClick={() => onChange({ job: "", status: "all", source: "all" })}
        >
          Clear
        </Button>
      }
      filters={
        <>
          <FormField
            label="Job"
            name="runJob"
            onChange={(event) => onChange({ ...filters, job: event.target.value }, { replace: true })}
            value={jobFilter}
            wide
          />
          <SelectField
            label="Status"
            name="runStatus"
            onChange={(event) => onChange({ ...filters, status: event.target.value as RunFilter })}
            options={runStatusOptions}
            value={status}
          />
          <SelectField
            label="Source"
            name="runSource"
            onChange={(event) => onChange({ ...filters, source: event.target.value as SourceFilter })}
            options={runSourceOptions}
            value={source}
          />
        </>
      }
    />
  );
});

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
