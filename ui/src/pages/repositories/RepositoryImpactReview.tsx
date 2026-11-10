import { useEffect, useState } from "react";
import { Button, DataTable, ErrorAlert, ResourceStatus, ToggleField } from "../../components";
import type { RepositorySource } from "../../data/repositories";
import type { RepositoryImpact } from "../../data/sourceJobs";
import styles from "./RepositoryConnectionEditor.module.css";
export function RepositoryImpactReview({
  id,
  source,
  onReviewed
}: {
  id: string;
  source: RepositorySource;
  onReviewed: (revision: string) => void;
}) {
  const [impact, setImpact] = useState<RepositoryImpact>();
  const [error, setError] = useState("");
  const [reload, setReload] = useState(0);
  const [reviewed, setReviewed] = useState(false);

  useEffect(() => {
    let ignore = false;
    source
      .impact(id)
      .then((row) => {
        if (!ignore) setImpact(row);
      })
      .catch((cause) => {
        if (!ignore) setError(cause instanceof Error ? cause.message : "Unable to load affected jobs.");
      });

    return () => {
      ignore = true;
    };
  }, [id, source, reload]);

  return (
    <section className={styles.impact} aria-labelledby="disable-impact-title">
      <div className={styles.heading}>
        <h3 id="disable-impact-title">Review affected jobs</h3>
        <Button
          variant="quiet"
          onClick={() => {
            onReviewed("");
            setReviewed(false);
            setImpact(undefined);
            setError("");
            setReload((value) => value + 1);
          }}
        >
          Refresh inventory
        </Button>
      </div>
      <p>
        Disabling this shared connection stops new submissions in every granted namespace. Existing runs keep executing
        from their captured definitions, and history is preserved.
      </p>
      <ErrorAlert title="Unable to Load Disable Inventory" message={error} />
      {!impact && !error ? <p role="status">Loading affected jobs and automation…</p> : null}
      {impact ? (
        <>
          <p>
            Namespace access:{" "}
            {impact.namespaces.map((path) => (path === "/" ? "Root" : path)).join(", ") || "No grants"}.
          </p>
          {impact.discovery_error ? (
            <ErrorAlert title="Inventory Is Incomplete" message={impact.discovery_error} />
          ) : null}
          <DataTable
            variant="inset"
            rows={impact.jobs}
            getRowKey={(row) => row.job_id}
            emptyMessage="No jobs have an assigned owner."
            columns={[
              { header: "Job", cell: (row) => <strong>{row.job_id}</strong> },
              { header: "Owner", cell: (row) => (row.namespace === "/" ? "Root" : row.namespace) },
              {
                header: "Definition",
                cell: (row) => (
                  <ResourceStatus tone={row.hotfix ? "warning" : "neutral"}>
                    {row.hotfix ? "Hotfix active" : "Source"}
                  </ResourceStatus>
                )
              }
            ]}
          />
          {impact.automations.length ? (
            <DataTable
              variant="inset"
              rows={impact.automations}
              getRowKey={(row) => `${row.namespace}:${row.job_id}:${row.kind}`}
              columns={[
                { header: "Automation job", cell: (row) => row.job_id },
                { header: "Namespace", cell: (row) => (row.namespace === "/" ? "Root" : row.namespace) },
                { header: "Trigger", cell: (row) => `${row.kind} (${row.enabled ? "enabled" : "disabled"})` }
              ]}
            />
          ) : (
            <p>No source automation is configured.</p>
          )}
          {impact.unregistered_jobs.length ? (
            <>
              <p>Unassigned definitions available through this connection are also affected:</p>
              <DataTable
                variant="inset"
                rows={impact.unregistered_jobs}
                getRowKey={(row) => row.path}
                columns={[
                  { header: "Job", cell: (row) => row.job_id },
                  { header: "Source file", cell: (row) => <code>{row.path}</code> }
                ]}
              />
            </>
          ) : null}
          <ToggleField
            label={
              impact.discovery_error
                ? "I reviewed the known impact and understand discovery is incomplete"
                : "I reviewed the affected jobs and automation"
            }
            name="disableImpactReviewed"
            checked={reviewed}
            onText="Reviewed"
            offText="Review required"
            onChange={(checked) => {
              setReviewed(checked);
              onReviewed(checked ? impact.revision : "");
            }}
          />
        </>
      ) : null}
    </section>
  );
}
