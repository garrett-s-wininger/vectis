import { useEffect, useState } from "react";
import { ChevronDown, FileText } from "lucide-react";
import { Button, CopyValue, ErrorAlert, ResourceStatus, type RunListItem } from "../components";
import type { LoadRunDefinition, RunDefinitionSnapshot } from "../data/runDefinition";
import { formatRunDefinition, runDefinitionDescription, runDefinitionTitle } from "./RunDetailPresentation";
import styles from "./RunDetailPage.module.css";
export function RunDefinition({
  run,
  load,
  open: controlledOpen,
  onOpenChange
}: {
  run: RunListItem;
  load?: LoadRunDefinition;
  open?: boolean;
  onOpenChange?: (open: boolean) => void;
}) {
  const [localOpen, setLocalOpen] = useState(false);
  const open = controlledOpen ?? localOpen;
  const [snapshot, setSnapshot] = useState<RunDefinitionSnapshot>();
  const [error, setError] = useState("");
  const [retry, setRetry] = useState(0);

  useEffect(() => {
    if (!open || !load || snapshot) return;

    let ignore = false;
    load(run.id)
      .then((value) => {
        if (value.run_id !== run.id || value.definition == null || typeof value.definition !== "object")
          throw new Error("The response did not contain this run's captured definition.");
        if (!ignore) setSnapshot(value);
      })
      .catch((cause) => {
        if (!ignore) setError(cause instanceof Error ? cause.message : "Unable to load the captured definition.");
      });

    return () => {
      ignore = true;
    };
  }, [open, load, run.id, retry, snapshot]);

  const definition = snapshot ? JSON.stringify(snapshot.definition, null, 2) : load ? "" : formatRunDefinition(run);
  const loading = open && Boolean(load) && !snapshot && !error;
  return (
    <section
      className={`${styles.panel} ${styles.definitionPanel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-definition-title"
    >
      <details
        className={styles.definitionDetails}
        open={open}
        onToggle={(event) => {
          const next = event.currentTarget.open;
          if (next !== open) {
            setLocalOpen(next);
            onOpenChange?.(next);
          }
        }}
      >
        <summary className={styles.definitionSummary}>
          <span className={styles.definitionIcon}>
            <FileText aria-hidden="true" />
          </span>
          <div>
            <h2 id="run-definition-title">{runDefinitionTitle(run)}</h2>
            <p>{runDefinitionDescription(run)}</p>
          </div>
          <span className={styles.definitionDisclosure} aria-hidden="true">
            <ChevronDown />
          </span>
        </summary>
        {loading ? (
          <p role="status">Loading captured definition…</p>
        ) : error ? (
          <div>
            <ErrorAlert title="Unable to Load Run Definition" message={error} />
            <Button
              variant="quiet"
              onClick={() => {
                setError("");
                setRetry((value) => value + 1);
              }}
            >
              Retry definition
            </Button>
          </div>
        ) : definition ? (
          <>
            <div className={styles.definitionMetadata}>
              <span>Captured snapshot{snapshot?.definition_version ? ` · v${snapshot.definition_version}` : ""}</span>
              <CopyValue label="run definition" value={definition}>
                Copy JSON
              </CopyValue>
            </div>
            {snapshot?.source?.hotfix ? (
              <div className={styles.definitionChangeReason}>
                {!run.hotfix ? (
                  <>
                    <div className={styles.summaryTitle}>
                      <h3>Reason for changes</h3>
                      <ResourceStatus tone="warning">Hotfix</ResourceStatus>
                    </div>
                    <p>{snapshot.source.hotfix.reason}</p>
                  </>
                ) : null}
                <div className={styles.snapshotBaseline}>
                  <span>Base commit</span>
                  <CopyValue label="Hotfix base commit" value={snapshot.source.hotfix.base_commit}>
                    <code title={snapshot.source.hotfix.base_commit}>
                      {snapshot.source.hotfix.base_commit.slice(0, 12)}
                    </code>
                  </CopyValue>
                </div>
              </div>
            ) : null}
            {snapshot?.source ? (
              <p className={styles.definitionProvenance}>
                {snapshot.source.repository_id} · {snapshot.source.path} · {snapshot.source.resolved_commit}
              </p>
            ) : null}
            <pre className={`definition-preview ${styles.definitionPreview}`}>{definition}</pre>
          </>
        ) : (
          <p>The captured definition is unavailable. Run metadata is not a substitute for its contents.</p>
        )}
      </details>
    </section>
  );
}
