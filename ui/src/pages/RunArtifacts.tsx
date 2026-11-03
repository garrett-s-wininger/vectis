import { useId } from "react";
import { Download, File, HardDrive, ListTree, RefreshCw } from "lucide-react";
import { Button, CopyValue, ErrorAlert, OperationalFact, type RunListItem } from "../components";
import { artifactDownloadURL, artifactSizeLabel, type LoadRunArtifacts, type RunArtifact } from "../data/runArtifacts";
import { useRunArtifacts } from "../data/useRunArtifacts";
import type { RunTaskDisplayNode } from "./RunDetailPresentation";
import buttonStyles from "../components/primitives/Button.module.css";
import styles from "./RunArtifacts.module.css";

export function RunArtifacts({
  run,
  selectedTask,
  loadArtifacts,
  attempt
}: {
  run: RunListItem;
  selectedTask?: RunTaskDisplayNode;
  loadArtifacts: LoadRunArtifacts;
  attempt?: number;
}) {
  const titleID = useId();
  const taskScope = Boolean(selectedTask);
  const artifacts = useRunArtifacts(run, loadArtifacts, selectedTask?.taskID, attempt);
  return (
    <section className={`${styles.panel} polished-panel polished-panel--accent-top`} aria-labelledby={titleID}>
      <div className={styles.header}>
        <div>
          <h2 id={titleID}>Artifacts</h2>
          <p>
            {selectedTask
              ? `Files published by ${selectedTask.name || selectedTask.taskKey}.`
              : "Files published by this run."}
          </p>
        </div>
        <Button
          variant="quiet"
          onClick={artifacts.refresh}
          disabled={artifacts.loading || artifacts.refreshing}
          aria-label="Refresh artifacts"
        >
          <RefreshCw aria-hidden="true" /> Refresh
        </Button>
      </div>
      <ErrorAlert title="Artifacts unavailable" message={artifacts.error} />
      {artifacts.loading ? (
        <p role="status" className={styles.notice}>
          Loading artifacts…
        </p>
      ) : null}
      {!artifacts.loading && !artifacts.error && artifacts.data.length === 0 ? (
        <p className={styles.notice}>
          {attempt !== undefined
            ? taskScope
              ? `No artifacts for this task in attempt ${attempt}.`
              : `No artifacts for attempt ${attempt}.`
            : taskScope
              ? "No artifacts published by this task."
              : "No artifacts published for this run."}
        </p>
      ) : null}
      {artifacts.data.length > 0 ? (
        <ul className={styles.items} aria-label="Published artifacts">
          {artifacts.data.map((artifact) => (
            <li key={artifact.name} className={styles.item}>
              <div className={styles.identity}>
                <strong>{artifact.name}</strong>
                <CopyValue appearance="field" label={`path for ${artifact.name}`} value={artifact.path} />
              </div>
              <dl className={styles.facts}>
                <OperationalFact icon={HardDrive} label="Size" value={artifactSizeLabel(artifact.sizeBytes)} />
                <OperationalFact icon={ListTree} label="Produced by" value={artifactProducer(artifact, run)} wrap />
                <OperationalFact
                  icon={File}
                  label="Type"
                  value={artifact.contentType || "application/octet-stream"}
                  wrap
                />
              </dl>
              <a
                className={`${buttonStyles.root} ${buttonStyles.quiet} ${styles.download}`}
                href={artifactDownloadURL(run.id, artifact.name)}
                download={artifact.path.replaceAll("\\", "/").split("/").at(-1) || artifact.name}
                target="_blank"
                rel="noopener noreferrer"
                aria-label={`Download ${artifact.name}`}
              >
                <Download aria-hidden="true" /> Download
              </a>
            </li>
          ))}
        </ul>
      ) : null}
      {artifacts.nextCursor !== undefined ? (
        <Button variant="quiet" onClick={artifacts.loadMore} disabled={artifacts.refreshing}>
          Load more artifacts
        </Button>
      ) : null}
    </section>
  );
}

function artifactProducer(artifact: RunArtifact, run: RunListItem) {
  if (!artifact.taskID) return "Run-wide";
  const task = run.tasks?.find((item) => item.taskID === artifact.taskID);
  return task?.name || task?.taskKey || artifact.taskID;
}
