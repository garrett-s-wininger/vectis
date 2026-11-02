import { useEffect, useState } from "react";
import { Clock, FileText, Server, Zap } from "lucide-react";
import { ErrorAlert, OperationalFact, type RunListItem } from "../components";
import { streamRunLogs, type RunLogEntry } from "../data/runLogs";
import { isTerminalRunStatus, runRootAttemptNumber } from "../components/data/RunPresentation";
import {
  runTaskExecutionLabel,
  runTaskStatusLabel,
  runTaskTimingLabel,
  type RunTaskDisplayNode
} from "./RunDetailPresentation";
import styles from "./RunDetailPage.module.css";

const retainedLogEntries = 5000;
const streamClosureGrace = 3000;

export function RunLogs({
  run,
  selectedTask,
  attempt
}: {
  run: RunListItem;
  selectedTask?: RunTaskDisplayNode;
  attempt?: number;
}) {
  const [state, setState] = useState<{
    runID: string;
    entries: RunLogEntry[];
    error: string;
    streamOpened: boolean;
    awaitingStatus: boolean;
    completedAttempt: number | null;
  }>({
    runID: run.id,
    entries: [],
    error: "",
    streamOpened: false,
    awaitingStatus: false,
    completedAttempt: null
  });

  const rootAttempt = runRootAttemptNumber(run);

  useEffect(() => {
    let active = true;
    let closureTimer: ReturnType<typeof setTimeout> | undefined;
    const close = streamRunLogs(
      run.id,
      (entry) => {
        if (!active) return;
        setState((previous) => {
          const entries = previous.runID === run.id ? previous.entries : [];
          if (entry.sequence >= 0 && entries.some((item) => item.sequence === entry.sequence)) return previous;

          return {
            runID: run.id,
            error: "",
            streamOpened: false,
            awaitingStatus: false,
            completedAttempt: null,
            entries: [...entries, entry].sort((a, b) => a.sequence - b.sequence).slice(-retainedLogEntries)
          };
        });
      },
      (error, streamOpened = false) => {
        if (!active) return;

        setState((previous) => ({
          runID: run.id,
          entries: previous.runID === run.id ? previous.entries : [],
          error,
          streamOpened,
          awaitingStatus: streamOpened,
          completedAttempt: null
        }));

        if (streamOpened)
          closureTimer = setTimeout(() => {
            if (active) setState((previous) => ({ ...previous, awaitingStatus: false }));
          }, streamClosureGrace);
      },
      () => {
        if (!active) return;
        clearTimeout(closureTimer);
        setState((previous) => ({
          runID: run.id,
          entries: previous.runID === run.id ? previous.entries : [],
          error: "",
          streamOpened: false,
          awaitingStatus: false,
          completedAttempt: rootAttempt
        }));
      }
    );

    return () => {
      active = false;
      clearTimeout(closureTimer);
      close();
    };
  }, [run.id, rootAttempt]);

  const visible =
    state.runID === run.id
      ? state
      : { entries: [], error: "", streamOpened: false, awaitingStatus: false, completedAttempt: null };
  const terminalStreamClosed = isTerminalRunStatus(run.status) && visible.streamOpened && Boolean(visible.error);
  const awaitingRunStatus = !terminalStreamClosed && visible.awaitingStatus && Boolean(visible.error);
  const taskScope = Boolean(selectedTask);
  const attempts = [...(selectedTask?.attempts ?? [])].sort((a, b) => b.attempt - a.attempt);
  const attemptValue = attempt ?? attempts[0]?.attempt ?? 1;
  const chosen = attempts.find((item) => item.attempt === attemptValue);
  const executions = new Set(chosen?.executionID ? [chosen.executionID] : []);

  const runExecutions = new Set(
    run.tasks?.flatMap((task) =>
      task.attempts
        .filter((item) => item.attempt === attemptValue)
        .flatMap((item) => (item.executionID ? [item.executionID] : []))
    )
  );

  const entries = taskScope
    ? visible.entries.filter((entry) => entry.executionID && executions.has(entry.executionID))
    : visible.entries.filter((entry) => entry.executionID && runExecutions.has(entry.executionID));
  const context = selectedTask && {
    ...selectedTask,
    status: chosen?.status ?? selectedTask.status,
    attempts: chosen ? [chosen] : []
  };

  return (
    <section
      className={`${styles.panel} ${styles.primaryPanel} polished-panel polished-panel--accent-top`}
      aria-labelledby="run-logs-title"
    >
      <div className={styles.panelHeader}>
        <div>
          <h2 id="run-logs-title">{taskScope ? "Task Logs" : "Run Logs"}</h2>
          <p>
            {taskScope ? "Worker output for the selected task." : "Worker output across all tasks for this attempt."}
          </p>
        </div>
      </div>
      {taskScope && context ? (
        <dl className={styles.taskContext}>
          <OperationalFact icon={FileText} label="Task" value={context.name || context.taskKey} />
          <OperationalFact icon={Zap} label="Status" value={runTaskStatusLabel(context.status)} />
          <OperationalFact icon={Clock} label="Duration" value={runTaskTimingLabel(context)} />
          <OperationalFact icon={Server} label="Execution" value={runTaskExecutionLabel(context)} />
        </dl>
      ) : null}
      <ErrorAlert
        message={terminalStreamClosed || awaitingRunStatus ? "" : visible.error}
        title="Log Stream Unavailable"
      />
      {terminalStreamClosed ? (
        <p className={styles.logNotice} role="status">
          Run {run.status}. Live log stream closed.
        </p>
      ) : null}
      {awaitingRunStatus ? (
        <p className={styles.logNotice} role="status">
          Live log stream closed. Waiting for run status…
        </p>
      ) : null}
      {visible.completedAttempt === rootAttempt ? (
        <p className={styles.logNotice} role="status">
          Output complete.
        </p>
      ) : null}
      {entries.length > 0 ? (
        <RunLogOutput entries={entries} />
      ) : (
        <div className={styles.logFrame}>
          <p className={`${styles.logNotice} ${styles.emptyLogs}`}>
            {taskScope && attempts.length === 0
              ? attempt !== undefined
                ? `This task has no execution in attempt ${attempt}.`
                : "This task has not started an execution attempt."
              : taskScope
                ? "No attributed output for this task in the recent run log."
                : "No output received for this run."}
          </p>
        </div>
      )}
      {visible.entries.some((entry) => !entry.executionID) ? (
        <p className={styles.logNotice}>
          Some output has no execution identity and cannot be assigned to this attempt.
        </p>
      ) : null}
      <p className={styles.logNotice}>This view shows recent output. Older records may be outside the replay window.</p>
    </section>
  );
}

export function RunLogOutput({ entries }: { entries: RunLogEntry[] }) {
  return (
    <div className={styles.logFrame}>
      <pre aria-label="Log output" tabIndex={0} className={`code-block ${styles.logs}`}>
        {entries.map((entry, index) => (
          <span className={entry.stream === "stderr" ? styles.stderr : undefined} key={`${entry.sequence}:${index}`}>
            {entry.data}
          </span>
        ))}
      </pre>
    </div>
  );
}
