import { useEffect, useState } from "react";
import { Clock, FileText, Server, Zap } from "lucide-react";
import { Button, ErrorAlert, OperationalFact, SelectField, type RunListItem } from "../components";
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

export function RunLogs({ run, selectedTask }: { run: RunListItem; selectedTask?: RunTaskDisplayNode }) {
  const [scope, setScope] = useState("task");
  const [selection, setSelection] = useState({ taskID: "", runID: "", attempt: "latest" });
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
  const taskScope = scope === "task" && Boolean(selectedTask);
  const attempts = [...(selectedTask?.attempts ?? [])].sort((a, b) => b.attempt - a.attempt);
  const requested =
    selection.runID === run.id && selection.taskID === selectedTask?.taskID ? selection.attempt : "latest";
  const attemptValue =
    requested === "all" || attempts.some((item) => item.attemptID === requested) ? requested : "latest";
  const chosen = attemptValue === "latest" ? attempts[0] : attempts.find((item) => item.attemptID === attemptValue);
  const executions = new Set(
    (attemptValue === "all" ? attempts : chosen ? [chosen] : []).flatMap((item) =>
      item.executionID ? [item.executionID] : []
    )
  );

  const entries = taskScope
    ? visible.entries.filter((entry) => entry.executionID && executions.has(entry.executionID))
    : visible.entries;
  const context = selectedTask && {
    ...selectedTask,
    status: chosen?.status ?? selectedTask.status,
    attempts: chosen ? [chosen] : []
  };

  return (
    <section
      className={`${styles.panel} ${styles.primaryPanel} polished-panel polished-panel--accent-top polished-panel--overlay-safe`}
      aria-labelledby="run-logs-title"
    >
      <div className={styles.panelHeader}>
        <div>
          <h2 id="run-logs-title">{taskScope ? "Task Logs" : "Run Logs"}</h2>
          <p>
            {taskScope
              ? "Worker output for the selected task."
              : "Worker output across all tasks, including unattributed records."}
          </p>
        </div>
      </div>
      <div className={styles.logControls}>
        <SelectField
          label="Output"
          value={taskScope ? "task" : "run"}
          onChange={(event) => setScope(event.target.value)}
          options={[
            ...(selectedTask ? [{ value: "task", label: "Selected task" }] : []),
            { value: "run", label: "All run output" }
          ]}
        />
        {taskScope && attempts.length > 0 ? (
          <SelectField
            label="Attempt"
            value={attemptValue}
            onChange={(event) =>
              setSelection({ runID: run.id, taskID: selectedTask!.taskID, attempt: event.target.value })
            }
            options={[
              { value: "latest", label: `Latest (attempt ${attempts[0].attempt})` },
              { value: "all", label: "All attempts" },
              ...attempts.map((item) => ({ value: item.attemptID, label: `Attempt ${item.attempt}` }))
            ]}
          />
        ) : null}
      </div>
      {taskScope && context ? (
        <dl className={styles.taskContext}>
          <OperationalFact icon={FileText} label="Task" value={context.name || context.taskKey} />
          <OperationalFact icon={Zap} label="Status" value={runTaskStatusLabel(context.status)} />
          {attemptValue === "all" ? (
            <OperationalFact icon={Server} label="Execution" value="All attempts" />
          ) : (
            <>
              <OperationalFact icon={Clock} label="Duration" value={runTaskTimingLabel(context)} />
              <OperationalFact icon={Server} label="Execution" value={runTaskExecutionLabel(context)} />
            </>
          )}
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
              ? "This task has not started an execution attempt."
              : taskScope
                ? "No attributed output for this task in the recent run log."
                : "No output received for this run."}
          </p>
        </div>
      )}
      {taskScope && visible.entries.some((entry) => !entry.executionID) ? (
        <div className={styles.logNotice}>
          <p>Some output has no task attribution. It is available in the run-wide view.</p>
          <Button variant="quiet" onClick={() => setScope("run")}>
            View all run output
          </Button>
        </div>
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
