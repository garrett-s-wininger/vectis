import type { ReactNode } from "react";
import { Ban, Check, CircleDashed, Clock, LoaderCircle, OctagonAlert, Play, X } from "lucide-react";
import { Button } from "../components";
import { runGraphDescription, runTaskStatusLabel, type RunTaskDisplayNode } from "./RunDetailPresentation";
import styles from "./RunTaskGraph.module.css";

type RunTaskTreeNode = RunTaskDisplayNode & {
  children: RunTaskTreeNode[];
};

type RunTaskGraphProps = {
  awaitingTasks?: boolean;
  controls?: ReactNode;
  onSelectTask: (taskID: string) => void;
  selectedTaskID?: string;
  onClearSelection?: () => void;
  tasks: RunTaskDisplayNode[];
};

export function RunTaskGraph({
  awaitingTasks = false,
  controls,
  onSelectTask,
  selectedTaskID,
  onClearSelection,
  tasks
}: RunTaskGraphProps) {
  const tree = taskTree(tasks);
  const graphDescription = runGraphDescription();

  return (
    <section
      className={`${styles.panel} polished-panel polished-panel--accent-top polished-panel--overlay-safe`}
      aria-labelledby="run-graph-title"
    >
      <div className={styles.header}>
        <div>
          <h2 id="run-graph-title">Graph</h2>
          {graphDescription ? <p>{graphDescription}</p> : null}
        </div>
        {controls || (selectedTaskID && onClearSelection) ? (
          <div className={styles.controls} role="group" aria-label="Run investigation controls">
            {controls}
            {selectedTaskID && onClearSelection ? (
              <Button variant="quiet" onClick={onClearSelection}>
                <X aria-hidden="true" /> Clear task selection
              </Button>
            ) : null}
          </div>
        ) : null}
      </div>
      <div className={styles.graph} role="group" aria-label="Execution graph">
        <div className={styles.tree}>
          {tree.map((task) => (
            <TaskGraphBranch
              key={task.taskID}
              onSelectTask={onSelectTask}
              selectedTaskID={selectedTaskID}
              task={task}
            />
          ))}
          {awaitingTasks ? (
            <div className={styles.awaitingTasks} role="status">
              <LoaderCircle className={styles.awaitingIcon} aria-hidden="true" />
              <span>Waiting for task updates</span>
            </div>
          ) : null}
        </div>
      </div>
    </section>
  );
}

function TaskGraphBranch({
  onSelectTask,
  selectedTaskID,
  task
}: {
  onSelectTask: (taskID: string) => void;
  selectedTaskID?: string;
  task: RunTaskTreeNode;
}) {
  const taskLabel = task.name || task.taskKey;
  const statusLabel = runTaskStatusLabel(task.status);

  return (
    <div className={styles.branch}>
      <button
        aria-label={`${taskLabel}: ${statusLabel}`}
        aria-pressed={task.taskID === selectedTaskID}
        className={`${styles.node} ${styles[task.status]} ${task.taskID === selectedTaskID ? styles.selected : ""}`}
        onClick={() => onSelectTask(task.taskID)}
        title={`${taskLabel}: ${statusLabel}`}
        type="button"
      >
        <span className={styles.indicator} aria-hidden="true">
          <TaskStatusGlyph status={task.status} />
        </span>
        <span className={styles.label}>{taskLabel}</span>
      </button>
      {task.children.length > 0 ? (
        <div
          className={`${styles.children} ${task.children.length > 1 ? styles.childrenMultiple : ""}`}
          role="group"
          aria-label={`${taskLabel} child tasks`}
        >
          {task.children.map((child) => (
            <div className={styles.child} key={child.taskID}>
              <TaskGraphBranch onSelectTask={onSelectTask} selectedTaskID={selectedTaskID} task={child} />
            </div>
          ))}
        </div>
      ) : null}
    </div>
  );
}

function TaskStatusGlyph({ status }: { status: RunTaskDisplayNode["status"] }) {
  switch (status) {
    case "planned":
      return <CircleDashed className={styles.statusIcon} />;
    case "pending":
      return <Clock className={styles.statusIcon} />;
    case "accepted":
      return <Play className={styles.statusIcon} />;
    case "running":
      return <LoaderCircle className={styles.statusIcon} />;
    case "succeeded":
      return <Check className={styles.statusIcon} />;
    case "failed":
      return <X className={styles.statusIcon} />;
    case "cancelled":
      return <Ban className={styles.statusIcon} />;
    case "aborted":
      return <OctagonAlert className={styles.statusIcon} />;
  }
}

function taskTree(tasks: RunTaskDisplayNode[]) {
  const nodesByID = new Map<string, RunTaskTreeNode>(tasks.map((task) => [task.taskID, { ...task, children: [] }]));
  const roots: RunTaskTreeNode[] = [];

  for (const task of tasks) {
    const node = nodesByID.get(task.taskID);
    if (!node) {
      continue;
    }

    const parent = task.parentTaskID ? nodesByID.get(task.parentTaskID) : undefined;
    if (parent) {
      parent.children.push(node);
    } else {
      roots.push(node);
    }
  }

  return roots;
}
