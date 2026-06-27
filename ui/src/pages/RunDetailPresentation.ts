import type { RunListItem, RunTaskItem, RunTaskStatus } from "../components";
import { runActorLabel, runTriggerLabel, shortRunID } from "../components/data/RunPresentation";

export type RunTimelineEvent = {
  delta?: string;
  detail: string;
  label: string;
  time: string;
};

export type RunTaskDisplayNode = RunTaskItem & {
  depth: number;
};

export function runDetailDescription() {
  return "Execution graph, logs, timeline, and definition context.";
}

export function runGraphDescription() {
  return "Execution flow for the run.";
}

export function runTasksForDisplay(run: RunListItem): RunTaskDisplayNode[] {
  const tasks = run.tasks?.length ? run.tasks : [fallbackRootTask(run)];
  const tasksByID = new Map(tasks.map((task) => [task.taskID, task]));
  const depthByID = new Map<string, number>();

  function depthFor(task: RunTaskItem): number {
    if (!task.parentTaskID || !tasksByID.has(task.parentTaskID)) {
      return 0;
    }

    const cached = depthByID.get(task.taskID);
    if (cached !== undefined) {
      return cached;
    }

    const parent = tasksByID.get(task.parentTaskID);
    const depth = parent ? depthFor(parent) + 1 : 0;
    depthByID.set(task.taskID, depth);

    return depth;
  }

  return tasks.map((task) => ({ ...task, depth: depthFor(task) })).sort((left, right) => left.depth - right.depth);
}

export function preferredRunTaskID(tasks: RunTaskDisplayNode[]) {
  const mostSpecificTasks = [...tasks].sort((left, right) => right.depth - left.depth);

  return (
    mostSpecificTasks.find((task) => task.status === "failed" || task.status === "aborted") ??
    mostSpecificTasks.find((task) => task.status === "running" || task.status === "accepted") ??
    tasks.find((task) => task.taskKey === "root") ??
    tasks[0]
  )?.taskID;
}

export function runTaskStatusLabel(status: RunTaskStatus) {
  switch (status) {
    case "planned":
      return "Planned";
    case "pending":
      return "Pending";
    case "accepted":
      return "Accepted";
    case "running":
      return "Running";
    case "succeeded":
      return "Succeeded";
    case "failed":
      return "Failed";
    case "cancelled":
      return "Cancelled";
    case "aborted":
      return "Aborted";
  }
}

export function runTaskTimingLabel(task: RunTaskItem) {
  const latestAttempt = latestTaskAttempt(task);
  if (!latestAttempt) {
    return "Not started";
  }

  const startedAt = parseTimestamp(latestAttempt.startedAt);
  const finishedAt = parseTimestamp(latestAttempt.finishedAt);
  if (startedAt && finishedAt) {
    return formatDelta(finishedAt.getTime() - startedAt.getTime());
  }

  if (startedAt) {
    return `Running ${formatDelta(Date.now() - startedAt.getTime())}`;
  }

  if (finishedAt) {
    return "Finished";
  }

  return "Accepted";
}

export function runTaskExecutionLabel(task: RunTaskItem) {
  const latestAttempt = latestTaskAttempt(task);
  if (!latestAttempt) {
    return "No execution attempt yet";
  }

  if (!latestAttempt.executionID) {
    return latestAttempt.cellID ? `Pending on ${latestAttempt.cellID}` : "Execution pending";
  }

  return shortIdentifier(latestAttempt.executionID);
}

export function runDefinitionTitle(run: RunListItem) {
  return run.source === "ephemeral" ? "Submitted Definition" : "Job Definition";
}

export function runDefinitionDescription(run: RunListItem) {
  return run.source === "ephemeral" ? "Inline work submitted for this run." : "Definition used when this run started.";
}

export function sourceLabel(source: NonNullable<RunListItem["source"]>) {
  return source === "ephemeral" ? "Ephemeral" : "Saved";
}

export function referenceLabel(run: RunListItem) {
  if (run.source === "ephemeral") {
    return "Inline";
  }

  if (run.definitionVersion) {
    return `v${run.definitionVersion}`;
  }

  return run.commit;
}

export function formatRunDefinition(run: RunListItem) {
  if (!run.definition) {
    return JSON.stringify(
      {
        id: run.jobName,
        source: run.source ?? "stored",
        run_id: run.id
      },
      null,
      2
    );
  }

  try {
    return JSON.stringify(JSON.parse(run.definition) as unknown, null, 2);
  } catch {
    return run.definition;
  }
}

export function runTimelineEvents(run: RunListItem): RunTimelineEvent[] {
  const acceptedAt = parseTimestamp(run.createdAt);
  const startedAt = parseTimestamp(run.startedAt);
  const finishedAt = parseTimestamp(run.finishedAt);
  const dispatchAt = startedAt ?? acceptedAt;
  const finishedLabel = run.status === "running" ? "Streaming" : "Finished";

  return [
    {
      label: "Accepted",
      detail: `${submittedByCopy(run)}.`,
      ...timingParts(acceptedAt)
    },
    {
      label: "Persisted",
      detail: `Assigned ID ${shortRunID(run.id)}.`,
      ...timingParts(acceptedAt, acceptedAt)
    },
    {
      label: "Dispatched",
      detail: `Worker selected on ${run.cellName ?? "an unassigned cell"}.`,
      ...timingParts(dispatchAt, acceptedAt)
    },
    {
      label: finishedLabel,
      detail: run.status === "running" ? "Logs are streaming from the worker." : "Worker finished execution.",
      ...timingParts(finishedAt, startedAt ?? acceptedAt)
    }
  ];
}

export function runLogLines(run: RunListItem) {
  if (run.status === "queued") {
    return [`accepted ${run.jobName}`, "waiting for queue dispatch"];
  }

  if (run.status === "running") {
    return [`accepted ${run.jobName}`, `worker claimed run on ${run.cellName ?? "cell"}`, "streaming output"];
  }

  return [`accepted ${run.jobName}`, "finished execution"];
}

function fallbackRootTask(run: RunListItem): RunTaskItem {
  return {
    attempts: [],
    name: "root",
    status: taskStatusFromRunStatus(run.status),
    taskID: `${run.id}:root`,
    taskKey: "root"
  };
}

function taskStatusFromRunStatus(status: RunListItem["status"]): RunTaskStatus {
  switch (status) {
    case "queued":
      return "pending";
    case "running":
      return "running";
    case "succeeded":
      return "succeeded";
    case "failed":
      return "failed";
    case "cancelled":
      return "cancelled";
    case "abandoned":
    case "orphaned":
    case "aborted":
      return "aborted";
  }
}

function latestTaskAttempt(task: RunTaskItem) {
  return [...task.attempts].sort((left, right) => right.attempt - left.attempt)[0];
}

function shortIdentifier(value: string) {
  return value.length > 12 ? `${value.slice(0, 4)}${value.slice(-4)}` : value;
}

function submittedByCopy(run: RunListItem) {
  const trigger = runTriggerLabel(run);
  const actor = run.submittedBy ? runActorLabel(run.submittedBy) : "Anonymous";
  return `Submitted via ${trigger} by ${actor}`;
}

function parseTimestamp(value?: string) {
  if (!value) {
    return null;
  }

  const time = Date.parse(value);
  return Number.isNaN(time) ? null : new Date(time);
}

function timingParts(timestamp: Date | null, previous?: Date | null) {
  if (!timestamp) {
    return { time: "time unavailable" };
  }

  const time = timestamp.toLocaleTimeString([], {
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit"
  });

  if (!previous) {
    return { time };
  }

  return {
    time,
    delta: `+${formatDelta(timestamp.getTime() - previous.getTime())}`
  };
}

function formatDelta(milliseconds: number) {
  const seconds = Math.max(0, Math.round(milliseconds / 1000));
  if (seconds < 60) {
    return `${seconds}s`;
  }

  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) {
    return `${minutes}m ${seconds % 60}s`;
  }

  const hours = Math.floor(minutes / 60);
  if (hours < 24) {
    return `${hours}h ${minutes % 60}m`;
  }

  const days = Math.floor(hours / 24);
  return `${days}d ${hours % 24}h`;
}
