import type { RunListItem } from "../components";
import { runActorLabel, runTriggerLabel } from "../components/data/RunPresentation";

export type RunTimelineEvent = {
  delta?: string;
  detail: string;
  label: string;
  time: string;
};

export function runDetailDescription() {
  return "Execution state, logs, and definition context.";
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
      detail: `Assigned ID ${run.id}.`,
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

  return `${Math.floor(seconds / 60)}m ${seconds % 60}s`;
}
