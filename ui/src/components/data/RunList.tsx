import { Clock, Server, User, Zap } from "lucide-react";
import { OperationalFact } from "../primitives/OperationalFact";
import { StatusBadge, type RunStatus } from "../status/StatusBadge";
import {
  RecordList,
  RecordListIdentity,
  RecordListMeta,
  RecordListSummary,
  type RecordListRailTone
} from "./RecordList";
import {
  runActorLabel,
  runCountLabel,
  runDisplayName,
  runDurationLabel,
  runTriggerLabel,
  type RunTrigger
} from "./RunPresentation";

export type RunListItem = {
  id: string;
  jobName: string;
  runNumber: number;
  commit: string;
  createdAt?: string;
  definition?: string;
  definitionVersion?: number;
  duration: string;
  finishedAt?: string;
  cellName?: string;
  namespacePath?: string;
  source?: "stored" | "ephemeral";
  startedAt?: string;
  submittedBy?: string;
  tasks?: RunTaskItem[];
  trigger?: RunTrigger;
  status: RunStatus;
};

export type RunTaskStatus =
  | "planned"
  | "pending"
  | "accepted"
  | "running"
  | "succeeded"
  | "failed"
  | "cancelled"
  | "aborted";

export type RunTaskAttemptItem = {
  attempt: number;
  attemptID: string;
  cellID: string;
  executionID?: string;
  executionStatus?: RunTaskStatus;
  finishedAt?: string;
  startedAt?: string;
  status: RunTaskStatus;
};

export type RunTaskItem = {
  attempts: RunTaskAttemptItem[];
  name: string;
  parentTaskID?: string;
  status: RunTaskStatus;
  taskID: string;
  taskKey: string;
};

type RunListProps = {
  emptyMessage?: string;
  hideJobName?: boolean;
  onSelectRun?: (runID: string) => void;
  title: string;
  runs: RunListItem[];
};

export function RunList({
  emptyMessage = "No runs match the current filters.",
  hideJobName,
  onSelectRun,
  title,
  runs
}: RunListProps) {
  return (
    <RecordList
      countLabel={runCountLabel(runs.length)}
      description="Latest execution records"
      emptyMessage={emptyMessage}
      items={runs.map((run) => ({
        actions: <StatusBadge status={run.status} />,
        ariaLabel: `Open run ${runDisplayName(run)} #${run.runNumber}`,
        content: (
          <RecordListSummary>
            <RecordListIdentity
              subtitle={!hideJobName ? `Run #${run.runNumber}` : undefined}
              title={hideJobName ? `Run #${run.runNumber}` : runDisplayName(run)}
            />
            <RecordListMeta>
              <OperationalFact emphasis icon={Clock} label={runDurationLabel(run.status)} value={run.duration} />
              <OperationalFact icon={Server} label="Cell" value={run.cellName ?? "Unassigned"} />
              <OperationalFact icon={Zap} label="Trigger" value={runTriggerLabel(run)} />
              {run.submittedBy ? (
                <OperationalFact icon={User} label="Actor" value={runActorLabel(run.submittedBy)} />
              ) : null}
            </RecordListMeta>
          </RecordListSummary>
        ),
        key: run.id,
        onSelect: onSelectRun ? () => onSelectRun(run.id) : undefined,
        railTone: runRailTone(run.status)
      }))}
      title={title}
    />
  );
}

function runRailTone(status: RunStatus): RecordListRailTone {
  switch (status) {
    case "queued":
      return "neutral";
    case "running":
      return "info";
    case "succeeded":
      return "success";
    case "failed":
      return "danger";
    case "cancelled":
    case "abandoned":
    case "aborted":
      return "warningMuted";
    case "orphaned":
      return "warning";
  }
}
