import type { RunListItem } from "../components";
import type { LoadRunArtifacts, RunArtifact } from "../data/runArtifacts";
import { artifactRun } from "./runArtifacts";

export const retriedArtifactRun: RunListItem = {
  ...artifactRun,
  tasks: artifactRun.tasks?.map((task) => ({
    ...task,
    attempts: (task.taskKey === "archive" ? [1] : [1, 2]).map((attempt) => ({
      attempt,
      attemptID: `${task.taskID}:attempt:${attempt}`,
      executionID: `${task.taskID}:exec:${attempt}`,
      cellID: "local",
      status: attempt === 1 && task.taskKey !== "archive" ? "failed" : "succeeded",
      startedAt: `2026-05-31T12:0${attempt}:00Z`,
      finishedAt: `2026-05-31T12:0${attempt}:05Z`
    }))
  }))
};

export const retriedArtifacts: RunArtifact[] = [
  {
    id: 1,
    name: "report-attempt-1",
    path: "report-1.txt",
    sizeBytes: 100,
    contentType: "text/plain",
    cellID: "local",
    taskID: retriedArtifactRun.tasks![1].taskID,
    taskAttemptID: retriedArtifactRun.tasks![1].attempts[0].attemptID
  },
  {
    id: 2,
    name: "archive-attempt-1",
    path: "archive-1.zip",
    sizeBytes: 1024,
    contentType: "application/zip",
    cellID: "local",
    taskID: retriedArtifactRun.tasks![2].taskID,
    taskAttemptID: retriedArtifactRun.tasks![2].attempts[0].attemptID
  },
  {
    id: 3,
    name: "report-attempt-2",
    path: "report-2.txt",
    sizeBytes: 200,
    contentType: "text/plain",
    cellID: "local",
    taskID: retriedArtifactRun.tasks![1].taskID,
    taskAttemptID: retriedArtifactRun.tasks![1].attempts[1].attemptID
  },
  { id: 4, name: "legacy-output", path: "legacy.txt", sizeBytes: 1, contentType: "text/plain", cellID: "local" }
];

export const loadRetriedArtifacts: LoadRunArtifacts = async (_id, query) => ({
  data: retriedArtifacts.filter((artifact) => {
    if (query?.taskID && artifact.taskID !== query.taskID) return false;
    if (query?.attempt === undefined) return true;
    return retriedArtifactRun.tasks?.some((task) =>
      task.attempts.some((attempt) => attempt.attempt === query.attempt && attempt.attemptID === artifact.taskAttemptID)
    );
  })
});
