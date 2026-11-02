import type { RunListItem } from "../components";
import type { LoadRunArtifacts, RunArtifact } from "../data/runArtifacts";
import { mockRunTasks } from "./fixtures";

export const artifactRun: RunListItem = {
  id: "artifact-demo-run",
  jobID: "artifact-demo",
  jobName: "artifacts-demo",
  runNumber: 7,
  commit: "v1",
  duration: "8s",
  status: "succeeded",
  cellName: "local",
  source: "ephemeral",
  tasks: mockRunTasks("artifact-demo-run", [
    { key: "root", name: "root", status: "succeeded" },
    { key: "report", name: "Report", parentKey: "root", status: "succeeded" },
    { key: "archive", name: "Archive", parentKey: "root", status: "succeeded" }
  ])
};

export const mockArtifacts: RunArtifact[] = [
  {
    id: 1,
    name: "demo-report",
    path: "reports/report.txt",
    sizeBytes: 2048,
    contentType: "text/plain",
    cellID: "local",
    taskID: artifactRun.tasks![1].taskID,
    taskAttemptID: artifactRun.tasks![1].attempts[0].attemptID
  },
  {
    id: 2,
    name: "demo-bundle",
    path: "demo-bundle.tar.gz",
    sizeBytes: 1048576,
    contentType: "application/gzip",
    cellID: "local",
    taskID: artifactRun.tasks![2].taskID,
    executionID: artifactRun.tasks![2].attempts[0].executionID
  },
  { id: 3, name: "empty-file", path: "empty.txt", sizeBytes: 0, contentType: "", cellID: "local" }
];

export const loadMockArtifacts: LoadRunArtifacts = async (_runID, query) => ({
  data: mockArtifacts.filter((item) => {
    if (query?.taskID && item.taskID !== query.taskID) return false;
    if (query?.attempt === undefined) return true;
    return artifactRun.tasks?.some((task) =>
      task.attempts.some(
        (attempt) =>
          attempt.attempt === query.attempt &&
          (attempt.attemptID === item.taskAttemptID || (item.executionID && attempt.executionID === item.executionID))
      )
    );
  })
});
