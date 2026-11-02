import { requestJSON } from "../api/client";

export type RunArtifact = {
  id: number;
  name: string;
  path: string;
  sizeBytes: number;
  contentType: string;
  cellID: string;
  taskID?: string;
  taskAttemptID?: string;
  executionID?: string;
};

export type ArtifactPage = { data: RunArtifact[]; nextCursor?: number };
export type ArtifactQuery = { cursor?: number; taskID?: string; attempt?: number; signal?: AbortSignal };
export type LoadRunArtifacts = (runID: string, query?: ArtifactQuery) => Promise<ArtifactPage>;

type APIArtifact = {
  id: number;
  name: string;
  path: string;
  size_bytes: number;
  content_type: string;
  cell_id: string;
  task_id?: string;
  task_attempt_id?: string;
  execution_id?: string;
};

export const loadRunArtifactPage: LoadRunArtifacts = async (runID, query = {}) => {
  const params = new URLSearchParams({ limit: "100" });
  if (query.cursor !== undefined) params.set("cursor", String(query.cursor));
  if (query.taskID) params.set("task_id", query.taskID);
  if (query.attempt !== undefined) params.set("attempt", String(query.attempt));

  const page = await requestJSON<{ data: APIArtifact[]; next_cursor?: number }>(
    `/api/v1/runs/${encodeURIComponent(runID)}/artifacts?${params}`,
    { signal: query.signal }
  );

  return {
    data: page.data.map((item) => ({
      id: item.id,
      name: item.name,
      path: item.path,
      sizeBytes: item.size_bytes,
      contentType: item.content_type,
      cellID: item.cell_id,
      taskID: item.task_id,
      taskAttemptID: item.task_attempt_id,
      executionID: item.execution_id
    })),
    nextCursor: page.next_cursor
  };
};

export function artifactDownloadURL(runID: string, name: string) {
  return `/api/v1/runs/${encodeURIComponent(runID)}/artifacts/${encodeURIComponent(name)}/download`;
}

export function artifactSizeLabel(bytes: number) {
  if (bytes < 1024) return `${bytes} B`;
  const units = ["KiB", "MiB", "GiB", "TiB"];

  let value = bytes / 1024;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit++;
  }

  return `${new Intl.NumberFormat(undefined, { maximumFractionDigits: 1 }).format(value)} ${units[unit]}`;
}
