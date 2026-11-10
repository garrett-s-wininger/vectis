import type { SourceHotfix } from "./sourceJobs";
import { requestJSON } from "../api/client";

export type RunDefinitionSnapshot = {
  run_id: string;
  definition_version: number;
  definition_hash?: string;
  definition: unknown;
  source?: {
    hotfix?: SourceHotfix;
    repository_id: string;
    requested_ref: string;
    resolved_commit: string;
    path: string;
    blob_sha?: string;
  };
};

export type LoadRunDefinition = (runID: string) => Promise<RunDefinitionSnapshot>;
export const loadRunDefinition: LoadRunDefinition = (runID) =>
  requestJSON(`/api/v1/runs/${encodeURIComponent(runID)}/definition`);
