import { runSourceLabels, runStatusLabels, type RunFilter, type SourceFilter } from "../domain/consoleOptions";

export type RunListFilters = { job: string; status: RunFilter; source: SourceFilter };

export function runListFiltersFromSearch(search: string): RunListFilters {
  const params = new URLSearchParams(search);
  const status = params.get("status") ?? "all";
  const source = params.get("source") ?? "all";

  return {
    job: params.get("job") ?? "",
    status: Object.hasOwn(runStatusLabels, status) ? (status as RunFilter) : "all",
    source: Object.hasOwn(runSourceLabels, source) ? (source as SourceFilter) : "all"
  };
}

export function runListFiltersPath(search: string, hash: string, filters: RunListFilters) {
  const params = new URLSearchParams(search);
  for (const key of ["job", "status", "source"]) params.delete(key);

  if (filters.job.trim()) params.set("job", filters.job);
  if (filters.status !== "all") params.set("status", filters.status);
  if (filters.source !== "all") params.set("source", filters.source);

  const query = params.toString();
  return `/runs${query ? `?${query}` : ""}${hash}`;
}
