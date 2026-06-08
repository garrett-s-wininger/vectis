import type { BreadcrumbItem } from "../../components";
import type { JobEditorMode } from "./JobEditor";

export function jobsIndexBreadcrumbItems(namespacePath: string): BreadcrumbItem[] {
  return [{ label: formatNamespaceCrumb(namespacePath) }, { label: "Jobs", current: true }];
}

export function jobDetailBreadcrumbItems({
  jobName,
  namespacePath,
  onJobs
}: {
  jobName: string;
  namespacePath: string;
  onJobs: () => void;
}): BreadcrumbItem[] {
  return [
    { label: formatNamespaceCrumb(namespacePath) },
    { label: "Jobs", onClick: onJobs },
    { label: jobName, current: true }
  ];
}

export function jobEditorBreadcrumbItems({
  editorJobName,
  mode,
  namespacePath,
  onJob,
  onJobs
}: {
  editorJobName: string;
  mode: JobEditorMode;
  namespacePath: string;
  onJob: () => void;
  onJobs: () => void;
}): BreadcrumbItem[] {
  const baseItems: BreadcrumbItem[] = [
    { label: formatNamespaceCrumb(namespacePath) },
    { label: "Jobs", onClick: onJobs }
  ];

  if (mode.kind === "create") {
    return [...baseItems, { label: "Create", current: true }];
  }

  return [...baseItems, { label: editorJobName, onClick: onJob }, { label: "Config", current: true }];
}

export function formatNamespaceCrumb(namespacePath: string) {
  return namespacePath === "/" ? "Root" : namespacePath;
}
