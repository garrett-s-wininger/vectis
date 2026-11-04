import type { BreadcrumbItem } from "../../components";
import type { JobEditorMode } from "./JobEditorModel";

export function jobsIndexBreadcrumbItems(namespacePath: string, hideNamespace = false): BreadcrumbItem[] {
  return [
    ...(!hideNamespace ? [{ label: formatNamespaceCrumb(namespacePath) }] : []),
    { label: "Jobs", current: true }
  ];
}

export function jobDetailBreadcrumbItems({
  jobName,
  namespacePath,
  onJobs,
  hideNamespace = false
}: {
  jobName: string;
  namespacePath: string;
  onJobs: () => void;
  hideNamespace?: boolean;
}): BreadcrumbItem[] {
  return [
    ...(!hideNamespace ? [{ label: formatNamespaceCrumb(namespacePath) }] : []),
    { label: "Jobs", onClick: onJobs },
    { label: jobName, current: true }
  ];
}

export function jobEditorBreadcrumbItems({
  editorJobName,
  mode,
  namespacePath,
  onJob,
  onJobs,
  hideNamespace = false
}: {
  editorJobName: string;
  mode: JobEditorMode;
  namespacePath: string;
  onJob: () => void;
  onJobs: () => void;
  hideNamespace?: boolean;
}): BreadcrumbItem[] {
  const baseItems: BreadcrumbItem[] = [
    ...(!hideNamespace ? [{ label: formatNamespaceCrumb(namespacePath) }] : []),
    { label: "Jobs", onClick: onJobs }
  ];

  if (mode.kind === "create") {
    return [...baseItems, { label: "Create", current: true }];
  }

  return [...baseItems, { label: editorJobName, onClick: onJob }, { label: "Configure", current: true }];
}

export function formatNamespaceCrumb(namespacePath: string) {
  return namespacePath === "/" ? "Root" : namespacePath;
}
