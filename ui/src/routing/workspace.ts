export function workspacePath(path: string, namespace: string) {
  const url = new URL(path, window.location.origin);
  url.searchParams.set("namespace", namespace);

  return `${url.pathname}${url.search}${url.hash}`;
}

export function isWorkspaceRoute(kind: string) {
  return ["jobs", "runs", "repositories"].includes(kind);
}
