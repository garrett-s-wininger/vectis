export const repositoryPath = (id: string, namespace?: string) =>
  `/repositories/${encodeURIComponent(id)}${namespace ? `?${new URLSearchParams({ namespace })}` : ""}`;
export const repositoryConnectionPath = (id: string) => `/manage/repositories/${encodeURIComponent(id)}`;
export const repositoryJobPath = (repository: { repository_id: string; namespace: string }, jobID: string) =>
  `/jobs/${encodeURIComponent(jobID)}?${new URLSearchParams({ repository: repository.repository_id, namespace: repository.namespace })}`;
