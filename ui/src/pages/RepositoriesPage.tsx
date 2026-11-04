import { useEffect, useMemo, useRef, useState } from "react";
import {
  type LucideIcon,
  FileCode,
  FolderGit2,
  GitBranch,
  GitCommitHorizontal,
  RotateCw,
  RefreshCw
} from "lucide-react";
import {
  AppState,
  BreadcrumbTrail,
  Button,
  CopyValue,
  DataTable,
  ErrorAlert,
  NamespacePicker,
  OperationalFact,
  PageHeader,
  ResourceStatus
} from "../components";
import { VectisAPIError } from "../api/client";
import { useConsoleAccess } from "../domain/access";
import type { Namespace } from "../domain/console";
import {
  createRepositorySource,
  type Repository,
  type RepositoryJobs,
  type RepositorySource,
  type RepositoryStatus,
  type RepositorySync
} from "../data/repositories";
import { navigateTo } from "../routing/navigation";
import styles from "./RepositoriesPage.module.css";

import { formatNamespaceCrumb } from "./jobs/JobBreadcrumbs";
import { InlineEmptyState } from "./shared/InlineEmptyState";
import { RepositoryAccess } from "./repositories/RepositoryAccess";
import { repositoryConnectionPath, repositoryPath, repositoryJobPath } from "../routing/repositories";

const message = (error: unknown) => (error instanceof Error ? error.message : "Unable to load repository information.");
const timestamp = (value?: number) =>
  value
    ? new Date(value * 1000).toLocaleString(undefined, { dateStyle: "medium", timeStyle: "short" })
    : "Not recorded";

export function RepositoriesPage({
  repositoryID,
  namespacePath = "/",
  source: providedSource,
  onNamespaceChange,
  management = false,
  sharedWorkspace = false
}: {
  repositoryID?: string;
  namespacePath?: string;
  source?: RepositorySource;
  onNamespaceChange?: (namespace: string) => void;
  management?: boolean;
  sharedWorkspace?: boolean;
}) {
  const source = useMemo(
    () => providedSource ?? createRepositorySource(namespacePath, management),
    [providedSource, namespacePath, management]
  );

  const { ready, error, can } = useConsoleAccess();
  if (error) return <AppState title="Unable to Load Permissions" description={error} tone="error" />;

  if (!ready)
    return (
      <>
        <PageHeader title="Repositories" description="Source repositories and definition discovery." />
        <AppState title="Loading Permissions" tone="loading" />
      </>
    );

  if (management && !can("admin:*"))
    return (
      <AppState
        title="Connection Administration Required"
        description="Repository connections are managed by instance administrators."
        tone="error"
      />
    );

  if (!can("job:read"))
    return (
      <AppState
        title="Repository Access Required"
        description="Your account cannot inspect source repositories."
        tone="error"
      />
    );

  return repositoryID ? (
    <RepositoryDetail
      key={repositoryID}
      id={repositoryID}
      source={source}
      onNamespaceChange={onNamespaceChange}
      management={management}
      sharedWorkspace={sharedWorkspace}
    />
  ) : (
    <RepositoryIndex
      namespacePath={namespacePath}
      source={source}
      onNamespaceChange={onNamespaceChange}
      management={management}
      sharedWorkspace={sharedWorkspace}
    />
  );
}

function RepositoryIndex({
  namespacePath,
  source,
  onNamespaceChange,
  management = false,
  sharedWorkspace = false
}: {
  management?: boolean;
  sharedWorkspace?: boolean;
  namespacePath: string;
  source: RepositorySource;
  onNamespaceChange?: (namespace: string) => void;
}) {
  const { can } = useConsoleAccess();
  const [namespaces, setNamespaces] = useState<Namespace[] | null>(null);
  const [namespaceError, setNamespaceError] = useState("");
  const [revision, setRevision] = useState(0);
  const [refreshing, setRefreshing] = useState(false);
  const refreshLock = useRef(false);
  const [result, setResult] = useState<{ namespace: string; rows?: Repository[]; error?: string }>();

  useEffect(() => {
    let ignore = false;
    source
      .namespaces()
      .then((rows) => {
        if (!ignore) {
          setNamespaces(rows);
          setNamespaceError("");

          const readable = rows.filter((row) => can("job:read", row.path));
          const selected =
            readable.find((row) => row.path === namespacePath) ?? (namespacePath === "/" ? readable[0] : undefined);

          if (selected) onNamespaceChange?.(selected.path);
        }
      })
      .catch((error) => {
        if (!ignore) {
          setNamespaceError(message(error));
          refreshLock.current = false;
          setRefreshing(false);
        }
      });

    return () => {
      ignore = true;
    };
  }, [source, revision, can, namespacePath, onNamespaceChange]);

  const allowed = (namespaces ?? []).filter((row) => can("job:read", row.path));
  const selected =
    allowed.find((row) => row.path === namespacePath)?.path ?? (namespacePath === "/" ? allowed[0]?.path : undefined);

  useEffect(() => {
    if (!selected) return;
    let ignore = false;

    source
      .list(selected)
      .then((rows) => {
        if (!ignore) setResult({ namespace: selected, rows });
      })
      .catch((error) => {
        if (!ignore)
          setResult((previous) => ({
            namespace: selected,
            rows: previous?.namespace === selected ? previous.rows : undefined,
            error: message(error)
          }));
      })
      .finally(() => {
        if (!ignore) {
          refreshLock.current = false;
          setRefreshing(false);
        }
      });

    return () => {
      ignore = true;
    };
  }, [source, selected, revision]);

  const current = result?.namespace === selected ? result : undefined;
  return (
    <>
      <PageHeader
        title={management ? "Repository connections" : "Repositories"}
        description={
          management
            ? "Shared connections and the namespaces allowed to use them."
            : "Repositories available in this namespace."
        }
        navigation={
          <BreadcrumbTrail
            items={[
              ...(!sharedWorkspace
                ? [{ label: management ? "Instance" : formatNamespaceCrumb(selected ?? namespacePath) }]
                : management
                  ? [{ label: "Instance" }]
                  : []),
              { label: management ? "Repository connections" : "Repositories", current: true }
            ]}
            label="Repository location"
          />
        }
        actions={
          <>
            {!management && !sharedWorkspace && allowed.length > 0 ? (
              <NamespacePicker
                compact
                namespaces={allowed}
                value={selected ?? namespacePath}
                onChange={(namespace) => {
                  onNamespaceChange?.(namespace);
                  navigateTo(`/repositories?${new URLSearchParams({ namespace })}`);
                }}
              />
            ) : null}
            <Button
              variant="quiet"
              disabled={!current && !namespaceError}
              pending={refreshing}
              pendingLabel="Refreshing…"
              onClick={() => {
                if (refreshLock.current) return;
                refreshLock.current = true;
                setRefreshing(true);
                setRevision((value) => value + 1);
              }}
            >
              <span className={styles.controlLabel}>
                <RotateCw aria-hidden="true" />
                Refresh
              </span>
            </Button>
          </>
        }
      />
      {namespaceError ? (
        <ErrorAlert title="Unable to Load Namespaces" message={namespaceError} />
      ) : namespaces === null ? (
        <AppState title="Loading Repositories" tone="loading" />
      ) : !selected ? (
        <AppState
          title="Repository Access Required"
          description="Choose a namespace where you can read jobs."
          tone="error"
        />
      ) : (
        <>
          {current?.error ? <ErrorAlert title="Unable to Load Repositories" message={current.error} /> : null}
          {!current ? (
            <AppState title="Loading Repositories" tone="loading" />
          ) : current.rows ? (
            <DataTable
              rows={current.rows}
              getRowKey={(row) => row.repository_id}
              getRowActionLabel={(row) => `Inspect ${row.repository_id}`}
              onRowClick={(row) =>
                navigateTo(
                  management ? repositoryConnectionPath(row.repository_id) : repositoryPath(row.repository_id, selected)
                )
              }
              emptyMessage={
                management ? "No repository connections registered." : "No repositories available in this namespace."
              }
              columns={[
                {
                  header: "Repository",
                  width: "32%",
                  cell: (row) => (
                    <div className={styles.identity}>
                      <strong>{row.repository_id}</strong>
                      <small className={styles.branch}>
                        <GitBranch aria-hidden="true" />
                        {row.default_ref || "HEAD"}
                      </small>
                    </div>
                  )
                },
                ...(management
                  ? [
                      {
                        header: "Used by",
                        width: "180px",
                        cell: (row: Repository) => (
                          <span className={styles.namespaceSummary}>
                            {row.namespaces?.length
                              ? row.namespaces.map((path) => (path === "/" ? "Root" : path)).join(", ")
                              : "No namespaces"}
                          </span>
                        )
                      }
                    ]
                  : []),
                {
                  header: "Source",
                  width: "132px",
                  hideOnMobile: true,
                  cell: (row) => sourceLabel(row.source_kind, row.checkout_mode)
                },
                {
                  header: "State",
                  width: "96px",
                  cell: (row) => (
                    <ResourceStatus tone={row.enabled ? "enabled" : "disabled"}>
                      {row.enabled ? "Enabled" : "Disabled"}
                    </ResourceStatus>
                  )
                },
                { header: "Last sync", width: "112px", cell: (row) => <SyncStatus sync={row.sync} /> },
                {
                  header: "Completed",
                  width: "164px",
                  hideOnMobile: true,
                  cell: (row) => <CompletedAt value={row.sync?.last_finished_at_unix} />
                }
              ]}
            />
          ) : null}
        </>
      )}
    </>
  );
}

function RepositoryDetail({
  id,
  source,
  onNamespaceChange,
  management = false,
  sharedWorkspace = false
}: {
  management?: boolean;
  sharedWorkspace?: boolean;
  id: string;
  source: RepositorySource;
  onNamespaceChange?: (namespace: string) => void;
}) {
  const { can } = useConsoleAccess();
  const [repository, setRepository] = useState<Repository>();
  const [status, setStatus] = useState<RepositoryStatus>();
  const [jobs, setJobs] = useState<RepositoryJobs>();
  const [error, setError] = useState("");
  const [syncError, setSyncError] = useState("");
  const [statusError, setStatusError] = useState("");
  const [jobsError, setJobsError] = useState("");
  const [missing, setMissing] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [syncing, setSyncing] = useState(false);
  const [notice, setNotice] = useState("");
  const [more, setMore] = useState(false);
  const [revision, setRevision] = useState(0);
  const syncLock = useRef(false);
  const pageLock = useRef(false);
  const generation = useRef(0);

  useEffect(() => {
    let ignore = false;
    const current = ++generation.current;

    async function load() {
      try {
        const row = await source.get(id);
        if (ignore) return;

        setRepository(row);
        onNamespaceChange?.(row.namespace);
        setError("");
        setMissing(false);

        await Promise.allSettled([
          source
            .status(id)
            .then((value) => {
              if (!ignore) {
                setStatus(value);
                setStatusError("");
              }
            })
            .catch((cause) => {
              if (!ignore) setStatusError(message(cause));
            }),
          row.enabled && !management
            ? source
                .jobs(id)
                .then((value) => {
                  if (!ignore) {
                    setJobs(value);
                    setJobsError("");
                  }
                })
                .catch((cause) => {
                  if (!ignore) setJobsError(message(cause));
                })
            : Promise.resolve().then(() => {
                if (!ignore) {
                  setJobs(undefined);
                  setJobsError("");
                }
              })
        ]);
      } catch (cause) {
        if (!ignore) {
          setError(message(cause));
          setMissing(cause instanceof VectisAPIError && cause.status === 404);
        }
      } finally {
        if (!ignore && current === generation.current) setRefreshing(false);
      }
    }

    void load();

    return () => {
      ignore = true;
      generation.current = current + 1;
    };
  }, [id, source, revision, onNamespaceChange, management]);

  useEffect(() => {
    if (repository?.sync?.status !== "running") return;

    let ignore = false;
    let pending = false;

    async function poll() {
      if (pending || document.visibilityState !== "visible") return;
      pending = true;

      try {
        const row = await source.get(id);
        if (!ignore) {
          setRepository(row);

          if (row.sync?.status !== "running") {
            setRefreshing(true);
            setRevision((value) => value + 1);
          }
        }
      } catch (cause) {
        if (!ignore) setError(message(cause));
      } finally {
        pending = false;
      }
    }

    const timer = window.setTimeout(() => void poll(), 3000);
    const visible = () => {
      if (document.visibilityState === "visible") void poll();
    };

    document.addEventListener("visibilitychange", visible);
    return () => {
      ignore = true;
      window.clearTimeout(timer);
      document.removeEventListener("visibilitychange", visible);
    };
  }, [repository, source, id]);

  async function sync() {
    if (syncLock.current || !repository || !can("admin:*")) return;

    syncLock.current = true;
    setSyncing(true);
    setSyncError("");
    setNotice("");

    try {
      const row = await source.sync(id);

      setRepository(row);
      setNotice(
        row.sync?.status === "running"
          ? "Sync is in progress."
          : row.sync?.status === "failed"
            ? "Sync failed. Inspect the sync error below."
            : "Sync completed."
      );
    } catch (cause) {
      setSyncError(message(cause));
    } finally {
      syncLock.current = false;
      setSyncing(false);
      setRefreshing(true);
      setRevision((value) => value + 1);
    }
  }

  async function loadMore() {
    if (pageLock.current || !jobs?.next_cursor) return;

    pageLock.current = true;
    setMore(true);
    setJobsError("");

    const current = generation.current;

    try {
      const next = await source.jobs(id, jobs.next_cursor, jobs.resolved_commit);
      if (current !== generation.current) return;
      if (next.resolved_commit !== jobs.resolved_commit)
        throw new Error("Repository revision changed. Refresh to reload its definitions.");

      setJobs({
        ...next,
        jobs: [...jobs.jobs, ...next.jobs],
        invalid: [...(jobs.invalid ?? []), ...(next.invalid ?? [])]
      });
    } catch (cause) {
      if (current === generation.current) setJobsError(message(cause));
    } finally {
      pageLock.current = false;
      setMore(false);
    }
  }

  const currentCommit = jobs?.resolved_commit || status?.resolved_commit || repository?.sync?.commit;
  const currentRef = jobs?.requested_ref || repository?.default_ref || status?.default_ref || "HEAD";
  const checkoutAdvanced = Boolean(
    jobs?.resolved_commit && status?.resolved_commit && jobs.resolved_commit !== status.resolved_commit
  );

  const syncCommitDiffers = Boolean(
    repository?.sync?.status === "failed" &&
    repository.sync.commit &&
    repository.sync.commit !== currentCommit &&
    repository.sync.commit !== status?.resolved_commit
  );

  const back = () =>
    navigateTo(
      management
        ? "/manage/repositories"
        : `/repositories${repository ? `?${new URLSearchParams({ namespace: repository.namespace })}` : ""}`
    );

  return (
    <>
      <PageHeader
        title={id}
        description={
          management
            ? "Shared source, synchronization, and namespace access."
            : "Source diagnostics and discovered job definitions."
        }
        navigation={
          <BreadcrumbTrail
            items={[
              ...(!sharedWorkspace && repository
                ? [{ label: management ? "Instance" : formatNamespaceCrumb(repository.namespace) }]
                : management
                  ? [{ label: "Instance" }]
                  : []),
              { label: management ? "Repository connections" : "Repositories", onClick: back },
              { label: id, current: true }
            ]}
            label="Repository location"
          />
        }
        actions={
          <>
            <Button
              variant="quiet"
              disabled={syncing || more}
              pending={refreshing}
              pendingLabel="Refreshing…"
              onClick={() => {
                setRefreshing(true);
                setRevision((value) => value + 1);
              }}
            >
              <span className={styles.controlLabel}>
                <RotateCw aria-hidden="true" />
                Refresh
              </span>
            </Button>
            {repository && can("admin:*") ? (
              <Button
                disabled={refreshing || more || repository.sync?.status === "running"}
                pending={syncing}
                pendingLabel="Syncing…"
                onClick={() => void sync()}
              >
                <span className={styles.controlLabel}>
                  <RefreshCw aria-hidden="true" />
                  Sync now
                </span>
              </Button>
            ) : null}
          </>
        }
      />
      <ErrorAlert title={missing ? "Repository Not Found" : "Repository Request Failed"} message={error} />
      {!repository ? (
        !error ? (
          <AppState title="Loading Repository" tone="loading" />
        ) : null
      ) : (
        <div className={styles.layout}>
          <section
            className={`${styles.panel} polished-panel polished-panel--accent-top`}
            aria-labelledby="repository-source-title"
          >
            <div className={styles.panelHeading}>
              <h2 id="repository-source-title">Source</h2>
              <ResourceStatus
                tone={
                  !repository.enabled
                    ? "disabled"
                    : status?.status === "ok"
                      ? "success"
                      : status || statusError
                        ? "warning"
                        : "neutral"
                }
              >
                {!repository.enabled
                  ? "Disabled"
                  : statusError
                    ? "Status unavailable"
                    : status?.status === "ok"
                      ? "Available"
                      : status
                        ? "Unavailable"
                        : "Checking…"}
              </ResourceStatus>
            </div>
            <dl className={`${styles.facts} ${styles.sourceFacts}`}>
              <Fact icon={FolderGit2} label="Repository ID" value={id} copy />
              <Fact icon={GitBranch} label="Ref" value={currentRef} />
              <Fact
                icon={FileCode}
                label="Checkout"
                value={sourceLabel(repository.source_kind, repository.checkout_mode)}
              />
              <Fact
                icon={GitCommitHorizontal}
                label="Revision"
                value={currentCommit || "Not available"}
                copy={Boolean(currentCommit)}
                wide
              />
              {repository.canonical_url ? (
                <Fact icon={FolderGit2} label="Remote" value={repository.canonical_url} wide />
              ) : null}
            </dl>
            <div className={styles.syncSummary}>
              <span>Last sync</span>
              <SyncStatus sync={repository.sync} />
              {repository.sync?.last_finished_at_unix ? (
                <span>{timestamp(repository.sync.last_finished_at_unix)}</span>
              ) : repository.sync?.last_started_at_unix ? (
                <span>Started {timestamp(repository.sync.last_started_at_unix)}</span>
              ) : null}
              <span className={styles.registration}>
                {repository.declared ? "Configuration file" : "API registration"}
              </span>
              {notice ? <span role="status">{notice}</span> : null}
            </div>
            <ErrorAlert title="Sync Failed" message={syncError || repository.sync?.error} />
            <ErrorAlert title="Checkout Unavailable" message={statusError || status?.error?.message} />
            {checkoutAdvanced ? (
              <div className={styles.revisionNotice} role="status">
                <ResourceStatus tone="warning">Checkout changed</ResourceStatus>
                <p>Jobs use the revision shown above. Refresh to load definitions from the current checkout.</p>
                <CopyValue appearance="field" label="Current checkout revision" value={status!.resolved_commit!}>
                  <code>{status!.resolved_commit}</code>
                </CopyValue>
              </div>
            ) : null}
            {syncCommitDiffers ? (
              <div className={styles.revisionNotice}>
                <p>The failed sync recorded a different revision.</p>
                <CopyValue appearance="field" label="Failed sync revision" value={repository.sync.commit!}>
                  <code>{repository.sync.commit}</code>
                </CopyValue>
              </div>
            ) : null}
            {repository.default_ref && repository.default_ref !== currentRef ? (
              <p className={styles.hint}>
                Configured default ref: <code>{repository.default_ref}</code>
              </p>
            ) : null}
            {repository.checkout_mode !== "managed" ? (
              <p className={styles.hint}>Local checkout: update it on disk before syncing.</p>
            ) : null}
          </section>
          {management ? (
            <RepositoryAccess id={id} source={source} />
          ) : (
            <>
              {" "}
              <section
                className={`${styles.panel} ${styles.full} polished-panel polished-panel--accent-top`}
                aria-labelledby="repository-jobs-title"
              >
                <div className={styles.panelHeading}>
                  <div className={styles.sectionCopy}>
                    <h2 id="repository-jobs-title">Discovered jobs</h2>
                    <p>Definitions found in this repository.</p>
                  </div>
                  {jobs ? (
                    <span className={styles.discoveryCount}>
                      {jobs.jobs.length}
                      {jobs.truncated ? "+" : ""} valid{jobs.invalid?.length ? ` · ${jobs.invalid.length} invalid` : ""}
                    </span>
                  ) : null}
                </div>
                <ErrorAlert title="Unable to Load Definitions" message={jobsError} />
                {!repository.enabled ? (
                  <p>Job discovery is unavailable while this repository is disabled.</p>
                ) : jobs ? (
                  <>
                    {jobs.jobs.length > 0 ? (
                      <DataTable
                        variant="inset"
                        rows={jobs.jobs}
                        getRowKey={(job) => job.path}
                        getRowActionLabel={(job) => `View ${job.job_id}`}
                        onRowClick={(job) => navigateTo(repositoryJobPath(repository, job.job_id))}
                        emptyMessage="No valid job definitions discovered."
                        columns={[
                          { header: "Job", width: "34%", cell: (job) => <strong>{job.job_id}</strong> },
                          { header: "Definition file", cell: (job) => <code className={styles.wrap}>{job.path}</code> }
                        ]}
                      />
                    ) : (
                      <InlineEmptyState>No valid job definitions discovered.</InlineEmptyState>
                    )}
                    {(jobs.invalid?.length ?? 0) > 0 ? (
                      <>
                        <div className={styles.validationHeading}>
                          <ResourceStatus tone="warning">{jobs.invalid?.length} invalid</ResourceStatus>
                          <div className={styles.sectionCopy}>
                            <h3>Invalid definitions</h3>
                            <p>Correct these files in source control, then sync and refresh.</p>
                          </div>
                        </div>
                        <DataTable
                          variant="inset"
                          rows={jobs.invalid ?? []}
                          getRowKey={(row) => row.path}
                          columns={[
                            {
                              header: "File",
                              width: "34%",
                              cell: (row) => <code className={styles.wrap}>{row.path}</code>
                            },
                            {
                              header: "Validation error",
                              cell: (row) => <span className={styles.wrap}>{row.error}</span>
                            }
                          ]}
                        />
                      </>
                    ) : null}
                    {jobs.truncated ? (
                      jobs.next_cursor ? (
                        <Button
                          variant="quiet"
                          pending={more}
                          pendingLabel="Loading…"
                          disabled={refreshing || syncing}
                          onClick={() => void loadMore()}
                        >
                          Load more definitions
                        </Button>
                      ) : (
                        <p>
                          Definition discovery was truncated. Narrow the repository import scope to inspect the
                          remaining files.
                        </p>
                      )
                    ) : null}
                  </>
                ) : !jobsError ? (
                  <p role="status">Discovering job definitions…</p>
                ) : null}
              </section>
            </>
          )}
        </div>
      )}
    </>
  );
}

function CompletedAt({ value }: { value?: number }) {
  if (!value) return <>Not recorded</>;

  const date = new Date(value * 1000);
  return (
    <time className={styles.completed} dateTime={date.toISOString()} title={date.toISOString()}>
      <span>{date.toLocaleDateString(undefined, { dateStyle: "medium" })}</span>
      <small>{date.toLocaleTimeString(undefined, { timeStyle: "short" })}</small>
    </time>
  );
}

function sourceLabel(kind: string, checkoutMode?: string) {
  if (checkoutMode === "managed") return "Managed Git";
  return kind === "local_checkout" ? "Local checkout" : kind === "managed_git" ? "Managed Git" : kind;
}
function SyncStatus({ sync }: { sync?: RepositorySync }) {
  const status = sync?.status || "never";

  return (
    <ResourceStatus
      tone={
        status === "succeeded" ? "success" : status === "failed" ? "danger" : status === "running" ? "info" : "neutral"
      }
    >
      {status === "succeeded"
        ? "Succeeded"
        : status === "failed"
          ? "Failed"
          : status === "running"
            ? "Running"
            : status === "never"
              ? "Never synced"
              : status}
    </ResourceStatus>
  );
}

function Fact({
  icon,
  label,
  value,
  copy = false,
  wide = false
}: {
  icon: LucideIcon;
  label: string;
  value: string;
  copy?: boolean;
  wide?: boolean;
}) {
  return (
    <OperationalFact
      className={`${styles.fact} ${copy || wide ? styles.wideFact : ""}`}
      icon={icon}
      label={label}
      layout="detail"
      wrap
      value={
        copy ? (
          <CopyValue appearance="field" label={label} value={value}>
            <code>{value}</code>
          </CopyValue>
        ) : (
          <span className={styles.wrap}>{value}</span>
        )
      }
    />
  );
}
