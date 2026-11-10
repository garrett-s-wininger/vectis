import { LoaderCircle } from "lucide-react";
import { useEffect, useRef, useState, type FormEvent } from "react";
import {
  AppState,
  BreadcrumbTrail,
  Button,
  CopyValue,
  ErrorAlert,
  FormField,
  PageHeader,
  ResourceStatus,
  SelectField,
  ToggleField
} from "../../components";
import type {
  NewRepository,
  Repository,
  RepositoryJobs,
  RepositorySettings,
  RepositorySource,
  RepositoryValidation
} from "../../data/repositories";
import type { Namespace } from "../../domain/console";
import { navigateTo } from "../../routing/navigation";
import { repositoryConnectionEditorPath, repositoryConnectionPath, repositoryPath } from "../../routing/repositories";
import { RepositoryImpactReview } from "./RepositoryImpactReview";
import styles from "./RepositoryConnectionEditor.module.css";
type Values = {
  id: string;
  url: string;
  ref: string;
  credential: string;
  namespace: string;
  enabled: boolean;
};

type Errors = Partial<Record<"id" | "url" | "ref" | "credential" | "namespace", string>>;
const message = (cause: unknown) => (cause instanceof Error ? cause.message : "The connection request failed.");

function fieldErrors(values: Values, remoteRequired: boolean): Errors {
  const errors: Errors = {};
  if (!/^[a-zA-Z0-9][a-zA-Z0-9._-]*$/.test(values.id.trim()))
    errors.id = "Use letters, numbers, dots, underscores, or hyphens, starting with a letter or number.";

  const scpRemote = /^[A-Za-z0-9][A-Za-z0-9._-]*@(?:[A-Za-z0-9.-]+|\[[0-9a-fA-F:]+\]):[^\s]+$/.test(values.url.trim());
  if (!scpRemote && (values.url.trim() || remoteRequired)) {
    try {
      const url = new URL(values.url.trim());
      if (!["https:", "http:", "ssh:", "git:"].includes(url.protocol) || !url.hostname)
        errors.url = "Use an HTTPS, HTTP, SSH, or Git repository URL.";

      if (url.password || (["http:", "https:"].includes(url.protocol) && url.username))
        errors.url = "Use a credential reference instead of credentials in the URL.";
    } catch {
      errors.url = "Enter a repository URL, such as https://github.com/owner/repository.git.";
    }
  }

  if (!values.ref.trim()) errors.ref = "Enter a branch, tag, commit, or HEAD.";
  if (!values.namespace) errors.namespace = "Select a namespace.";

  return errors;
}

export function RepositoryConnectionEditor({
  id,
  namespacePath,
  source
}: {
  id?: string;
  namespacePath: string;
  source: RepositorySource;
}) {
  const [values, setValues] = useState<Values>({
    id: id ?? "",
    url: "",
    ref: "HEAD",
    credential: "",
    namespace: "",
    enabled: true
  });

  const [namespaces, setNamespaces] = useState<Namespace[]>([]);
  const [credentials, setCredentials] = useState<string[]>([]);
  const [credentialError, setCredentialError] = useState("");
  const [credentialsLoading, setCredentialsLoading] = useState(true);
  const [original, setOriginal] = useState<Repository>();
  const [saved, setSaved] = useState<Repository>();
  const [jobs, setJobs] = useState<RepositoryJobs>();
  const [phase, setPhase] = useState<"idle" | "running" | "ready" | "failed" | "disabled">("idle");
  const [errors, setErrors] = useState<Errors>({});
  const [loadError, setLoadError] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);
  const [busy, setBusy] = useState(false);
  const [manualValidating, setManualValidating] = useState(false);
  const [preview, setPreview] = useState<RepositoryValidation>();
  const [previewCurrent, setPreviewCurrent] = useState(false);
  const [reload, setReload] = useState(0);
  const [impactRevision, setImpactRevision] = useState("");
  const [impactReload, setImpactReload] = useState(0);
  const form = useRef<HTMLFormElement>(null);
  const settingsHeading = useRef<HTMLHeadingElement>(null);
  const validationHeading = useRef<HTMLHeadingElement>(null);
  const previewPanel = useRef<HTMLElement>(null);
  const active = useRef(true);
  const lock = useRef(false);

  useEffect(() => {
    let ignore = false;
    source
      .credentials()
      .then((refs) => {
        if (!ignore) {
          setCredentials(refs);
          setCredentialError("");
        }
      })
      .catch(() => {
        if (!ignore) setCredentialError("Unable to load saved credentials. Refresh to try again.");
      })
      .finally(() => {
        if (!ignore) setCredentialsLoading(false);
      });

    return () => {
      ignore = true;
    };
  }, [source]);

  useEffect(() => {
    const invalid = form.current?.querySelector<HTMLElement>('[aria-invalid="true"]');
    const target =
      invalid?.tagName === "SELECT" ? (invalid.parentElement?.querySelector("summary") ?? invalid) : invalid;
    target?.focus();
  }, [errors]);

  useEffect(() => {
    if (error && !saved) {
      previewPanel.current?.focus();
      previewPanel.current?.scrollIntoView?.({ block: "nearest" });
    }
  }, [error, saved]);

  const savedID = saved?.repository_id;
  useEffect(() => {
    if (savedID) validationHeading.current?.focus({ preventScroll: true });
  }, [savedID]);

  useEffect(() => {
    const panel = validationHeading.current?.closest("section");
    if (savedID && panel?.contains(document.activeElement)) panel.scrollIntoView?.({ block: "nearest" });
  }, [savedID, phase]);

  useEffect(() => {
    let ignore = false;
    active.current = true;

    Promise.all([
      source.namespaces(),
      id ? source.get(id) : Promise.resolve(undefined),
      id ? source.access(id) : Promise.resolve([])
    ])
      .then(([rows, row, grants]) => {
        if (ignore) return;
        setNamespaces(rows);
        setOriginal(row);
        setPreview(undefined);
        setPreviewCurrent(false);

        const paths = grants.map((grant) => grant.namespace);
        const namespace = id
          ? (paths.find((path) => path === namespacePath) ?? paths[0] ?? "")
          : (rows.find((item) => item.path === namespacePath)?.path ?? rows[0]?.path ?? "");

        setValues({
          id: id ?? "",
          url: row?.canonical_url ?? "",
          ref: row?.default_ref || "HEAD",
          credential: row?.credential_ref ?? "",
          namespace,
          enabled: row?.enabled ?? true
        });

        setLoadError("");
      })
      .catch((cause) => {
        if (!ignore) setLoadError(message(cause));
      })
      .finally(() => {
        if (!ignore) setLoading(false);
      });

    return () => {
      ignore = true;
      active.current = false;
    };
  }, [id, namespacePath, reload, source]);

  function change(next: Partial<Values>) {
    setPreviewCurrent(false);
    setError("");
    setValues((current) => ({ ...current, ...next }));
    setErrors({});
  }

  async function validate(row: Repository) {
    setPhase("running");
    setJobs(undefined);

    const synced = await source.sync(row.repository_id);
    if (!active.current) return;

    setSaved(synced);

    if (synced.sync.status === "running") return;
    if (synced.sync.status !== "succeeded")
      throw new Error(
        synced.sync.error || "Synchronization failed. Check the repository URL, ref, and authentication."
      );

    const status = await source.status(row.repository_id);
    if (!active.current) return;
    if (status.status !== "ok" || !status.default_ref_resolved)
      throw new Error(status.error?.message || "The selected ref could not be resolved.");

    const listing = await source.jobs(row.repository_id);

    if (!active.current) return;
    setJobs(listing);
    setPhase("ready");
  }

  const disabling = Boolean(id && original?.enabled && !values.enabled);
  const validatingConnection = manualValidating || (busy && !saved && values.enabled && !previewCurrent);
  function checkFields() {
    const nextErrors = fieldErrors(values, !id || original?.checkout_mode === "managed");
    if (id) {
      delete nextErrors.namespace;
      delete nextErrors.id;

      if (original?.checkout_mode === "external") delete nextErrors.url;
    }

    setErrors(nextErrors);
    return Object.keys(nextErrors).length === 0;
  }

  function proposedSettings(): RepositorySettings {
    return {
      canonical_url: values.url.trim(),
      default_ref: values.ref.trim(),
      credential_ref: values.credential.trim(),
      enabled: values.enabled
    };
  }

  function newConnection(settings: RepositorySettings): NewRepository {
    return {
      ...settings,
      repository_id: values.id.trim(),
      namespace: values.namespace,
      source_kind: "local_checkout",
      checkout_mode: "managed",
      authoring_mode: "read_only"
    };
  }

  async function previewValidation() {
    if (lock.current || saved || !checkFields()) return;

    lock.current = true;
    setBusy(true);
    setManualValidating(true);
    setPreviewCurrent(false);
    setError("");

    try {
      const settings = proposedSettings();
      const result = await source.validate(id ? settings : newConnection(settings), id);
      if (active.current) {
        setPreview(result);
        setPreviewCurrent(true);
      }
    } catch (cause) {
      if (active.current) setError(message(cause));
    } finally {
      lock.current = false;

      if (active.current) {
        setBusy(false);
        setManualValidating(false);
      }
    }
  }

  async function submit(event: FormEvent) {
    event.preventDefault();
    if (lock.current || saved) return;
    if (disabling && !impactRevision) {
      setError("Review the affected jobs before disabling this connection.");
      return;
    }

    if (!checkFields()) return;

    lock.current = true;
    setBusy(true);
    setError("");

    const input: RepositorySettings = {
      ...proposedSettings(),
      ...(values.enabled && !previewCurrent ? { validate_connection: true } : {}),
      ...(disabling ? { impact_revision: impactRevision } : {})
    };

    try {
      const row = id ? await source.update(id, input) : await source.create(newConnection(input));
      if (!active.current) return;

      setSaved(row);

      if (row.enabled) await validate(row);
      else setPhase("disabled");
    } catch (cause) {
      if (active.current) {
        setError(message(cause));
        setPhase("failed");

        if (disabling) {
          setImpactRevision("");
          setImpactReload((value) => value + 1);
        }
      }
    } finally {
      lock.current = false;
      if (active.current) setBusy(false);
    }
  }

  async function retry() {
    if (lock.current || !saved) return;

    lock.current = true;
    setBusy(true);
    setError("");

    try {
      await validate(saved);
    } catch (cause) {
      if (active.current) {
        setError(message(cause));
        setPhase("failed");
      }
    } finally {
      lock.current = false;
      if (active.current) setBusy(false);
    }
  }

  async function viewJobs() {
    if (!saved || lock.current) return;

    lock.current = true;
    setBusy(true);
    setError("");

    try {
      const grants = await source.access(saved.repository_id);
      if (!active.current) return;

      const namespace = grants.find((grant) => grant.namespace === values.namespace)?.namespace ?? grants[0]?.namespace;
      if (!namespace) throw new Error("Grant a namespace access on the connection page before viewing its jobs.");

      navigateTo(repositoryPath(saved.repository_id, namespace));
    } catch (cause) {
      if (active.current) setError(message(cause));
    } finally {
      lock.current = false;
      if (active.current) setBusy(false);
    }
  }

  function editSaved() {
    if (!saved) return;
    if (id !== saved.repository_id) {
      navigateTo(repositoryConnectionEditorPath(saved.repository_id));
      return;
    }

    setPreview(undefined);
    setPreviewCurrent(false);
    setOriginal(saved);
    setSaved(undefined);
    setJobs(undefined);
    setError("");
    setPhase("idle");
    settingsHeading.current?.focus();
  }

  const back = () => navigateTo(id ? repositoryConnectionPath(id) : "/manage/repositories");
  return (
    <>
      <PageHeader
        title={id ? "Edit connection" : "Add connection"}
        description="Connect a Git repository, validate its checkout, and make its jobs available to a namespace."
        navigation={
          <BreadcrumbTrail
            label="Repository location"
            items={[
              { label: "Repository connections", onClick: () => navigateTo("/manage/repositories") },
              ...(id ? [{ label: id, onClick: back }] : []),
              { label: id ? "Edit connection" : "Add connection", current: true }
            ]}
          />
        }
      />
      {loading ? (
        <AppState title="Loading Connection Settings" tone="loading" />
      ) : loadError ? (
        <div className={styles.layout}>
          <ErrorAlert title="Unable to Load Connection" message={loadError} />
          <Button
            variant="quiet"
            onClick={() => {
              setLoading(true);
              setReload((value) => value + 1);
            }}
          >
            Retry loading
          </Button>
        </div>
      ) : original?.declared ? (
        <section className={`${styles.panel} polished-panel`}>
          <h2>Managed by configuration</h2>
          <p>
            This connection is declared in the server configuration. Update its settings there so they remain consistent
            after a restart.
          </p>
          <Button variant="quiet" onClick={back}>
            View connection
          </Button>
        </section>
      ) : (
        <div className={styles.layout}>
          <form
            ref={form}
            className={`${styles.panel} polished-panel polished-panel--accent-top`}
            onSubmit={(event) => void submit(event)}
          >
            <div className={styles.heading}>
              <div>
                <h2 ref={settingsHeading} tabIndex={-1}>
                  Connection settings
                </h2>
                <p>Choose where jobs come from and who can use them.</p>
              </div>
              <fieldset className={styles.enabledControl} disabled={busy || Boolean(saved)}>
                <ToggleField
                  hideLabel
                  label="Connection enabled"
                  name="enabled"
                  checked={values.enabled}
                  onText="Enabled"
                  offText="Disabled"
                  onChange={(checked) => {
                    setImpactRevision("");
                    change({ enabled: checked });
                  }}
                />
              </fieldset>
            </div>
            <fieldset className={styles.fields} disabled={busy || Boolean(saved)}>
              <div className={styles.fullWidth}>
                <FormField
                  label="Repository URL"
                  name="url"
                  value={values.url}
                  readOnly={original?.checkout_mode === "external"}
                  error={errors.url}
                  onChange={(event) => change({ url: event.target.value })}
                  wide
                />
              </div>
              <div className={styles.fieldGroup}>
                <FormField
                  label="Repository ID"
                  name="id"
                  value={values.id}
                  readOnly={Boolean(id)}
                  error={errors.id}
                  onChange={(event) => change({ id: event.target.value })}
                  wide
                />
                <p className={styles.hint}>A unique name used in job links and repository references.</p>
              </div>
              <div className={styles.fieldGroup}>
                <FormField
                  label="Default ref"
                  name="ref"
                  value={values.ref}
                  error={errors.ref}
                  onChange={(event) => change({ ref: event.target.value })}
                  wide
                />
                <p className={styles.hint}>
                  Use HEAD for the repository's default branch, or choose a branch, tag, or commit.
                </p>
              </div>
              <div className={`${styles.fieldGroup} ${id ? styles.fullWidth : ""}`}>
                <SelectField
                  searchable
                  label="Credential"
                  disabled={busy || Boolean(saved) || credentialsLoading}
                  name="credential"
                  value={values.credential}
                  error={errors.credential || credentialError}
                  options={[
                    { value: "", label: "None — Public access" },
                    ...Array.from(new Set([...credentials, ...(values.credential ? [values.credential] : [])]))
                      .sort()
                      .map((ref) => ({
                        value: ref,
                        label: `${ref.replace(/^[^:]+:\/\//, "")}${!credentialsLoading && !credentials.includes(ref) ? " (current)" : ""}`,
                        description: ref.startsWith("encryptedfs:") ? "Encrypted filesystem" : ref.split(":")[0]
                      }))
                  ]}
                  onChange={(event) => change({ credential: event.target.value })}
                  wide
                />
                <p className={styles.hint}>
                  {credentialsLoading
                    ? "Loading saved credentials…"
                    : credentials.length
                      ? "Search saved credentials by name or provider, or choose None for public access."
                      : "No saved credentials are available. Public repositories can use None."}
                </p>
              </div>
              {!id ? (
                <div className={styles.fieldGroup}>
                  <SelectField
                    wide
                    label="Initial namespace access"
                    name="namespace"
                    value={values.namespace}
                    disabled={busy || Boolean(saved)}
                    error={errors.namespace}
                    options={namespaces.map((row) => ({
                      value: row.path,
                      label: row.path === "/" ? "Root" : row.path
                    }))}
                    onChange={(event) => change({ namespace: event.target.value })}
                  />
                  <p className={styles.hint}>
                    This namespace can use the connection. Manage additional grants on the connection page.
                  </p>
                </div>
              ) : null}
            </fieldset>
            {!saved ? (
              <section
                className={styles.validationResult}
                ref={previewPanel}
                tabIndex={-1}
                aria-label="Connection validation"
                aria-busy={validatingConnection}
              >
                <div
                  className={styles.validationContent}
                  aria-live="polite"
                  inert={validatingConnection}
                  tabIndex={0}
                  role="group"
                  aria-label="Validation details"
                >
                  <ResourceStatus tone={error ? "danger" : previewCurrent ? "success" : "neutral"}>
                    {error
                      ? "Validation failed"
                      : previewCurrent
                        ? "Connection valid"
                        : preview
                          ? "Validation needed"
                          : "Not validated"}
                  </ResourceStatus>
                  {error ? (
                    <ErrorAlert title="Connection Request Failed" message={error} />
                  ) : (
                    <p>
                      {preview ? (
                        <>
                          {" "}
                          {previewCurrent ? "Ref" : "Previously checked ref"} <code>{preview.requested_ref}</code>{" "}
                          {previewCurrent ? "resolves to:" : "resolved to:"}
                        </>
                      ) : (
                        "Validate to check Git access and resolve the selected ref."
                      )}
                    </p>
                  )}
                  {preview ? (
                    <CopyValue appearance="field" label="Validated revision" value={preview.resolved_commit}>
                      <code>{preview.resolved_commit}</code>
                    </CopyValue>
                  ) : (
                    <div className={styles.revisionPlaceholder}>No revision checked yet.</div>
                  )}
                </div>
                {validatingConnection ? (
                  <div className={styles.validationOverlay} role="status">
                    <LoaderCircle aria-hidden="true" /> Validating connection…
                  </div>
                ) : null}
              </section>
            ) : null}
            {id && disabling && !saved ? (
              <RepositoryImpactReview key={impactReload} id={id} source={source} onReviewed={setImpactRevision} />
            ) : null}
            {!saved ? (
              <div className={styles.actions}>
                <Button
                  type="submit"
                  disabled={busy || (disabling && !impactRevision)}
                  pending={busy && !manualValidating}
                  pendingLabel="Saving…"
                >
                  Save
                </Button>
                <Button
                  variant="quiet"
                  disabled={busy}
                  pending={manualValidating}
                  pendingLabel="Validating…"
                  onClick={() => void previewValidation()}
                >
                  {preview ? "Revalidate" : "Validate connection"}
                </Button>
                <Button variant="quiet" disabled={busy} onClick={back}>
                  Cancel
                </Button>
              </div>
            ) : null}
          </form>
          {saved ? (
            <section
              className={`${styles.panel} polished-panel polished-panel--accent-top`}
              aria-labelledby="connection-validation-title"
            >
              <div className={styles.heading}>
                <h2 id="connection-validation-title" ref={validationHeading} tabIndex={-1}>
                  Connection saved
                </h2>
                <span role="status">
                  <ResourceStatus tone={phase === "ready" ? "success" : phase === "failed" ? "danger" : "neutral"}>
                    {phase === "ready"
                      ? "Validated"
                      : phase === "failed"
                        ? "Validation failed"
                        : phase === "disabled"
                          ? "Disabled"
                          : phase === "running"
                            ? "Validation in progress"
                            : "Saved"}
                  </ResourceStatus>
                </span>
              </div>
              <ErrorAlert title="Validation Failed" message={error} />
              {phase === "running" ? (
                <p>Sync is in progress. Check again or inspect the connection's status.</p>
              ) : phase === "disabled" ? (
                <p>The connection is disabled. Enable it before validating or running jobs.</p>
              ) : phase === "failed" ? (
                <p>
                  The connection was saved. Correct its settings or retry validation; this will reuse the existing
                  connection.
                </p>
              ) : jobs ? (
                <>
                  <p>
                    {jobs.jobs.length}
                    {jobs.truncated ? "+" : ""}{" "}
                    {jobs.jobs.length === 1 && !jobs.truncated ? "job definition" : "job definitions"} discovered.
                  </p>
                  {jobs.invalid?.length ? (
                    <p>{jobs.invalid.length} definition paths need attention. Inspect the repository for details.</p>
                  ) : null}
                  {!jobs.jobs.length && !jobs.invalid?.length ? (
                    <p>
                      Add <code>.vectis.json</code> for CI or <code>.vectis/&lt;name&gt;.json</code> for named jobs,
                      commit them, and sync again.
                    </p>
                  ) : null}
                </>
              ) : null}
              <div className={styles.actions}>
                {phase === "ready" ? (
                  <Button disabled={busy} onClick={() => void viewJobs()}>
                    View jobs
                  </Button>
                ) : saved.enabled ? (
                  <Button pending={busy} pendingLabel="Validating…" onClick={() => void retry()}>
                    {phase === "running" ? "Check again" : "Retry validation"}
                  </Button>
                ) : null}
                <Button
                  variant="quiet"
                  disabled={busy}
                  onClick={() => navigateTo(repositoryConnectionPath(saved.repository_id))}
                >
                  Manage connection
                </Button>
                <Button variant="quiet" disabled={busy} onClick={editSaved}>
                  Edit settings
                </Button>
              </div>
            </section>
          ) : null}
        </div>
      )}
    </>
  );
}
