import { useEffect, useRef, useState, type ReactNode } from "react";
import {
  Button,
  CopyValue,
  ErrorAlert,
  FormField,
  ResourceStatus,
  OperationalFact,
  TextAreaField,
  ToggleField
} from "../../components";
import {
  Users,
  UserRound,
  CalendarDays,
  GitCommitHorizontal,
  Wrench,
  RotateCcw,
  CircleCheck,
  Pencil
} from "lucide-react";
import { SelectControl } from "../../components/primitives/SelectControl";
import type { Job } from "../../domain/console";
import { useConsoleAccess } from "../../domain/access";
import { randomRequestID } from "../../api/requestID";
import { navigateTo } from "../../routing/navigation";
import { repositoryJobPath } from "../../routing/repositories";
import {
  createSourceJobClient,
  type SourceJobClient,
  type SourceJobLifecycle,
  type SourceJobChange,
  type SourceHotfixInput
} from "../../data/sourceJobs";
import styles from "./SourceJobDefinition.module.css";
import { DefinitionDiff } from "./DefinitionDiff";
import { formatDefinitionJSON as pretty } from "./DefinitionJSON";
type SourceJobDefinitionProps = {
  job: Job;
  sourceDetails?: (ownership: ReactNode) => ReactNode;
  renderLayout?: (definition: ReactNode, provenance: ReactNode) => ReactNode;
  onConfigure?: () => void;
  onRun?: (input: SourceHotfixInput) => Promise<void> | void;
  onChanged?: (change: SourceJobChange) => void;
  client?: SourceJobClient;
};

export function SourceJobDefinition(props: SourceJobDefinitionProps) {
  return (
    <SourceJobDefinitionContent key={`${props.job.repository}:${props.job.id}:${props.job.namespacePath}`} {...props} />
  );
}

function SourceJobDefinitionContent({
  job,
  sourceDetails,
  renderLayout,
  onConfigure,
  onRun,
  onChanged,
  client: provided
}: SourceJobDefinitionProps) {
  const { can } = useConsoleAccess();
  const [defaultClient] = useState(() => createSourceJobClient(job));
  const client = provided ?? defaultClient;
  const [state, setState] = useState<SourceJobLifecycle>();
  const [choices, setChoices] = useState<{ path: string; label: string }[]>([]);
  const [owner, setOwner] = useState(job.namespacePath);
  const [error, setError] = useState<{ title: string; message: string }>();
  const [pendingAction, setPendingAction] = useState<"assign" | "hotfix" | "clear" | "run" | "reset">();
  const busy = Boolean(pendingAction);
  const [revision, setRevision] = useState(0);
  const [compare, setCompare] = useState(false);
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState("");
  const [reason, setReason] = useState("");
  const [review, setReview] = useState<"changes" | "clear">();
  const [notice, setNotice] = useState("");
  const [confirmRevert, setConfirmRevert] = useState(false);
  const lock = useRef(false);
  const runRequest = useRef<{ signature: string; key: string }>(undefined);
  const reviewHeading = useRef<HTMLHeadingElement>(null);
  const reasonInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    if (editing && !review && !pendingAction) reasonInput.current?.focus();
  }, [editing, review, pendingAction]);

  useEffect(() => {
    if (review) {
      reviewHeading.current?.focus({ preventScroll: true });
      reviewHeading.current?.scrollIntoView?.({ block: "nearest" });
    }
  }, [review]);

  useEffect(() => {
    let ignore = false;
    client
      .load()
      .then(async (row) => {
        const namespaces = !row.registration && can("admin:*") ? await client.namespaces() : [];
        if (!ignore) {
          setState(row);
          setChoices(namespaces);
          setError(undefined);
        }
      })
      .catch((cause) => {
        if (!ignore)
          setError({
            title: "Could not load definition",
            message: cause instanceof Error ? cause.message : "Unable to load job controls."
          });
      });

    return () => {
      ignore = true;
    };
  }, [client, revision, can]);

  const registration = state?.registration;
  const displayedHotfix = state ? registration?.hotfix : job.hotfix;
  const admin = Boolean(registration && can("admin:*", registration.namespace));
  const trigger = Boolean(registration && onRun && can("run:trigger", registration.namespace));
  const effective = pretty(state?.effective_definition);
  const source = pretty(state?.source_json || state?.source_definition);
  const canCompare = Boolean(registration?.hotfix && source && effective);
  const showDiff = canCompare && compare;
  const changeLabel = !admin
    ? "Run with changes"
    : job.canAuthor === false
      ? registration?.hotfix
        ? "Edit"
        : "Hotfix"
      : "Make changes";

  const input = (): SourceHotfixInput => ({
    definition: JSON.parse(draft),
    reason: reason.trim(),
    expected_revision: registration!.revision,
    expected_source_commit: state!.source_commit!
  });

  async function perform(action: "assign" | "hotfix" | "clear" | "run", selectedOwner?: string) {
    if (lock.current) return;

    lock.current = true;
    setPendingAction(action);
    setError(undefined);
    setNotice("");

    try {
      if (action === "assign") {
        const assigned = await client.assign(selectedOwner ?? owner);
        const refreshed = await client.load();

        setState(refreshed);
        onChanged?.({
          registration: assigned,
          definition: refreshed.effective_definition,
          sourceCommit: refreshed.source_commit
        });

        setNotice("Job owner assigned.");
        if (assigned.namespace !== job.namespacePath)
          navigateTo(repositoryJobPath({ repository_id: job.repository, namespace: assigned.namespace }, job.id));
      } else if (action === "run") {
        const request = input();
        const signature = JSON.stringify(request);

        if (runRequest.current?.signature !== signature) runRequest.current = { signature, key: randomRequestID() };

        await onRun?.({ ...request, idempotency_key: runRequest.current.key });
        runRequest.current = undefined;
      } else {
        const request =
          action === "clear"
            ? { expected_revision: registration!.revision, expected_source_commit: state!.source_commit! }
            : input();

        const updated = await (action === "clear" ? client.clear(request) : client.activate(request));
        const refreshed = await client.load();
        setState(refreshed);
        onChanged?.({
          registration: updated,
          definition: refreshed.effective_definition,
          sourceCommit: refreshed.source_commit
        });

        setNotice(action === "clear" ? "Hotfix reverted. Future runs use the source definition." : "");
      }

      setReview(undefined);
      setEditing(false);
      setReason("");
    } catch (cause) {
      const titles = {
        assign: "Could not assign namespace",
        hotfix: "Could not activate hotfix",
        clear: "Could not revert hotfix",
        run: "Could not start run"
      };

      setError({
        title: titles[action],
        message:
          cause instanceof Error
            ? cause.message
            : "The request failed. Reset your changes before retrying if the source changed."
      });

      setReview(undefined);
    } finally {
      lock.current = false;
      setPendingAction(undefined);
      setConfirmRevert(false);
    }
  }

  async function resetChanges() {
    if (lock.current) return;

    lock.current = true;
    setPendingAction("reset");
    setError(undefined);
    setNotice("");

    try {
      const refreshed = await client.load();
      const replacement =
        pretty(refreshed.effective_definition) || pretty(refreshed.source_json || refreshed.source_definition);

      if (!replacement)
        throw new Error(refreshed.source_error || "The current definition is unavailable. Your draft has been kept.");

      const namespaces = !refreshed.registration && can("admin:*") ? await client.namespaces() : [];
      setState(refreshed);
      setChoices(namespaces);
      setDraft(replacement);
      setReason("");
      setReview(undefined);
      setConfirmRevert(false);

      runRequest.current = undefined;
      const refreshedOwner = refreshed.registration;

      setEditing(
        Boolean(
          refreshedOwner &&
          (can("admin:*", refreshedOwner.namespace) || (onRun && can("run:trigger", refreshedOwner.namespace)))
        )
      );

      if (refreshedOwner)
        onChanged?.({
          registration: refreshedOwner,
          definition: refreshed.effective_definition,
          sourceCommit: refreshed.source_commit
        });
    } catch (cause) {
      setError({
        title: "Could not reset changes",
        message: cause instanceof Error ? cause.message : "Your draft has been kept. Try resetting again."
      });
    } finally {
      lock.current = false;
      setPendingAction(undefined);
    }
  }

  function beginReview() {
    try {
      const value: unknown = JSON.parse(draft);
      if (!value || typeof value !== "object" || Array.isArray(value))
        throw new Error("Definition must be a JSON object.");

      if (!reason.trim()) throw new Error("Enter a reason for these changes.");

      setDraft(pretty(value));
      setError(undefined);
      setReview("changes");
    } catch (cause) {
      setError({
        title: "Check your changes",
        message: cause instanceof Error ? cause.message : "Enter a valid JSON definition."
      });
    }
  }

  function beginEditing() {
    setDraft(effective || source);
    setReason("");
    setError(undefined);
    setNotice("");
    setEditing(true);
  }

  const ownership = (
    <OperationalFact
      icon={Users}
      label="Namespace"
      layout="detail"
      wrap
      value={
        !state ? (
          <span className={styles.muted}>{error ? "Unavailable" : "Loading…"}</span>
        ) : registration ? (
          <span className={styles.ownerName}>{registration.namespace === "/" ? "Root" : registration.namespace}</span>
        ) : can("admin:*") ? (
          <div className={styles.ownerControls}>
            <SelectControl
              aria-label="Namespace"
              summaryLabel="Namespace"
              name="sourceJobOwner"
              value={choices.some((row) => row.path === owner) ? owner : choices[0]?.path || ""}
              disabled={busy}
              options={choices.map((row) => ({ value: row.path, label: row.label }))}
              onChange={(event) => setOwner(event.target.value)}
            />
            <Button
              disabled={busy || !choices.length}
              onClick={() => {
                const selected = choices.some((row) => row.path === owner) ? owner : choices[0]?.path;
                if (selected) {
                  setOwner(selected);
                  void perform("assign", selected);
                }
              }}
            >
              Assign owner
            </Button>
          </div>
        ) : (
          <span className={styles.muted}>Unassigned · An instance administrator can assign this job.</span>
        )
      }
    />
  );

  const provenance = sourceDetails ? sourceDetails(ownership) : <dl className={styles.ownership}>{ownership}</dl>;
  const definition = (
    <section
      className={`${styles.panel} polished-panel polished-panel--accent-top`}
      aria-labelledby="job-definition-title"
    >
      <div className={styles.header}>
        <div className={styles.headerSummary}>
          <div className={styles.headingTitle}>
            <h3 id="job-definition-title">Definition</h3>
            <ResourceStatus tone={displayedHotfix ? "warning" : "neutral"}>
              {displayedHotfix ? "Hotfix" : "Source"}
            </ResourceStatus>
          </div>
        </div>
        <div className={styles.viewControls}>
          <fieldset className={styles.compareControl} disabled={!canCompare || busy || editing || Boolean(review)}>
            <ToggleField
              hideLabel
              label="Definition comparison"
              name="compareSource"
              checked={showDiff}
              onText="Diff"
              offText="Effective"
              onChange={setCompare}
            />
          </fieldset>
          <div className={styles.viewActions}>
            {can("admin:*") && onConfigure && job.canAuthor !== false ? (
              <Button
                variant="quiet"
                disabled={busy || editing || Boolean(review)}
                aria-label={`Edit source for ${job.name}`}
                onClick={onConfigure}
              >
                Edit source
              </Button>
            ) : null}
            {effective ? <CopyValue iconOnly label="effective JSON" value={effective} /> : null}
            {(admin || trigger) && !(admin && registration?.hotfix) ? (
              <Button
                variant="quiet"
                disabled={busy || editing || Boolean(review) || !state?.source_commit}
                onClick={beginEditing}
              >
                {changeLabel}
              </Button>
            ) : null}
          </div>
        </div>
      </div>
      <ErrorAlert title={error?.title} message={error?.message} />
      {notice ? (
        <div className={styles.notice} role="status">
          <CircleCheck aria-hidden="true" />
          <p>{notice}</p>
        </div>
      ) : null}
      {!state ? (
        !error ? (
          <p role="status">Loading definition…</p>
        ) : (
          <Button className={styles.retryAction} variant="quiet" onClick={() => setRevision((v) => v + 1)}>
            Retry
          </Button>
        )
      ) : (
        <>
          <div className={styles.workspace}>
            {registration?.hotfix ? (
              <section className={styles.hotfix} aria-labelledby="active-hotfix-title">
                <div className={styles.hotfixHeader}>
                  <div className={styles.hotfixIntroduction} role="status">
                    <h4 id="active-hotfix-title">
                      <Wrench aria-hidden="true" />
                      Active hotfix
                    </h4>
                    <p className={styles.hotfixReason}>{registration.hotfix.reason}</p>
                    <p>New runs use this definition until reverted.</p>
                  </div>
                  {admin ? (
                    <div className={styles.hotfixActions}>
                      <Button
                        className={styles.hotfixAction}
                        variant="quiet"
                        disabled={busy || editing || Boolean(review) || !state.source_commit}
                        onClick={beginEditing}
                      >
                        <Pencil aria-hidden="true" />
                        Edit
                      </Button>
                      <Button
                        className={styles.hotfixAction}
                        variant="quiet"
                        disabled={
                          busy || editing || Boolean(review) || !state.source_definition || !state.source_commit
                        }
                        onClick={() => {
                          setConfirmRevert(false);
                          setReview("clear");
                        }}
                      >
                        <RotateCcw aria-hidden="true" />
                        Revert
                      </Button>
                    </div>
                  ) : null}
                </div>
                <dl className={styles.hotfixMetadata}>
                  <div>
                    <dt>
                      <UserRound aria-hidden="true" />
                      Activated by
                    </dt>
                    <dd>{registration.hotfix.actor}</dd>
                  </div>
                  <div>
                    <dt>
                      <CalendarDays aria-hidden="true" />
                      Activated
                    </dt>
                    <dd>
                      <time dateTime={new Date(registration.hotfix.created_at * 1000).toISOString()}>
                        {new Date(registration.hotfix.created_at * 1000).toLocaleString()}
                      </time>
                    </dd>
                  </div>
                  <div>
                    <dt>
                      <GitCommitHorizontal aria-hidden="true" />
                      Base commit
                    </dt>
                    <dd>
                      <CopyValue label="Hotfix base commit" value={registration.hotfix.base_commit}>
                        <code title={registration.hotfix.base_commit}>
                          {registration.hotfix.base_commit.slice(0, 12)}
                        </code>
                      </CopyValue>
                    </dd>
                  </div>
                </dl>
              </section>
            ) : null}
            <form
              id="job-definition-form"
              className={styles.definitionWorkspace}
              onSubmit={(event) => event.preventDefault()}
              aria-labelledby={review ? "definition-review-title" : "job-definition-title"}
            >
              {state.source_error ? (
                <ErrorAlert title="Upstream Definition Unavailable" message={state.source_error} />
              ) : null}

              <div className={styles.definitionFrame} aria-label="Definition frame">
                {review ? (
                  <div className={styles.reviewContent}>
                    <div className={styles.reviewIntroduction} tabIndex={0} aria-label="Review context">
                      <h4 id="definition-review-title" ref={reviewHeading} tabIndex={-1}>
                        {review === "clear" ? "Revert hotfix" : "Review changes"}
                      </h4>
                      <p>
                        {review === "clear"
                          ? "New runs return to source. Existing runs keep their definitions."
                          : `Reason: ${reason}`}
                      </p>
                    </div>
                    <DefinitionDiff
                      fill
                      before={effective}
                      after={review === "clear" ? pretty(state.source_definition) : draft}
                      beforeLabel="Effective"
                      afterLabel={review === "clear" ? "Source" : "Proposed"}
                    />
                  </div>
                ) : editing ? (
                  <div className={styles.editingFields}>
                    <FormField
                      ref={reasonInput}
                      label="Reason for changes"
                      name="hotfixReason"
                      value={reason}
                      maxLength={512}
                      onChange={(event) => setReason(event.target.value)}
                      disabled={busy}
                      required
                      wide
                    />
                    <TextAreaField
                      code
                      wide
                      label="Definition"
                      name="hotfixDefinition"
                      value={draft}
                      onChange={(event) => setDraft(event.target.value)}
                      disabled={busy}
                      rows={16}
                    />
                  </div>
                ) : showDiff ? (
                  <DefinitionDiff fill before={source} after={effective} beforeLabel="Source" afterLabel="Effective" />
                ) : (
                  <pre
                    className={`definition-preview ${styles.definitionPreview}`}
                    tabIndex={0}
                    aria-label="Effective definition"
                  >
                    {effective || "Definition contents are unavailable."}
                  </pre>
                )}
              </div>
              {editing || review ? (
                <div className={styles.workspaceActions}>
                  {review === "clear" ? (
                    <Button
                      className={styles.confirmRevert}
                      pending={pendingAction === "clear"}
                      pendingLabel="Reverting…"
                      disabled={busy}
                      variant="danger"
                      onClick={() => {
                        if (confirmRevert) void perform("clear");
                        else setConfirmRevert(true);
                      }}
                      onBlur={() => {
                        if (!lock.current) setConfirmRevert(false);
                      }}
                      onKeyDown={(event) => {
                        if (event.key === "Escape") setConfirmRevert(false);
                      }}
                      title={confirmRevert ? "Click again to revert. Escape keeps the hotfix." : undefined}
                    >
                      {confirmRevert ? "Confirm revert" : "Revert"}
                    </Button>
                  ) : review ? (
                    <>
                      {admin ? (
                        <Button
                          pending={pendingAction === "hotfix"}
                          pendingLabel="Activating…"
                          disabled={busy}
                          onClick={() => void perform("hotfix")}
                        >
                          Activate
                        </Button>
                      ) : null}
                      {trigger ? (
                        <Button
                          variant={admin ? "quiet" : "primary"}
                          pending={pendingAction === "run"}
                          pendingLabel="Starting…"
                          disabled={busy}
                          onClick={() => void perform("run")}
                        >
                          Run once
                        </Button>
                      ) : null}
                    </>
                  ) : editing ? (
                    <>
                      <Button disabled={busy} onClick={beginReview}>
                        Review
                      </Button>
                      <Button
                        variant="quiet"
                        pending={pendingAction === "reset"}
                        disabled={busy}
                        onClick={() => void resetChanges()}
                        title="Discard local changes and reload the effective definition"
                      >
                        Reset
                      </Button>
                    </>
                  ) : null}
                  {editing || review ? (
                    <Button
                      variant="quiet"
                      disabled={busy}
                      onClick={() => {
                        setConfirmRevert(false);
                        setError(undefined);
                        if (review) setReview(undefined);
                        else setEditing(false);
                      }}
                    >
                      Cancel
                    </Button>
                  ) : null}
                </div>
              ) : null}
            </form>
          </div>
        </>
      )}
      {!renderLayout ? (
        <section className={styles.provenance} aria-label="Provenance">
          <h4>Provenance</h4>
          {provenance}
        </section>
      ) : null}
    </section>
  );
  return renderLayout ? renderLayout(definition, provenance) : definition;
}
