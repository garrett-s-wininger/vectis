import { WorkspacePicker } from "./components/navigation/WorkspacePicker";
import { workspacePath, isWorkspaceRoute } from "./routing/workspace";
import {
  ConsoleAccessProvider,
  hasAccess,
  namespaceAccessLabel,
  useConsoleAccess,
  type ConsoleAccess
} from "./domain/access";
import type { FormEvent, MouseEvent, ReactNode } from "react";
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import "./index.css";
import vectisLogo from "../../assets/brand/public/vectis.png";
import vectisDarkLogo from "../../assets/brand/public/vectis-dark.png";
import { completeSetup, loadUIContext, login, logout } from "./api/auth";
import { AppShell, AppState, Button, FormError, FormField } from "./components";
import { VectisAPIError } from "./api/client";
import { createConsoleDataSource, type CreatedUserCredential } from "./data/consoleDataSource";
import { watchRun } from "./data/watchRun";
import type { LoadRunArtifacts } from "./data/runArtifacts";
import type {
  Cell,
  Job,
  NewJob,
  NewNamespace,
  NewUser,
  RoleBindingRole,
  UpdateJob,
  UpdateNamespace,
  User,
  UserStatus
} from "./domain/console";
import { canDeleteMockNamespace, nextMockRunID, type MockConsoleData } from "./mocks/consoleData";
import { DashboardPage } from "./pages/DashboardPage";
import { RepositoriesPage } from "./pages/RepositoriesPage";
import { HealthPage } from "./pages/HealthPage";
import { JobsPage } from "./pages/JobsPage";
import { NamespacesPage } from "./pages/NamespacesPage";
import { ProfilePage } from "./pages/ProfilePage";
import { useResolvedTheme } from "./theme";
import { NotFoundPage } from "./pages/NotFoundPage";
import { returnToRunList, runListNamespace, useNavigationScroll } from "./routing/navigation";
import { runListFiltersPath } from "./routing/runListFilters";
import { runInvestigationPath } from "./routing/runInvestigation";
import { RunDetailPage } from "./pages/RunDetailPage";
import { RunsPage } from "./pages/RunsPage";
import { UsersPage } from "./pages/UsersPage";
import { ActionAlertRail, PageMissingState, PageStateRail } from "./pages/shared";
import {
  managementNavItems,
  navigateTo,
  primaryNavItems,
  routeFromPath,
  safeNextPath,
  safeReturnPath,
  type AppRoute
} from "./routing/routes";

type SetupValues = {
  adminPassword: string;
  adminUsername: string;
  bootstrapToken: string;
};

type LoginValues = {
  password: string;
  username: string;
};

export function App() {
  const [route, setRoute] = useState<AppRoute>(() => routeFromPath(window.location.pathname, window.location.search));

  const [setupValues, setSetupValues] = useState<SetupValues>({
    bootstrapToken: "",
    adminUsername: "admin",
    adminPassword: ""
  });

  const [loginValues, setLoginValues] = useState<LoginValues>({
    username: "",
    password: ""
  });

  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState("");
  const [docsURL, setDocsURL] = useState<string | undefined>(undefined);
  const [accountName, setAccountName] = useState("admin");
  const [authEnabled, setAuthEnabled] = useState(true);
  const [consoleDataSource] = useState(createConsoleDataSource);
  const consoleDataSourceRef = useRef(consoleDataSource);
  const [access, setAccess] = useState<ConsoleAccess | null>(() => consoleDataSource.initialAccess ?? null);
  const [accessError, setAccessError] = useState("");
  const loadArtifacts = useCallback<LoadRunArtifacts>(
    (runID, query) => consoleDataSourceRef.current.loadRunArtifacts(runID, query),
    []
  );

  const [consoleData, setConsoleData] = useState<MockConsoleData | null>(null);
  const [consoleError, setConsoleError] = useState("");
  const [actionError, setActionError] = useState("");
  const [healthCells, setHealthCells] = useState<Cell[] | null>(null);
  const [healthError, setHealthError] = useState("");
  const [missingRunID, setMissingRunID] = useState<string | null>(null);
  const [jobDefinitionError, setJobDefinitionError] = useState<{
    id: string;
    repository: string;
    message: string;
  } | null>(null);

  const definitionJobID =
    route.kind === "jobs"
      ? (route.jobID ?? (route.jobEditor?.kind === "edit" ? route.jobEditor.jobID : undefined))
      : undefined;
  const definitionJob = consoleData?.jobs.find((job) => job.id === definitionJobID && jobMatchesRoute(job, route));
  const [selectedNamespacePath, setSelectedNamespacePath] = useState(route.workspaceNamespace ?? "/");
  const runJobs = consoleData?.jobs;
  const runCacheRef = useRef<MockConsoleData["runs"]>([]);

  useEffect(() => {
    runCacheRef.current = consoleData?.runs ?? [];
  }, [consoleData?.runs]);

  const workspaceNamespaces = access?.namespaces["*"]
    ? (consoleData?.namespaces ?? consoleDataSource.initialNamespaces ?? []).map((row) => row.path)
    : Object.entries(access?.namespaces ?? {})
        .filter(([, actions]) => actions.includes("job:read") || actions.includes("run:read"))
        .map(([path]) => path)
        .sort();

  const workspaceActive = isWorkspaceRoute(route.kind) && !route.repositoryManagement;
  const activeWorkspace = workspaceNamespaces.includes(selectedNamespacePath)
    ? selectedNamespacePath
    : (workspaceNamespaces[0] ?? selectedNamespacePath);

  function changeWorkspace(namespace: string) {
    setSelectedNamespacePath(namespace);
    const index = route.kind === "repositories" ? "/repositories" : route.kind === "runs" ? "/runs" : "/jobs";
    const target =
      route.runID || route.jobID || route.jobEditor || route.repositoryID
        ? index
        : `${window.location.pathname}${window.location.search}${window.location.hash}`;

    navigateTo(workspacePath(target, namespace), { namespace, scroll: { top: 0, left: 0 } });
  }

  const shellActive = route.kind !== "login" && route.kind !== "setup";
  const consoleActive =
    route.kind !== "login" &&
    route.kind !== "setup" &&
    route.kind !== "health" &&
    route.kind !== "profile" &&
    route.kind !== "repositories";

  useLayoutEffect(() => {
    if (
      workspaceActive &&
      access &&
      workspaceNamespaces.length &&
      !route.workspaceNamespace &&
      !route.jobID &&
      !route.jobEditor &&
      !route.runID &&
      !route.repositoryID
    ) {
      navigateTo(
        workspacePath(`${window.location.pathname}${window.location.search}${window.location.hash}`, activeWorkspace),
        { replace: true, namespace: activeWorkspace }
      );
    }
  }, [workspaceActive, access, workspaceNamespaces.length, route, activeWorkspace]);

  useEffect(() => {
    const onPopState = () => {
      const nextRoute = routeFromPath(window.location.pathname, window.location.search);
      setActionError("");
      setFormError("");
      setSubmitting(false);
      setRoute(nextRoute);

      if (nextRoute.workspaceNamespace) setSelectedNamespacePath(nextRoute.workspaceNamespace);

      if (consoleData) {
        const routeNamespacePath = namespacePathForRoute(consoleData, nextRoute);
        if (routeNamespacePath) {
          setSelectedNamespacePath(routeNamespacePath);
        }
      }
    };

    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [consoleData]);

  useEffect(() => {
    if (!shellActive) return;
    let ignore = false;
    consoleDataSourceRef.current
      .loadAccess()
      .then((permissions) => {
        if (!ignore) {
          setAccess(permissions);
          setAccessError("");
        }
      })
      .catch((error: unknown) => {
        if (!ignore) {
          setAccess(null);
          setAccessError(errorMessage(error, "Unable to load permissions."));
        }
      });
    return () => {
      ignore = true;
    };
  }, [shellActive]);

  useEffect(() => {
    if (!shellActive) return;
    let ignore = false;

    loadUIContext()
      .then((context) => {
        if (ignore) {
          return;
        }

        setAuthEnabled(context.auth_enabled);
        setAccountName(context.principal.username);
        setDocsURL(context.docs_url);
      })
      .catch(() => {
        // NOTE(garrett): Dev servers may not expose UI context; keep the shell defaults.
      });

    return () => {
      ignore = true;
    };
  }, [shellActive]);

  useEffect(() => {
    let ignore = false;

    if (!consoleActive) {
      return () => {
        ignore = true;
      };
    }

    consoleDataSourceRef.current
      .loadConsole()
      .then((data) => {
        if (!ignore) {
          setConsoleData(data);
          const currentRoute = routeFromPath(window.location.pathname, window.location.search);
          const routeNamespacePath = namespacePathForRoute(data, currentRoute);

          setSelectedNamespacePath(
            (current) =>
              routeNamespacePath ??
              (data.namespaces.some((namespace) => namespace.path === current)
                ? current
                : (data.namespaces[0]?.path ?? "/"))
          );

          setConsoleError("");
          setActionError("");
        }
      })
      .catch((error: unknown) => {
        if (!ignore) {
          setConsoleError(errorMessage(error, "Unable to load console data."));
        }
      });

    return () => {
      ignore = true;
    };
  }, [consoleActive]);

  useEffect(() => {
    if (route.kind !== "health" || !hasAccess(access, "admin:*")) {
      return;
    }

    let ignore = false;

    consoleDataSourceRef.current
      .loadCells()
      .then((cells) => {
        if (!ignore) {
          setHealthCells(cells);
          setHealthError("");
        }
      })
      .catch((error: unknown) => {
        if (!ignore) {
          setHealthError(errorMessage(error, "Unable to load cell status."));
        }
      });

    return () => {
      ignore = true;
    };
  }, [route.kind, access]);

  useEffect(() => {
    if (!definitionJob || definitionJob.definition !== undefined) return;
    let ignore = false;
    consoleDataSourceRef.current
      .loadJob(definitionJob)
      .then((loaded) => {
        if (ignore) return;
        setJobDefinitionError(null);
        setConsoleData((data) =>
          data ? { ...data, jobs: data.jobs.map((job) => (job === definitionJob ? loaded : job)) } : data
        );
      })
      .catch((error: unknown) => {
        if (!ignore)
          setJobDefinitionError({
            id: definitionJob.id,
            repository: definitionJob.repository,
            message: errorMessage(error, "Unable to load job definition.")
          });
      });
    return () => {
      ignore = true;
    };
  }, [definitionJob]);

  useEffect(() => {
    if (route.kind !== "runs" || !route.runID || !runJobs) {
      return;
    }

    const runID = route.runID;
    return watchRun(
      () => consoleDataSourceRef.current.loadRun(runID, runJobs),
      (run) => {
        setConsoleData((data) => {
          if (!data) {
            return data;
          }

          if (data.runs.some((candidate) => candidate.id === run.id)) {
            return {
              ...data,
              runs: data.runs.map((candidate) => (candidate.id === run.id ? run : candidate))
            };
          }

          return {
            ...data,
            runs: [run, ...data.runs]
          };
        });

        if (run.namespacePath) setSelectedNamespacePath(run.namespacePath);
        setMissingRunID(null);
        setConsoleError("");
      },
      (error) => {
        const message = errorMessage(error, "Unable to load run.");
        if ((error instanceof VectisAPIError && error.code === "run_not_found") || message === "Run not found.") {
          setMissingRunID(runID);
          return false;
        }

        setConsoleError(message);
        return true;
      },
      runCacheRef.current.find((run) => run.id === runID)
    );
  }, [runJobs, route.kind, route.runID]);

  async function handleSetup(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setFormError("");

    try {
      const response = await completeSetup({
        bootstrap_token: setupValues.bootstrapToken,
        admin_username: setupValues.adminUsername,
        admin_password: setupValues.adminPassword
      });

      setAccountName(response.username);
      setAuthEnabled(true);
      navigateAfterAuth();
    } catch (error) {
      setFormError(errorMessage(error, "Setup failed"));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleLogin(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setFormError("");

    try {
      const response = await login({
        username: loginValues.username,
        password: loginValues.password
      });

      setAccountName(response.username);
      setAuthEnabled(true);
      navigateAfterAuth();
    } catch (error) {
      setFormError(errorMessage(error, "Login failed"));
    } finally {
      setSubmitting(false);
    }
  }

  async function handleCreateUser(input: NewUser) {
    try {
      const result = await consoleDataSourceRef.current.createUser(input);
      setConsoleData((data) => (data ? { ...data, users: result.users } : data));
      setConsoleError("");
      setActionError("");

      return result.credential;
    } catch (error) {
      setActionError(errorMessage(error, "Unable to create user."));
      throw error;
    }
  }

  async function handleUpdateUserStatus(userID: string, status: UserStatus) {
    try {
      const users = await consoleDataSourceRef.current.updateUserStatus(userID, status);
      setConsoleData((data) => withUpdatedUsers(data, users));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to update user."));
    }
  }

  async function handleDeleteUser(userID: string) {
    try {
      const users = await consoleDataSourceRef.current.deleteUser(userID);
      setConsoleData((data) => withUpdatedUsers(data, users));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to delete user."));
      throw error;
    }
  }

  async function refreshAccess() {
    try {
      setAccess(await consoleDataSourceRef.current.loadAccess());
      setAccessError("");
    } catch (error) {
      setAccess(null);
      setAccessError(errorMessage(error, "Unable to refresh permissions."));
    }
  }

  async function handleGrantRoleBinding(
    userID: string,
    namespaceID: number,
    role: RoleBindingRole,
    previousRole?: RoleBindingRole
  ) {
    try {
      setConsoleData(await consoleDataSourceRef.current.grantRoleBinding(userID, namespaceID, role, previousRole));
      await refreshAccess();
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(
        errorMessage(error, previousRole ? "Unable to update role binding." : "Unable to grant role binding.")
      );
      throw error;
    }
  }

  async function handleRevokeRoleBinding(userID: string, namespaceID: number, role: RoleBindingRole) {
    try {
      setConsoleData(await consoleDataSourceRef.current.revokeRoleBinding(userID, namespaceID, role));
      await refreshAccess();
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to revoke role binding."));
      throw error;
    }
  }

  async function handleCreateNamespace(input: NewNamespace) {
    try {
      setConsoleData(await consoleDataSourceRef.current.createNamespace(input));
      await refreshAccess();
      setConsoleError("");
      setActionError("");
    } catch (error) {
      const message = errorMessage(error, "Unable to create namespace.");
      setActionError(message);
      throw new Error(message, { cause: error });
    }
  }

  async function handleCreateJob(input: NewJob) {
    try {
      setConsoleData(await consoleDataSourceRef.current.createJob(input));
      setSelectedNamespacePath(input.namespacePath);
      setConsoleError("");
      setActionError("");
    } catch (error) {
      const message = errorMessage(error, "Unable to create job.");
      setActionError(message);
      throw new Error(message, { cause: error });
    }
  }

  async function handleUpdateJob(jobID: string, input: UpdateJob) {
    try {
      setConsoleData(await consoleDataSourceRef.current.updateJob(jobID, input));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      const message = errorMessage(error, "Unable to update job.");
      setActionError(message);
      throw new Error(message, { cause: error });
    }
  }

  async function handleDeleteNamespace(namespaceID: number) {
    const namespacePath = consoleData?.namespaces.find((namespace) => namespace.id === namespaceID)?.path;
    const shouldReturnToNamespaces =
      route.kind === "namespaces" && (route.namespaceID === namespaceID || route.workspaceNamespace === namespacePath);

    try {
      setConsoleData(await consoleDataSourceRef.current.deleteNamespace(namespaceID));
      await refreshAccess();
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to delete namespace."));
      return;
    }

    if (namespacePath === selectedNamespacePath) {
      setSelectedNamespacePath("/");
    }

    if (shouldReturnToNamespaces) {
      navigateTo("/namespaces");
    }
  }

  async function handleUpdateNamespace(namespaceID: number, input: UpdateNamespace) {
    try {
      if (!consoleDataSourceRef.current.updateNamespace) {
        consoleDataSourceRef.current = createConsoleDataSource();
      }

      setConsoleData(await consoleDataSourceRef.current.updateNamespace(namespaceID, input));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      const message = errorMessage(error, "Unable to update namespace.");
      setActionError(message);

      throw new Error(message, { cause: error });
    }
  }

  async function handleTriggerRun(jobID: string) {
    try {
      const repositoryID = consoleData?.jobs.find((job) => job.id === jobID && jobMatchesRoute(job, route))?.repository;
      const runID = await consoleDataSourceRef.current.triggerRun(
        jobID,
        repositoryID,
        route.workspaceNamespace ?? selectedNamespacePath
      );

      setConsoleData(
        (data) =>
          data && {
            ...data,
            jobs: data.jobs.map((job) =>
              job.id === jobID && jobMatchesRoute(job, route)
                ? { ...job, lastRunStatus: "queued", nextRun: "Queued" }
                : job
            )
          }
      );

      setConsoleError("");
      setActionError("");
      navigateTo(`/runs/${encodeURIComponent(runID)}`);
    } catch (error) {
      setActionError(errorMessage(error, "Unable to trigger job."));
    }
  }

  async function handleCancelRun(runID: string) {
    await consoleDataSourceRef.current.cancelRun(runID);
    setConsoleData(
      (data) =>
        data && { ...data, runs: data.runs.map((run) => (run.id === runID ? { ...run, cancelRequested: true } : run)) }
    );
  }

  async function handleReplayRun(runID: string, idempotencyKey: string) {
    const newRunID = await consoleDataSourceRef.current.replayRun(runID, idempotencyKey);
    const run = await consoleDataSourceRef.current.loadRun(newRunID, consoleData?.jobs).catch(() => undefined);

    if (run) {
      setConsoleData((data) =>
        data ? { ...data, runs: [run, ...data.runs.filter((candidate) => candidate.id !== run.id)] } : data
      );
    }

    setConsoleError("");
    setActionError("");
    navigateTo(`/runs/${encodeURIComponent(newRunID)}`);
  }

  async function handleOpenRun(runID: string) {
    const fromPath = window.location.pathname;

    try {
      const cached = consoleData?.runs.find((run) => run.id === runID);
      if (!cached?.tasks) {
        const run = await consoleDataSourceRef.current.loadRun(runID, consoleData?.jobs);
        setConsoleData((data) =>
          data ? { ...data, runs: [run, ...data.runs.filter((candidate) => candidate.id !== run.id)] } : data
        );
      }

      if (window.location.pathname !== fromPath) return;
      setConsoleError("");
      setActionError("");
      navigateTo(`/runs/${encodeURIComponent(runID)}`);
    } catch (error) {
      if (window.location.pathname === fromPath) setActionError(errorMessage(error, "Unable to open run."));
    }
  }

  async function handleSubmitEphemeralRun(definition: string) {
    const fallbackRunID = consoleData ? nextMockRunID(consoleData) : null;

    try {
      const nextData = await consoleDataSourceRef.current.submitEphemeralRun({
        definition,
        namespacePath: selectedNamespacePath,
        submittedBy: accountName
      });
      const runID = nextData.runs[0]?.id ?? fallbackRunID;

      setConsoleData(nextData);
      setConsoleError("");
      setActionError("");

      if (runID) {
        navigateTo(`/runs/${runID}`, { namespace: runListNamespace() ?? selectedNamespacePath });
      }
    } catch (error) {
      throw new Error(errorMessage(error, "Unable to submit run."), { cause: error });
    }
  }

  function handleShellNavigate(href: string, event: MouseEvent<HTMLAnchorElement>) {
    if (
      event.defaultPrevented ||
      event.button !== 0 ||
      event.altKey ||
      event.ctrlKey ||
      event.metaKey ||
      event.shiftKey
    ) {
      return;
    }

    event.preventDefault();

    if (new URL(href, window.location.origin).pathname === "/runs" && route.kind === "runs" && route.runID)
      returnToRunList();
    else navigateTo(href);
  }

  function clearAccountState() {
    setConsoleData(null);
    setHealthCells(null);
    setHealthError("");
    setAccess(null);
    setAccessError("");
    setConsoleError("");
    setActionError("");
    setMissingRunID(null);
    setJobDefinitionError(null);
    setSelectedNamespacePath("/");
    setLoginValues({ username: "", password: "" });
    runCacheRef.current = [];
  }

  async function signOut() {
    try {
      await logout();
    } finally {
      clearAccountState();
      navigateTo("/login");
    }
  }

  async function handleChangePassword(currentPassword: string, newPassword: string) {
    await consoleDataSourceRef.current.changePassword(currentPassword, newPassword);
    try {
      await logout();
    } catch {
      /* The API has already revoked this session and its tokens. */
    }
    clearAccountState();
    navigateTo("/login?password_changed=1");
  }

  if (route.kind === "setup") {
    return (
      <SetupPage
        error={formError}
        onChange={setSetupValues}
        onSubmit={handleSetup}
        submitting={submitting}
        values={setupValues}
      />
    );
  }

  if (route.kind === "login") {
    return (
      <LoginPage
        error={formError}
        onChange={setLoginValues}
        onSubmit={handleLogin}
        submitting={submitting}
        values={loginValues}
      />
    );
  }

  return (
    <ConsoleAccessProvider access={access} error={accessError}>
      <AppShell
        activeHref={route.activeHref}
        accountDetail={authEnabled ? undefined : "Auth disabled"}
        accountName={accountName}
        brand="Vectis"
        docsURL={docsURL}
        homeHref={workspacePath("/jobs", activeWorkspace)}
        contextLabel={workspaceActive ? activeWorkspace : route.kind === "profile" ? "My account" : "Instance"}
        workspaceControl={
          <WorkspacePicker
            namespaces={workspaceNamespaces}
            accessLabel={namespaceAccessLabel(access, activeWorkspace)}
            namespaceHref={
              hasAccess(access, "job:read", activeWorkspace) ? workspacePath("/namespaces", activeWorkspace) : undefined
            }
            namespaceManagementHref={
              hasAccess(access, "admin:*") ||
              Object.values(access?.namespaces ?? {}).some((actions) => actions.includes("admin:*"))
                ? "/namespaces"
                : undefined
            }
            onNavigate={handleShellNavigate}
            value={activeWorkspace}
            disabled={!access}
            onChange={changeWorkspace}
          />
        }
        navItems={primaryNavItems.map((item) =>
          "href" in item ? { ...item, href: workspacePath(item.href, activeWorkspace) } : item
        )}
        onNavigate={handleShellNavigate}
        onSignOut={authEnabled ? signOut : undefined}
        utilityNavItems={managementNavItems
          .map((group) =>
            "items" in group
              ? {
                  ...group,
                  items: group.items.filter((item) =>
                    item.href === "/health" || item.href === "/manage/repositories"
                      ? hasAccess(access, "admin:*")
                      : item.href === "/users"
                        ? hasAccess(access, "user:admin")
                        : hasAccess(access, "job:read")
                  )
                }
              : group
          )
          .filter((group) => !("items" in group) || group.items.length > 0)}
      >
        <RouteContent
          loadArtifacts={loadArtifacts}
          actionError={actionError}
          consoleData={consoleData}
          consoleError={consoleError}
          jobDefinitionError={
            jobDefinitionError &&
            jobDefinitionError.id === definitionJobID &&
            jobDefinitionError.repository === definitionJob?.repository
              ? jobDefinitionError.message
              : ""
          }
          healthCells={healthCells}
          healthError={healthError}
          loadingHealthCells={route.kind === "health" && healthCells === null && !healthError}
          missingRunID={missingRunID}
          namespacePath={activeWorkspace}
          onChangePassword={authEnabled ? handleChangePassword : undefined}
          onCreateJob={handleCreateJob}
          onCreateNamespace={handleCreateNamespace}
          onCreateUser={handleCreateUser}
          onDeleteNamespace={handleDeleteNamespace}
          onDeleteUser={handleDeleteUser}
          onGrantRoleBinding={handleGrantRoleBinding}
          onRevokeRoleBinding={handleRevokeRoleBinding}
          onSubmitEphemeralRun={handleSubmitEphemeralRun}
          onTriggerRun={handleTriggerRun}
          onCancelRun={handleCancelRun}
          onReplayRun={handleReplayRun}
          onOpenRun={handleOpenRun}
          onUpdateJob={handleUpdateJob}
          onUpdateNamespace={handleUpdateNamespace}
          onUpdateUserStatus={handleUpdateUserStatus}
          onSelectNamespace={setSelectedNamespacePath}
          route={route}
        />
      </AppShell>
    </ConsoleAccessProvider>
  );
}

function SetupPage({
  error,
  onChange,
  onSubmit,
  submitting,
  values
}: {
  error: string;
  onChange: (values: SetupValues) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  submitting: boolean;
  values: SetupValues;
}) {
  return (
    <main className="app-frame app-frame--centered">
      <section className="auth-panel" aria-labelledby="setup-title">
        <AuthHeader subtitle="Vectis Console" title="Complete setup" titleID="setup-title" />
        <form className="auth-form" onSubmit={onSubmit}>
          <FormField
            autoComplete="off"
            label="Bootstrap token"
            name="bootstrapToken"
            onChange={(event) => onChange({ ...values, bootstrapToken: event.target.value })}
            required
            type="password"
            value={values.bootstrapToken}
            wide
          />
          <FormField
            autoComplete="username"
            label="Admin username"
            name="adminUsername"
            onChange={(event) => onChange({ ...values, adminUsername: event.target.value })}
            required
            value={values.adminUsername}
            wide
          />
          <FormField
            autoComplete="new-password"
            label="Admin password"
            minLength={8}
            name="adminPassword"
            onChange={(event) => onChange({ ...values, adminPassword: event.target.value })}
            required
            type="password"
            value={values.adminPassword}
            wide
          />
          <FormError message={error} />
          <Button disabled={submitting} type="submit">
            {submitting ? "Creating admin" : "Create admin"}
          </Button>
        </form>
      </section>
    </main>
  );
}

function LoginPage({
  error,
  onChange,
  onSubmit,
  submitting,
  values
}: {
  error: string;
  onChange: (values: LoginValues) => void;
  onSubmit: (event: FormEvent<HTMLFormElement>) => void;
  submitting: boolean;
  values: LoginValues;
}) {
  return (
    <main className="app-frame app-frame--centered">
      <section className="auth-panel" aria-labelledby="login-title">
        <AuthHeader subtitle="Vectis Console" title="Sign in" titleID="login-title" />
        {new URLSearchParams(window.location.search).get("password_changed") === "1" ? (
          <p role="status">Password changed. Sign in with your new password.</p>
        ) : null}
        <form className="auth-form" onSubmit={onSubmit}>
          <FormField
            autoComplete="username"
            label="Username"
            name="username"
            onChange={(event) => onChange({ ...values, username: event.target.value })}
            required
            value={values.username}
            wide
          />
          <FormField
            autoComplete="current-password"
            label="Password"
            name="password"
            onChange={(event) => onChange({ ...values, password: event.target.value })}
            required
            type="password"
            value={values.password}
            wide
          />
          <FormError message={error} />
          <Button disabled={submitting} type="submit">
            {submitting ? "Signing in" : "Sign in"}
          </Button>
        </form>
      </section>
    </main>
  );
}

function AuthHeader({ subtitle, title, titleID }: { subtitle: string; title: string; titleID: string }) {
  const theme = useResolvedTheme();
  return (
    <div className="auth-panel__header">
      <div className="auth-panel__logo-frame">
        <img alt="Vectis" className="auth-panel__logo" src={theme === "dark" ? vectisDarkLogo : vectisLogo} />
      </div>
      <div>
        <p className="eyebrow">{subtitle}</p>
        <h1 id={titleID}>{title}</h1>
      </div>
    </div>
  );
}

function RouteContent({
  onChangePassword,
  loadArtifacts,
  actionError,
  jobDefinitionError,
  consoleData,
  consoleError,
  healthCells,
  healthError,
  loadingHealthCells,
  missingRunID,
  namespacePath,
  onCreateNamespace,
  onCreateUser,
  onCreateJob,
  onDeleteNamespace,
  onDeleteUser,
  onGrantRoleBinding,
  onRevokeRoleBinding,
  onSubmitEphemeralRun,
  onTriggerRun,
  onCancelRun,
  onReplayRun,
  onOpenRun,
  onUpdateJob,
  onUpdateNamespace,
  onUpdateUserStatus,
  onSelectNamespace,
  route
}: {
  onChangePassword?: (currentPassword: string, newPassword: string) => Promise<void>;
  loadArtifacts: LoadRunArtifacts;
  actionError: string;
  jobDefinitionError: string;
  consoleData: MockConsoleData | null;
  consoleError: string;
  healthCells: Cell[] | null;
  healthError: string;
  loadingHealthCells: boolean;
  missingRunID: string | null;
  namespacePath: string;
  onCreateJob: (input: NewJob) => Promise<void> | void;
  onCreateNamespace: (input: NewNamespace) => Promise<void> | void;
  onCreateUser: (input: NewUser) => Promise<CreatedUserCredential | undefined> | CreatedUserCredential | undefined;
  onDeleteNamespace: (namespaceID: number) => Promise<void> | void;
  onDeleteUser: (userID: string) => Promise<void> | void;
  onGrantRoleBinding: (
    userID: string,
    namespaceID: number,
    role: RoleBindingRole,
    previousRole?: RoleBindingRole
  ) => Promise<void> | void;
  onRevokeRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  onSubmitEphemeralRun: (definition: string) => Promise<void> | void;
  onTriggerRun: (jobID: string) => void;
  onCancelRun: (runID: string) => Promise<void>;
  onReplayRun: (runID: string, idempotencyKey: string) => Promise<void>;
  onOpenRun: (runID: string) => Promise<void>;
  onUpdateJob: (jobID: string, input: UpdateJob) => Promise<void> | void;
  onUpdateNamespace: (namespaceID: number, input: UpdateNamespace) => Promise<void> | void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => Promise<void> | void;
  onSelectNamespace: (namespacePath: string) => void;
  route: AppRoute;
}) {
  const { can, ready: accessReady, error: accessError } = useConsoleAccess();
  const pendingRun =
    route.kind === "runs" &&
    route.runID &&
    !consoleData?.runs.some((run) => run.id === route.runID) &&
    missingRunID !== route.runID;

  const scrollReady =
    route.kind === "profile" || route.kind === "repositories"
      ? true
      : (accessReady || Boolean(accessError)) &&
        (route.kind === "health"
          ? !can("admin:*") || !loadingHealthCells
          : Boolean(consoleData || consoleError) && (!pendingRun || Boolean(consoleError)));

  useNavigationScroll(route, scrollReady);

  const withActionAlert = (children: ReactNode) => (
    <>
      <ActionAlertRail
        message={
          actionError ||
          (route.kind === "health" ? healthError : "") ||
          (accessError ? `Unable to load permissions: ${accessError}. Reload to retry.` : "")
        }
      />
      {children}
    </>
  );

  if (route.kind === "health") {
    if (accessError)
      return (
        <PageStateRail>
          <AppState title="Unable to Load Permissions" description={accessError} tone="error" />
        </PageStateRail>
      );
    if (!accessReady) return withActionAlert(<HealthPage cells={[]} loading onSelectCell={() => undefined} />);
    if (!can("admin:*"))
      return (
        <PageStateRail>
          <AppState
            title="Administrative Access Required"
            description="Health contains administrative diagnostics. You can operate jobs and runs within your namespace access."
            tone="error"
          />
        </PageStateRail>
      );
    const cells = healthCells ?? [];

    if (loadingHealthCells && cells.length === 0) {
      return withActionAlert(<HealthPage cells={[]} loading onSelectCell={() => undefined} />);
    }

    if (healthError && !healthCells) {
      return (
        <PageStateRail>
          <AppState title="Unable to Load Cell Status" description={healthError} tone="error" />
        </PageStateRail>
      );
    }

    if (route.cellID) {
      const cell = cells.find((candidate) => candidate.id === route.cellID);

      if (!cell) {
        return withActionAlert(
          <PageMissingState
            actionLabel="Go to Health"
            breadcrumbs={[
              { label: "Health", onClick: () => navigateTo("/health") },
              { label: "Missing Cell", current: true }
            ]}
            description="The requested cell is not registered with this console."
            label="Health breadcrumbs"
            onAction={() => navigateTo("/health")}
            panelDescription="The cell may have been removed, renamed, or never registered."
            panelEyebrow="Missing"
            panelTitle="Cell Not Found"
            title="Cell Not Found"
          />
        );
      }

      return withActionAlert(<DashboardPage cell={cell} onOpenHealth={() => navigateTo("/health")} />);
    }

    return withActionAlert(
      <HealthPage cells={cells} onSelectCell={(cellID) => navigateTo(`/health/${encodeURIComponent(cellID)}`)} />
    );
  }

  if (route.kind === "repositories")
    return (
      <RepositoriesPage
        repositoryID={route.repositoryID}
        namespacePath={route.workspaceNamespace ?? route.repositoryNamespace ?? namespacePath}
        management={route.repositoryManagement}
        sharedWorkspace
        onNamespaceChange={route.repositoryManagement ? undefined : onSelectNamespace}
      />
    );

  if (route.kind === "profile") return <ProfilePage onChangePassword={onChangePassword} />;

  if (consoleError && !consoleData) {
    return (
      <PageStateRail>
        <AppState description={consoleError} title="Unable to Load Console" tone="error" />
      </PageStateRail>
    );
  }

  if (!consoleData) {
    return (
      <PageStateRail>
        <AppState title="Loading Console" tone="loading" />
      </PageStateRail>
    );
  }

  if (!accessReady && !accessError)
    return (
      <PageStateRail>
        <AppState title="Loading Permissions" tone="loading" />
      </PageStateRail>
    );

  if (route.kind === "users" && (!can("user:admin") || consoleData.denied?.users)) {
    return (
      <PageStateRail>
        <AppState
          title="Access Required"
          description="Your account cannot manage users. Jobs and runs remain available within your namespace access."
          tone="error"
        />
      </PageStateRail>
    );
  }

  const scopedConsoleData = {
    ...consoleData,
    jobs: consoleData.jobs.filter((job) => job.namespacePath === namespacePath),
    runs: consoleData.runs.filter((run) => run.namespacePath === namespacePath)
  };

  switch (route.kind) {
    case "runs":
      if (route.runID) {
        const run = consoleData.runs.find((candidate) => candidate.id === route.runID);
        if (!run && missingRunID !== route.runID) {
          return (
            <PageStateRail>
              <AppState title="Loading Run" tone="loading" />
            </PageStateRail>
          );
        }

        return withActionAlert(
          <RunDetailPage
            sharedWorkspace
            investigation={route.runInvestigation}
            onInvestigationChange={(context) => {
              const { pathname, search, hash } = window.location;
              const path = runInvestigationPath(pathname, search, hash, context);
              if (path !== `${pathname}${search}${hash}`) navigateTo(path);
            }}
            loadArtifacts={loadArtifacts}
            onCancel={onCancelRun}
            onReplay={onReplayRun}
            onOpenRun={onOpenRun}
            onBack={returnToRunList}
            onOpenJob={(jobName) =>
              navigateTo(jobPathForRunJob(consoleData, jobName, run?.namespacePath, run?.repositoryID))
            }
            run={run}
            originalRun={consoleData.runs.find((candidate) => candidate.id === run?.replayOfRunID)}
            runID={route.runID}
          />
        );
      }

      return withActionAlert(
        <RunsPage
          filters={route.runFilters}
          sharedWorkspace
          onFiltersChange={(filters, options) => {
            const { search, hash } = window.location;
            const path = runListFiltersPath(search, hash, filters);
            if (path !== `${window.location.pathname}${search}${hash}`)
              navigateTo(path, { ...options, namespace: namespacePath });
          }}
          namespaces={consoleData.namespaces}
          namespacePath={namespacePath}
          onSelectNamespace={(namespace) => {
            onSelectNamespace(namespace);
            navigateTo(`${window.location.pathname}${window.location.search}${window.location.hash}`, {
              replace: true,
              namespace
            });
          }}
          onSelectRun={(runID) => navigateTo(`/runs/${runID}`, { namespace: namespacePath })}
          onSubmitEphemeralRun={onSubmitEphemeralRun}
          jobName={route.runJobName}
          runs={scopedConsoleData.runs}
        />
      );
    case "jobs": {
      const targetID = route.jobID ?? (route.jobEditor?.kind === "edit" ? route.jobEditor.jobID : undefined);
      const targetJob = consoleData.jobs.find((job) => job.id === targetID && jobMatchesRoute(job, route));

      if (route.jobEditor && (!can("admin:*") || !can("job:write", targetJob?.namespacePath ?? namespacePath))) {
        return (
          <PageStateRail>
            <AppState
              title="Job Editing Access Required"
              description="Your account can inspect or operate jobs but cannot change definitions in this namespace."
              tone="error"
            />
          </PageStateRail>
        );
      }
      if (targetJob && targetJob.definition === undefined) {
        return (
          <PageStateRail>
            <AppState
              title={jobDefinitionError ? "Unable to Load Job Definition" : "Loading Job Definition"}
              description={jobDefinitionError || undefined}
              tone={jobDefinitionError ? "error" : "loading"}
            />
          </PageStateRail>
        );
      }
      return withActionAlert(
        <JobsPage
          sharedWorkspace
          detailJobID={route.jobID}
          repositories={consoleData.repositories.filter(
            (repository) => repository.namespacePath === namespacePath && can("job:write", repository.namespacePath)
          )}
          editorMode={route.jobEditor ?? null}
          allJobs={consoleData.jobs.filter((job) => jobMatchesRoute(job, route))}
          jobs={scopedConsoleData.jobs}
          namespaces={consoleData.namespaces}
          namespacePath={namespacePath}
          onCloseEditor={() => navigateTo(safeReturnPath() ?? "/jobs")}
          onCreateJob={onCreateJob}
          onOpenCreate={() => navigateTo(workspacePath("/jobs/create", namespacePath))}
          onOpenEditor={(jobID) => navigateTo(jobConfigPath(jobID, route))}
          onOpenJob={(jobID, repositoryID) => {
            const selectedJob = consoleData.jobs.find(
              (job) =>
                job.id === jobID &&
                job.namespacePath === namespacePath &&
                (!repositoryID || job.repository === repositoryID)
            );
            const target = workspacePath(
              jobID ? `/jobs/${encodeURIComponent(jobID)}` : "/jobs",
              selectedJob?.namespacePath ?? namespacePath
            );
            navigateTo(repositoryID ? `${target}&repository=${encodeURIComponent(repositoryID)}` : target);
          }}
          onOpenJobRuns={(jobName) => navigateTo(`/runs?job=${encodeURIComponent(jobName)}`)}
          onSelectRun={(runID) => navigateTo(`/runs/${runID}`)}
          onSelectNamespace={onSelectNamespace}
          onTriggerRun={onTriggerRun}
          onUpdateJob={onUpdateJob}
          runs={consoleData.runs}
        />
      );
    }
    case "users":
      return withActionAlert(
        <UsersPage
          onCreateUser={onCreateUser}
          onDeleteUser={onDeleteUser}
          onGrantRoleBinding={onGrantRoleBinding}
          onOpenUser={(userID) => navigateTo(`/users/${encodeURIComponent(userID)}`)}
          onOpenUsers={() => navigateTo("/users")}
          onRevokeRoleBinding={onRevokeRoleBinding}
          onUpdateUserStatus={onUpdateUserStatus}
          namespaces={consoleData.namespaces}
          selectedUserID={route.userID}
          users={consoleData.users}
        />
      );
    case "namespaces": {
      const namespaceByPath = route.workspaceNamespace
        ? consoleData.namespaces.find((row) => row.path === route.workspaceNamespace)
        : undefined;

      return withActionAlert(
        <NamespacesPage
          unavailableBindings={consoleData.denied?.bindings}
          usersUnavailable={consoleData.denied?.users}
          canDeleteNamespace={(namespaceID) => canDeleteMockNamespace(consoleData, namespaceID)}
          jobs={consoleData.jobs}
          namespaces={consoleData.namespaces}
          onCreateNamespace={onCreateNamespace}
          onDeleteNamespace={onDeleteNamespace}
          onOpenJobs={(namespacePath) => {
            onSelectNamespace(namespacePath);
            navigateTo(workspacePath("/jobs", namespacePath));
          }}
          onCloseEditor={() => navigateTo(`/namespaces/${route.namespaceID ?? ""}`)}
          onConfigureNamespace={(namespaceID) => navigateTo(`/namespaces/${namespaceID}/config`)}
          onOpenNamespace={(namespaceID) => navigateTo(`/namespaces/${namespaceID}`)}
          onOpenNamespaces={() => navigateTo("/namespaces")}
          onGrantRoleBinding={onGrantRoleBinding}
          onRevokeRoleBinding={onRevokeRoleBinding}
          onUpdateNamespace={onUpdateNamespace}
          editorMode={route.namespaceEditor ?? null}
          selectedNamespaceMissing={
            route.namespaceMissing || Boolean(!route.namespaceID && route.workspaceNamespace && !namespaceByPath)
          }
          selectedNamespaceID={route.namespaceID ?? namespaceByPath?.id}
          users={consoleData.users}
        />
      );
    }
    default:
      return (
        <PageStateRail>
          <NotFoundPage />
        </PageStateRail>
      );
  }
}

function withUpdatedUsers(data: MockConsoleData | null, users: User[]) {
  if (!data) return data;
  const remainingIDs = new Set(users.map((user) => user.id));
  return {
    ...data,
    users: users.map((user) => {
      const previous = data.users.find((candidate) => candidate.id === user.id);
      // Status and deletion responses contain account rows, without namespace grants.
      return previous && user.roleBindings === undefined
        ? { ...user, role: previous.role, roleBindings: previous.roleBindings }
        : user;
    }),
    roleBindings: data.roleBindings.filter((binding) => remainingIDs.has(binding.userID))
  };
}

function navigateAfterAuth() {
  navigateTo(safeNextPath() ?? "/jobs");
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

function jobConfigPath(jobID: string, route: AppRoute) {
  const jobPath = `/jobs/${encodeURIComponent(jobID)}`;
  const context = new URLSearchParams();
  if (route.jobRepository) context.set("repository", route.jobRepository);
  if (route.jobNamespace) context.set("namespace", route.jobNamespace);

  const returnTo = route.jobID === jobID ? `${jobPath}${context.size ? `?${context}` : ""}` : route.pathname;
  context.set("returnTo", returnTo);

  return `${jobPath}/config?${context}`;
}

function jobMatchesRoute(job: Job, route: AppRoute) {
  return (
    (!route.jobRepository || job.repository === route.jobRepository) &&
    (!(route.jobNamespace ?? route.workspaceNamespace) ||
      job.namespacePath === (route.jobNamespace ?? route.workspaceNamespace))
  );
}

function jobPathForRunJob(consoleData: MockConsoleData, jobName: string, namespace?: string, repository?: string) {
  const job = consoleData.jobs.find(
    (candidate) =>
      candidate.name === jobName &&
      (!namespace || candidate.namespacePath === namespace) &&
      (!repository || candidate.repository === repository)
  );

  return job
    ? `${workspacePath(`/jobs/${encodeURIComponent(job.id)}`, job.namespacePath)}&repository=${encodeURIComponent(job.repository)}`
    : `/runs?job=${encodeURIComponent(jobName)}`;
}

function namespacePathForRoute(consoleData: MockConsoleData, route: AppRoute) {
  if (route.kind === "runs" && route.runID)
    return consoleData.runs.find((run) => run.id === route.runID)?.namespacePath ?? route.workspaceNamespace ?? null;

  if (route.workspaceNamespace) return route.workspaceNamespace;
  if (route.kind === "runs" && !route.runID) {
    const saved = runListNamespace();
    if (consoleData.namespaces.some((namespace) => namespace.path === saved)) return saved ?? null;
  }

  if (route.kind === "runs" && route.runJobName) {
    return consoleData.jobs.find((job) => job.name === route.runJobName)?.namespacePath ?? null;
  }

  if (route.kind !== "jobs") {
    return null;
  }

  const jobID = route.jobID ?? (route.jobEditor?.kind === "edit" ? route.jobEditor.jobID : null);
  if (!jobID) {
    return null;
  }

  return consoleData.jobs.find((job) => job.id === jobID && jobMatchesRoute(job, route))?.namespacePath ?? null;
}
