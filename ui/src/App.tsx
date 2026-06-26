import type { FormEvent, MouseEvent, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import "./index.css";
import vectisLogo from "../../assets/brand/public/vectis.png";
import { completeSetup, loadUIContext, login, logout } from "./api/auth";
import { Button } from "./components";
import { AppShell } from "./components";
import { AppState } from "./components";
import { ErrorAlert } from "./components";
import { FormError } from "./components";
import { FormField } from "./components";
import { VectisAPIError } from "./api/client";
import { createConsoleDataSource, type CreatedUserCredential } from "./data/consoleDataSource";
import type { Cell, NewJob, NewNamespace, UpdateJob, UpdateNamespace } from "./domain/console";
import type { NewUser, RoleBindingRole, UserStatus } from "./domain/console";
import { canDeleteMockNamespace, nextMockRunID, scopeMockConsoleData, type MockConsoleData } from "./mocks/consoleData";
import { DashboardPage } from "./pages/DashboardPage";
import { HealthPage } from "./pages/HealthPage";
import { JobsPage } from "./pages/JobsPage";
import { NamespacesPage } from "./pages/NamespacesPage";
import { NotFoundPage } from "./pages/NotFoundPage";
import { RunDetailPage } from "./pages/RunDetailPage";
import { RunsPage } from "./pages/RunsPage";
import { UsersPage } from "./pages/UsersPage";
import { PageMissingState } from "./pages/shared";
import {
  adminNavItems,
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
  const [accountName, setAccountName] = useState("admin");
  const [authEnabled, setAuthEnabled] = useState(true);
  const consoleDataSourceRef = useRef(createConsoleDataSource());
  const [consoleData, setConsoleData] = useState<MockConsoleData | null>(null);
  const [consoleError, setConsoleError] = useState("");
  const [actionError, setActionError] = useState("");
  const [healthCells, setHealthCells] = useState<Cell[] | null>(null);
  const [missingRunID, setMissingRunID] = useState<string | null>(null);
  const [selectedNamespacePath, setSelectedNamespacePath] = useState("/");
  const loadingRunIDRef = useRef<string | null>(null);

  useEffect(() => {
    const onPopState = () => {
      const nextRoute = routeFromPath(window.location.pathname, window.location.search);
      setActionError("");
      setFormError("");
      setSubmitting(false);
      setRoute(nextRoute);
      if (consoleData) {
        const routeNamespacePath = jobNamespacePathForRoute(consoleData, nextRoute);
        if (routeNamespacePath) {
          setSelectedNamespacePath(routeNamespacePath);
        }
      }
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, [consoleData]);

  useEffect(() => {
    if (route.kind === "login" || route.kind === "setup") {
      return;
    }

    let ignore = false;

    loadUIContext()
      .then((context) => {
        if (ignore) {
          return;
        }

        setAuthEnabled(context.auth_enabled);
        setAccountName(context.principal.username);
      })
      .catch(() => {
        // Older dev servers and Storybook do not expose UI context. Keep the
        // existing authenticated-looking shell as the compatibility fallback.
      });

    return () => {
      ignore = true;
    };
    // Only discover server context for the route the BFF initially served.
    // Login/setup responses update the account state directly.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  useEffect(() => {
    let ignore = false;

    if (route.kind === "login" || route.kind === "setup") {
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
          const routeNamespacePath = jobNamespacePathForRoute(data, currentRoute);
          if (routeNamespacePath) {
            setSelectedNamespacePath(routeNamespacePath);
          }
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
  }, [route.kind]);

  useEffect(() => {
    if (route.kind !== "health") {
      return;
    }

    let ignore = false;

    consoleDataSourceRef.current
      .loadCells()
      .then((cells) => {
        if (!ignore) {
          setHealthCells(cells);
          setActionError("");
        }
      })
      .catch((error: unknown) => {
        if (!ignore) {
          setActionError(errorMessage(error, "Unable to load cell status."));
        }
      });

    return () => {
      ignore = true;
    };
  }, [route.kind]);

  useEffect(() => {
    if (route.kind !== "runs" || !route.runID || !consoleData) {
      return;
    }

    if (consoleData.runs.some((run) => run.id === route.runID)) {
      loadingRunIDRef.current = null;
      return;
    }

    if (missingRunID === route.runID || loadingRunIDRef.current === route.runID) {
      return;
    }

    let ignore = false;
    loadingRunIDRef.current = route.runID;

    consoleDataSourceRef.current
      .loadRun(route.runID)
      .then((run) => {
        if (ignore) {
          if (loadingRunIDRef.current === route.runID) {
            loadingRunIDRef.current = null;
          }

          return;
        }

        setConsoleData((data) => {
          if (!data || data.runs.some((candidate) => candidate.id === run.id)) {
            return data;
          }

          return {
            ...data,
            runs: [run, ...data.runs]
          };
        });

        setMissingRunID(null);
        loadingRunIDRef.current = null;
        setConsoleError("");
        setActionError("");
      })
      .catch((error: unknown) => {
        loadingRunIDRef.current = null;
        if (!ignore) {
          if (error instanceof VectisAPIError && error.code === "run_not_found") {
            setMissingRunID(route.runID ?? null);
            return;
          }

          const message = errorMessage(error, "Unable to load run.");
          if (message !== "Run not found.") {
            setConsoleError(message);
          } else {
            setMissingRunID(route.runID ?? null);
          }
        }
      });

    return () => {
      ignore = true;
      if (loadingRunIDRef.current === route.runID) {
        loadingRunIDRef.current = null;
      }
    };
  }, [consoleData, missingRunID, route.kind, route.runID]);

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
      setConsoleData((data) => (data ? { ...data, users } : data));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to update user."));
    }
  }

  async function handleDeleteUser(userID: string) {
    try {
      const users = await consoleDataSourceRef.current.deleteUser(userID);
      setConsoleData((data) => (data ? { ...data, users } : data));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to delete user."));
      throw error;
    }
  }

  async function handleGrantRoleBinding(userID: string, namespaceID: number, role: RoleBindingRole) {
    try {
      setConsoleData(await consoleDataSourceRef.current.grantRoleBinding(userID, namespaceID, role));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to grant role binding."));
      throw error;
    }
  }

  async function handleRevokeRoleBinding(userID: string, namespaceID: number, role: RoleBindingRole) {
    try {
      setConsoleData(await consoleDataSourceRef.current.revokeRoleBinding(userID, namespaceID, role));
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
    const shouldReturnToNamespaces = route.kind === "namespaces" && route.namespaceID === namespaceID;

    try {
      setConsoleData(await consoleDataSourceRef.current.deleteNamespace(namespaceID));
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
      setConsoleData(await consoleDataSourceRef.current.triggerRun(jobID));
      setConsoleError("");
      setActionError("");
    } catch (error) {
      setActionError(errorMessage(error, "Unable to trigger job."));
    }
  }

  async function handleSubmitEphemeralRun(definition: string) {
    const fallbackRunID = consoleData ? nextMockRunID(consoleData) : null;

    try {
      const nextData = await consoleDataSourceRef.current.submitEphemeralRun({
        definition,
        namespacePath: selectedNamespacePath,
        submittedBy: "admin"
      });
      const runID = nextData.runs[0]?.id ?? fallbackRunID;

      setConsoleData(nextData);
      setConsoleError("");
      setActionError("");

      if (runID) {
        navigateTo(`/runs/${runID}`);
      }
    } catch (error) {
      setActionError(errorMessage(error, "Unable to submit run."));
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
    navigateTo(href);
  }

  async function signOut() {
    try {
      await logout();
    } finally {
      setConsoleData(null);
      navigateTo("/login");
    }
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
    <AppShell
      activeHref={route.activeHref}
      accountDetail={authEnabled ? undefined : "Auth disabled"}
      accountName={accountName}
      brand="Vectis"
      navItems={primaryNavItems}
      onNavigate={handleShellNavigate}
      onSignOut={authEnabled ? signOut : undefined}
      showProfile={authEnabled}
      utilityNavItems={adminNavItems}
    >
      <RouteContent
        actionError={actionError}
        consoleData={consoleData}
        consoleError={consoleError}
        healthCells={healthCells}
        loadingHealthCells={route.kind === "health" && healthCells === null && !actionError}
        missingRunID={missingRunID}
        namespacePath={selectedNamespacePath}
        onCreateJob={handleCreateJob}
        onCreateNamespace={handleCreateNamespace}
        onCreateUser={handleCreateUser}
        onDeleteNamespace={handleDeleteNamespace}
        onDeleteUser={handleDeleteUser}
        onGrantRoleBinding={handleGrantRoleBinding}
        onRevokeRoleBinding={handleRevokeRoleBinding}
        onSubmitEphemeralRun={handleSubmitEphemeralRun}
        onTriggerRun={handleTriggerRun}
        onUpdateJob={handleUpdateJob}
        onUpdateNamespace={handleUpdateNamespace}
        onUpdateUserStatus={handleUpdateUserStatus}
        onSelectNamespace={setSelectedNamespacePath}
        route={route}
      />
    </AppShell>
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
  return (
    <div className="auth-panel__header">
      <div className="auth-panel__logo-frame">
        <img alt="Vectis" className="auth-panel__logo" src={vectisLogo} />
      </div>
      <div>
        <p className="eyebrow">{subtitle}</p>
        <h1 id={titleID}>{title}</h1>
      </div>
    </div>
  );
}

function RouteContent({
  actionError,
  consoleData,
  consoleError,
  healthCells,
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
  onUpdateJob,
  onUpdateNamespace,
  onUpdateUserStatus,
  onSelectNamespace,
  route
}: {
  actionError: string;
  consoleData: MockConsoleData | null;
  consoleError: string;
  healthCells: Cell[] | null;
  loadingHealthCells: boolean;
  missingRunID: string | null;
  namespacePath: string;
  onCreateJob: (input: NewJob) => Promise<void> | void;
  onCreateNamespace: (input: NewNamespace) => Promise<void> | void;
  onCreateUser: (input: NewUser) => Promise<CreatedUserCredential | undefined> | CreatedUserCredential | undefined;
  onDeleteNamespace: (namespaceID: number) => Promise<void> | void;
  onDeleteUser: (userID: string) => Promise<void> | void;
  onGrantRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  onRevokeRoleBinding: (userID: string, namespaceID: number, role: RoleBindingRole) => Promise<void> | void;
  onSubmitEphemeralRun: (definition: string) => Promise<void> | void;
  onTriggerRun: (jobID: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => Promise<void> | void;
  onUpdateNamespace: (namespaceID: number, input: UpdateNamespace) => Promise<void> | void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => Promise<void> | void;
  onSelectNamespace: (namespacePath: string) => void;
  route: AppRoute;
}) {
  if (consoleError && !consoleData) {
    return (
      <CenteredAppState>
        <AppState description={consoleError} title="Unable to load console" tone="error" />
      </CenteredAppState>
    );
  }

  if (!consoleData) {
    return (
      <CenteredAppState>
        <AppState title="Loading console" tone="loading" />
      </CenteredAppState>
    );
  }

  const scopedConsoleData = scopeMockConsoleData(consoleData, namespacePath);
  const withActionAlert = (children: ReactNode) => (
    <PageWithActionAlert actionError={actionError}>{children}</PageWithActionAlert>
  );

  switch (route.kind) {
    case "health": {
      const cells = healthCells ?? consoleData.cells;

      if (loadingHealthCells && cells.length === 0) {
        return (
          <CenteredAppState>
            <AppState title="Loading cell status" tone="loading" />
          </CenteredAppState>
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
    case "runs":
      if (route.runID) {
        const run = consoleData.runs.find((candidate) => candidate.id === route.runID);
        if (!run && missingRunID !== route.runID) {
          return (
            <CenteredAppState>
              <AppState title="Loading run" tone="loading" />
            </CenteredAppState>
          );
        }

        return withActionAlert(
          <RunDetailPage
            onBack={() => navigateTo("/runs")}
            onOpenJob={(jobName) => navigateTo(jobPathForRunJob(consoleData, jobName))}
            run={run}
            runID={route.runID}
          />
        );
      }

      return withActionAlert(
        <RunsPage
          namespaces={consoleData.namespaces}
          namespacePath={namespacePath}
          onSelectNamespace={onSelectNamespace}
          onSelectRun={(runID) => navigateTo(`/runs/${runID}`)}
          onSubmitEphemeralRun={onSubmitEphemeralRun}
          jobName={route.runJobName}
          runs={scopedConsoleData.runs}
        />
      );
    case "jobs":
      return withActionAlert(
        <JobsPage
          detailJobID={route.jobID}
          editorMode={route.jobEditor ?? null}
          allJobs={consoleData.jobs}
          jobs={scopedConsoleData.jobs}
          namespaces={consoleData.namespaces}
          namespacePath={namespacePath}
          onCloseEditor={() => navigateTo(safeReturnPath() ?? "/jobs")}
          onCreateJob={onCreateJob}
          onOpenCreate={() => navigateTo("/jobs/create")}
          onOpenEditor={(jobID) => navigateTo(jobConfigPath(jobID, route))}
          onOpenJob={(jobID) => navigateTo(jobID ? `/jobs/${encodeURIComponent(jobID)}` : "/jobs")}
          onOpenJobRuns={(jobName) => navigateTo(`/runs?job=${encodeURIComponent(jobName)}`)}
          onSelectRun={(runID) => navigateTo(`/runs/${runID}`)}
          onSelectNamespace={onSelectNamespace}
          onTriggerRun={onTriggerRun}
          onUpdateJob={onUpdateJob}
          runs={consoleData.runs}
        />
      );
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
    case "namespaces":
      return withActionAlert(
        <NamespacesPage
          canDeleteNamespace={(namespaceID) => canDeleteMockNamespace(consoleData, namespaceID)}
          jobs={consoleData.jobs}
          namespaces={consoleData.namespaces}
          onCreateNamespace={onCreateNamespace}
          onDeleteNamespace={onDeleteNamespace}
          onOpenJobs={(namespacePath) => {
            onSelectNamespace(namespacePath);
            navigateTo("/jobs");
          }}
          onCloseEditor={() => navigateTo(`/namespaces/${route.namespaceID ?? ""}`)}
          onConfigureNamespace={(namespaceID) => navigateTo(`/namespaces/${namespaceID}/config`)}
          onOpenNamespace={(namespaceID) => navigateTo(`/namespaces/${namespaceID}`)}
          onOpenNamespaces={() => navigateTo("/namespaces")}
          onGrantRoleBinding={onGrantRoleBinding}
          onRevokeRoleBinding={onRevokeRoleBinding}
          onUpdateNamespace={onUpdateNamespace}
          editorMode={route.namespaceEditor ?? null}
          selectedNamespaceMissing={route.namespaceMissing}
          selectedNamespaceID={route.namespaceID}
          users={consoleData.users}
        />
      );
    case "profile":
      return withActionAlert(
        <CenteredAppState>
          <AppState description="Account preferences and session details will live here." title="Profile" />
        </CenteredAppState>
      );
    default:
      return (
        <CenteredAppState>
          <NotFoundPage />
        </CenteredAppState>
      );
  }
}

function CenteredAppState({ children }: { children: ReactNode }) {
  return <div className="app-state-rail">{children}</div>;
}

function PageWithActionAlert({ actionError, children }: { actionError: string; children: ReactNode }) {
  return (
    <>
      {actionError ? (
        <div className="app-alert-rail">
          <ErrorAlert message={actionError} title="Action Failed" />
        </div>
      ) : null}
      {children}
    </>
  );
}

function navigateAfterAuth() {
  navigateTo(safeNextPath() ?? "/");
}

function errorMessage(error: unknown, fallback: string) {
  return error instanceof Error ? error.message : fallback;
}

function jobConfigPath(jobID: string, route: AppRoute) {
  const jobPath = `/jobs/${encodeURIComponent(jobID)}`;
  const returnTo = route.jobID === jobID ? jobPath : route.pathname;
  return `${jobPath}/config?returnTo=${encodeURIComponent(returnTo)}`;
}

function jobPathForRunJob(consoleData: MockConsoleData, jobName: string) {
  const job = consoleData.jobs.find((candidate) => candidate.name === jobName);
  return job ? `/jobs/${encodeURIComponent(job.id)}` : `/runs?job=${encodeURIComponent(jobName)}`;
}

function jobNamespacePathForRoute(consoleData: MockConsoleData, route: AppRoute) {
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

  return consoleData.jobs.find((job) => job.id === jobID)?.namespacePath ?? null;
}
