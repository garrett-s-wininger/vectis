import type { FormEvent, MouseEvent } from "react";
import { useEffect, useState } from "react";
import "./index.css";
import vectisLogo from "../../assets/brand/public/vectis.png";
import { completeSetup, loadUIContext, login, logout } from "./api/auth";
import { Button } from "./components";
import { AppShell } from "./components";
import { AppState } from "./components";
import { FormError } from "./components";
import { FormField } from "./components";
import { createConsoleDataSource } from "./data/consoleDataSource";
import type { NewJob, UpdateJob } from "./domain/console";
import {
  canDeleteMockNamespace,
  createMockNamespace,
  createMockUser,
  deleteMockNamespace,
  deleteMockUser,
  nextMockRunID,
  scopeMockConsoleData,
  submitMockEphemeralRun,
  updateMockUserStatus,
  type MockConsoleData,
  type MockUserStatus,
  type NewMockNamespace,
  type NewMockUser
} from "./mocks/consoleData";
import { HealthPage } from "./pages/HealthPage";
import { JobsPage } from "./pages/JobsPage";
import { NamespacesPage } from "./pages/NamespacesPage";
import { NotFoundPage } from "./pages/NotFoundPage";
import { RunDetailPage } from "./pages/RunDetailPage";
import { RunsPage } from "./pages/RunsPage";
import { UsersPage } from "./pages/UsersPage";
import {
  adminNavItems,
  navigateTo,
  primaryNavItems,
  routeFromPath,
  safeNextPath,
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
  const [route, setRoute] = useState<AppRoute>(() => routeFromPath(window.location.pathname));

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
  const [consoleDataSource] = useState(() => createConsoleDataSource());
  const [consoleData, setConsoleData] = useState<MockConsoleData | null>(null);
  const [consoleError, setConsoleError] = useState("");
  const [selectedNamespacePath, setSelectedNamespacePath] = useState("/");

  useEffect(() => {
    const onPopState = () => {
      setFormError("");
      setSubmitting(false);
      setRoute(routeFromPath(window.location.pathname));
    };
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

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

    consoleDataSource
      .loadConsole()
      .then((data) => {
        if (!ignore) {
          setConsoleData(data);
          setConsoleError("");
        }
      })
      .catch((error: unknown) => {
        if (!ignore) {
          setConsoleError(error instanceof Error ? error.message : "Unable to load console data.");
        }
      });

    return () => {
      ignore = true;
    };
  }, [consoleDataSource, route.kind]);

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
      setFormError(error instanceof Error ? error.message : "Setup failed");
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
      setFormError(error instanceof Error ? error.message : "Login failed");
    } finally {
      setSubmitting(false);
    }
  }

  function updateConsoleData(update: (data: MockConsoleData) => MockConsoleData) {
    setConsoleData((data) => (data ? update(data) : data));
  }

  function handleCreateUser(input: NewMockUser) {
    updateConsoleData((data) => createMockUser(data, input));
  }

  function handleUpdateUserStatus(userID: string, status: MockUserStatus) {
    updateConsoleData((data) => updateMockUserStatus(data, userID, status));
  }

  function handleDeleteUser(userID: string) {
    updateConsoleData((data) => deleteMockUser(data, userID));
  }

  function handleCreateNamespace(input: NewMockNamespace) {
    updateConsoleData((data) => createMockNamespace(data, input));
  }

  async function handleCreateJob(input: NewJob) {
    try {
      setConsoleData(await consoleDataSource.createJob(input));
      setConsoleError("");
    } catch (error) {
      setConsoleError(error instanceof Error ? error.message : "Unable to create job.");
    }
  }

  async function handleUpdateJob(jobID: string, input: UpdateJob) {
    try {
      setConsoleData(await consoleDataSource.updateJob(jobID, input));
      setConsoleError("");
    } catch (error) {
      setConsoleError(error instanceof Error ? error.message : "Unable to update job.");
    }
  }

  function handleDeleteNamespace(namespaceID: number) {
    const namespacePath = consoleData?.namespaces.find((namespace) => namespace.id === namespaceID)?.path;

    updateConsoleData((data) => deleteMockNamespace(data, namespaceID));

    if (namespacePath === selectedNamespacePath) {
      setSelectedNamespacePath("/");
    }
  }

  async function handleTriggerRun(jobID: string) {
    try {
      setConsoleData(await consoleDataSource.triggerRun(jobID));
      setConsoleError("");
    } catch (error) {
      setConsoleError(error instanceof Error ? error.message : "Unable to trigger job.");
    }
  }

  function handleSubmitEphemeralRun(definition: string) {
    const runID = consoleData ? nextMockRunID(consoleData) : null;

    updateConsoleData((data) =>
      submitMockEphemeralRun(data, {
        definition,
        namespacePath: selectedNamespacePath,
        submittedBy: "admin"
      })
    );

    if (runID) {
      navigateTo(`/runs/${runID}`);
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
        consoleData={consoleData}
        consoleError={consoleError}
        namespacePath={selectedNamespacePath}
        onCreateJob={handleCreateJob}
        onCreateNamespace={handleCreateNamespace}
        onCreateUser={handleCreateUser}
        onDeleteNamespace={handleDeleteNamespace}
        onDeleteUser={handleDeleteUser}
        onSubmitEphemeralRun={handleSubmitEphemeralRun}
        onTriggerRun={handleTriggerRun}
        onUpdateJob={handleUpdateJob}
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
  consoleData,
  consoleError,
  namespacePath,
  onCreateNamespace,
  onCreateUser,
  onCreateJob,
  onDeleteNamespace,
  onDeleteUser,
  onSubmitEphemeralRun,
  onTriggerRun,
  onUpdateJob,
  onUpdateUserStatus,
  onSelectNamespace,
  route
}: {
  consoleData: MockConsoleData | null;
  consoleError: string;
  namespacePath: string;
  onCreateJob: (input: NewJob) => void;
  onCreateNamespace: (input: NewMockNamespace) => void;
  onCreateUser: (input: NewMockUser) => void;
  onDeleteNamespace: (namespaceID: number) => void;
  onDeleteUser: (userID: string) => void;
  onSubmitEphemeralRun: (definition: string) => void;
  onTriggerRun: (jobID: string) => void;
  onUpdateJob: (jobID: string, input: UpdateJob) => void;
  onUpdateUserStatus: (userID: string, status: MockUserStatus) => void;
  onSelectNamespace: (namespacePath: string) => void;
  route: AppRoute;
}) {
  if (consoleError) {
    return <AppState description={consoleError} title="Unable to load console" tone="error" />;
  }

  if (!consoleData) {
    return <AppState title="Loading console" tone="loading" />;
  }

  const scopedConsoleData = scopeMockConsoleData(consoleData, namespacePath);

  switch (route.kind) {
    case "health":
      return (
        <HealthPage
          cells={consoleData.cells}
          onSelectCell={(cellID) => navigateTo(`/health/${cellID}`)}
          selectedCellID={route.cellID}
        />
      );
    case "runs":
      if (route.runID) {
        return (
          <RunDetailPage
            onBack={() => navigateTo("/runs")}
            run={consoleData.runs.find((run) => run.id === route.runID)}
            runID={route.runID}
          />
        );
      }

      return (
        <RunsPage
          namespaces={consoleData.namespaces}
          namespacePath={namespacePath}
          onSelectNamespace={onSelectNamespace}
          onSelectRun={(runID) => navigateTo(`/runs/${runID}`)}
          onSubmitEphemeralRun={onSubmitEphemeralRun}
          runs={scopedConsoleData.runs}
        />
      );
    case "jobs":
      return (
        <JobsPage
          editorMode={route.jobEditor ?? null}
          jobs={scopedConsoleData.jobs}
          namespaces={consoleData.namespaces}
          namespacePath={namespacePath}
          onCloseEditor={() => navigateTo("/jobs")}
          onCreateJob={onCreateJob}
          onOpenCreate={() => navigateTo("/jobs/create")}
          onOpenEditor={(jobID) => navigateTo(`/jobs/${encodeURIComponent(jobID)}/config`)}
          onSelectRun={(runID) => navigateTo(`/runs/${runID}`)}
          onSelectNamespace={onSelectNamespace}
          onTriggerRun={onTriggerRun}
          onUpdateJob={onUpdateJob}
          runs={scopedConsoleData.runs}
        />
      );
    case "users":
      return (
        <UsersPage
          onCreateUser={onCreateUser}
          onDeleteUser={onDeleteUser}
          onUpdateUserStatus={onUpdateUserStatus}
          users={consoleData.users}
        />
      );
    case "namespaces":
      return (
        <NamespacesPage
          canDeleteNamespace={(namespaceID) => canDeleteMockNamespace(consoleData, namespaceID)}
          namespaces={consoleData.namespaces}
          onCreateNamespace={onCreateNamespace}
          onDeleteNamespace={onDeleteNamespace}
        />
      );
    case "profile":
      return <AppState description="Account preferences and session details will live here." title="Profile" />;
    default:
      return <NotFoundPage />;
  }
}

function navigateAfterAuth() {
  navigateTo(safeNextPath() ?? "/");
}
