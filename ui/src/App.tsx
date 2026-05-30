import type { FormEvent, MouseEvent } from "react";
import { useEffect, useState } from "react";
import "./index.css";
import { completeSetup, login, logout } from "./api/auth";
import { Button } from "./components/Button";
import { AppShell } from "./components/AppShell";
import { AppState } from "./components/AppState";
import { FormError } from "./components/FormError";
import { FormField } from "./components/FormField";
import { NamespacePicker } from "./components/NamespacePicker";
import {
  canDeleteMockNamespace,
  createMockNamespace,
  createMockUser,
  deleteMockNamespace,
  deleteMockUser,
  loadMockConsoleData,
  nextMockRunID,
  scopeMockConsoleData,
  submitMockEphemeralRun,
  triggerMockRun,
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
  const [route, setRoute] = useState<AppRoute>(() =>
    routeFromPath(window.location.pathname)
  );

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
  const [consoleData, setConsoleData] = useState<MockConsoleData | null>(null);
  const [consoleError, setConsoleError] = useState("");
  const [selectedNamespacePath, setSelectedNamespacePath] = useState("/");

  useEffect(() => {
    const onPopState = () => setRoute(routeFromPath(window.location.pathname));
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    setFormError("");
    setSubmitting(false);
  }, [route.kind]);

  useEffect(() => {
    let ignore = false;

    loadMockConsoleData()
      .then((data) => {
        if (!ignore) {
          setConsoleData(data);
        }
      })
      .catch(() => {
        if (!ignore) {
          setConsoleError("Unable to load console data.");
        }
      });

    return () => {
      ignore = true;
    };
  }, []);

  async function handleSetup(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitting(true);
    setFormError("");

    try {
      await completeSetup({
        bootstrap_token: setupValues.bootstrapToken,
        admin_username: setupValues.adminUsername,
        admin_password: setupValues.adminPassword
      });

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
      await login({
        username: loginValues.username,
        password: loginValues.password
      });

      navigateAfterAuth();
    } catch (error) {
      setFormError(error instanceof Error ? error.message : "Login failed");
    } finally {
      setSubmitting(false);
    }
  }

  function updateConsoleData(
    update: (data: MockConsoleData) => MockConsoleData
  ) {
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

  function handleDeleteNamespace(namespaceID: number) {
    const namespacePath = consoleData?.namespaces.find(
      (namespace) => namespace.id === namespaceID
    )?.path;

    updateConsoleData((data) => deleteMockNamespace(data, namespaceID));

    if (namespacePath === selectedNamespacePath) {
      setSelectedNamespacePath("/");
    }
  }

  function handleTriggerRun(jobID: string) {
    updateConsoleData((data) => triggerMockRun(data, jobID));
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

  function handleShellNavigate(
    href: string,
    event: MouseEvent<HTMLAnchorElement>
  ) {
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

  const showNamespacePicker = route.kind === "jobs" || route.kind === "runs";

  return (
    <AppShell
      activeHref={route.activeHref}
      actions={
        <>
          {consoleData && showNamespacePicker ? (
            <NamespacePicker
              namespaces={consoleData.namespaces}
              onChange={setSelectedNamespacePath}
              value={selectedNamespacePath}
            />
          ) : null}
          <Button onClick={signOut}>Sign out</Button>
        </>
      }
      brand="Vectis"
      navItems={primaryNavItems}
      onNavigate={handleShellNavigate}
    >
      <RouteContent
        consoleData={consoleData}
        consoleError={consoleError}
        namespacePath={selectedNamespacePath}
        onCreateNamespace={handleCreateNamespace}
        onCreateUser={handleCreateUser}
        onDeleteNamespace={handleDeleteNamespace}
        onDeleteUser={handleDeleteUser}
        onSubmitEphemeralRun={handleSubmitEphemeralRun}
        onTriggerRun={handleTriggerRun}
        onUpdateUserStatus={handleUpdateUserStatus}
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
        <p className="eyebrow">Vectis Console</p>
        <h1 id="setup-title">Complete setup</h1>
        <form className="auth-form" onSubmit={onSubmit}>
          <FormField
            autoComplete="off"
            label="Bootstrap token"
            name="bootstrapToken"
            onChange={(event) =>
              onChange({ ...values, bootstrapToken: event.target.value })
            }
            required
            type="password"
            value={values.bootstrapToken}
          />
          <FormField
            autoComplete="username"
            label="Admin username"
            name="adminUsername"
            onChange={(event) =>
              onChange({ ...values, adminUsername: event.target.value })
            }
            required
            value={values.adminUsername}
          />
          <FormField
            autoComplete="new-password"
            label="Admin password"
            minLength={8}
            name="adminPassword"
            onChange={(event) =>
              onChange({ ...values, adminPassword: event.target.value })
            }
            required
            type="password"
            value={values.adminPassword}
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
        <p className="eyebrow">Vectis Console</p>
        <h1 id="login-title">Sign in</h1>
        <form className="auth-form" onSubmit={onSubmit}>
          <FormField
            autoComplete="username"
            label="Username"
            name="username"
            onChange={(event) =>
              onChange({ ...values, username: event.target.value })
            }
            required
            value={values.username}
          />
          <FormField
            autoComplete="current-password"
            label="Password"
            name="password"
            onChange={(event) =>
              onChange({ ...values, password: event.target.value })
            }
            required
            type="password"
            value={values.password}
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

function RouteContent({
  consoleData,
  consoleError,
  namespacePath,
  onCreateNamespace,
  onCreateUser,
  onDeleteNamespace,
  onDeleteUser,
  onSubmitEphemeralRun,
  onTriggerRun,
  onUpdateUserStatus,
  route
}: {
  consoleData: MockConsoleData | null;
  consoleError: string;
  namespacePath: string;
  onCreateNamespace: (input: NewMockNamespace) => void;
  onCreateUser: (input: NewMockUser) => void;
  onDeleteNamespace: (namespaceID: number) => void;
  onDeleteUser: (userID: string) => void;
  onSubmitEphemeralRun: (definition: string) => void;
  onTriggerRun: (jobID: string) => void;
  onUpdateUserStatus: (userID: string, status: MockUserStatus) => void;
  route: AppRoute;
}) {
  if (consoleError) {
    return (
      <AppState
        description={consoleError}
        title="Unable to load console"
        tone="error"
      />
    );
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
          namespacePath={namespacePath}
          onSelectRun={(runID) => navigateTo(`/runs/${runID}`)}
          onSubmitEphemeralRun={onSubmitEphemeralRun}
          runs={scopedConsoleData.runs}
        />
      );
    case "jobs":
      return (
        <JobsPage
          jobs={scopedConsoleData.jobs}
          namespacePath={namespacePath}
          onTriggerRun={onTriggerRun}
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
          canDeleteNamespace={(namespaceID) =>
            canDeleteMockNamespace(consoleData, namespaceID)
          }
          namespaces={consoleData.namespaces}
          onCreateNamespace={onCreateNamespace}
          onDeleteNamespace={onDeleteNamespace}
        />
      );
    default:
      return <NotFoundPage />;
  }
}

function navigateAfterAuth() {
  navigateTo(safeNextPath() ?? "/");
}
