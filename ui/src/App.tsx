import type { FormEvent, MouseEvent } from "react";
import { useEffect, useState } from "react";
import "./index.css";
import { completeSetup, login, logout } from "./api/auth";
import { Button } from "./components/Button";
import { AppShell } from "./components/AppShell";
import { AppState } from "./components/AppState";
import { FormError } from "./components/FormError";
import { FormField } from "./components/FormField";
import {
  createMockUser,
  deleteMockUser,
  loadMockConsoleData,
  triggerMockRun,
  updateMockUserStatus,
  type MockConsoleData,
  type MockUserStatus,
  type NewMockUser
} from "./mocks/consoleData";
import { DashboardPage } from "./pages/DashboardPage";
import { JobsPage } from "./pages/JobsPage";
import { NotFoundPage } from "./pages/NotFoundPage";
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

  function handleTriggerRun(jobID: string) {
    updateConsoleData((data) => triggerMockRun(data, jobID));
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

  return (
    <AppShell
      activeHref={route.activeHref}
      actions={<Button onClick={signOut}>Sign out</Button>}
      brand="Vectis"
      navItems={primaryNavItems}
      onNavigate={handleShellNavigate}
    >
      <RouteContent
        consoleData={consoleData}
        consoleError={consoleError}
        onCreateUser={handleCreateUser}
        onDeleteUser={handleDeleteUser}
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
  onCreateUser,
  onDeleteUser,
  onTriggerRun,
  onUpdateUserStatus,
  route
}: {
  consoleData: MockConsoleData | null;
  consoleError: string;
  onCreateUser: (input: NewMockUser) => void;
  onDeleteUser: (userID: string) => void;
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

  switch (route.kind) {
    case "dashboard":
      return <DashboardPage data={consoleData} />;
    case "runs":
      return <RunsPage runs={consoleData.runs} />;
    case "jobs":
      return (
        <JobsPage jobs={consoleData.jobs} onTriggerRun={onTriggerRun} />
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
    default:
      return <NotFoundPage />;
  }
}

function navigateAfterAuth() {
  navigateTo(safeNextPath() ?? "/");
}
