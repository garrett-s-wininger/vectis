import type { FormEvent, MouseEvent } from "react";
import { useEffect, useState } from "react";
import "./index.css";
import { completeSetup, login, logout } from "./api/auth";
import { Button } from "./components/Button";
import { AppShell } from "./components/AppShell";
import { AppState } from "./components/AppState";
import { FormError } from "./components/FormError";
import { FormField } from "./components/FormField";
import { MetricCard } from "./components/MetricCard";
import { PageHeader } from "./components/PageHeader";
import { ProgressMeter } from "./components/ProgressMeter";
import { RunList } from "./components/RunList";
import { SectionPanel } from "./components/SectionPanel";
import { SignalList } from "./components/SignalList";
import {
  activeRuns,
  dashboardMetrics,
  instanceSignals,
  workloadProgress
} from "./mocks/fixtures";
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

  useEffect(() => {
    const onPopState = () => setRoute(routeFromPath(window.location.pathname));
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
  }, []);

  useEffect(() => {
    setFormError("");
    setSubmitting(false);
  }, [route.kind]);

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
      <RouteContent route={route} />
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

function RouteContent({ route }: { route: AppRoute }) {
  switch (route.kind) {
    case "dashboard":
      return <DashboardPage />;
    case "runs":
      return <RunsPage />;
    case "jobs":
      return <JobsPage />;
    case "users":
      return <UsersPage />;
    default:
      return <NotFoundPage />;
  }
}

function DashboardPage() {
  return (
    <>
      <PageHeader
        description="Live activity and instance health for this Vectis local."
        eyebrow="Console"
        title="Dashboard"
      />
      <div className="metric-card-grid">
        {dashboardMetrics.map((metric) => (
          <MetricCard
            detail={metric.detail}
            key={metric.id}
            label={metric.label}
            tone={metric.tone}
            value={metric.value}
          />
        ))}
      </div>
      <div className="dashboard-layout">
        <RunList title="Active runs" runs={activeRuns} />
        <div className="dashboard-side-stack">
          <SectionPanel title="Instance signals">
            <SignalList signals={instanceSignals} />
          </SectionPanel>
          <SectionPanel title="Workload">
            <div className="progress-meter-stack">
              {workloadProgress.map((progress) => (
                <ProgressMeter
                  detail={progress.detail}
                  key={progress.id}
                  label={progress.label}
                  tone={progress.tone}
                  value={progress.value}
                />
              ))}
            </div>
          </SectionPanel>
        </div>
      </div>
    </>
  );
}

function RunsPage() {
  return (
    <>
      <PageHeader
        description="Recent queued, running, and completed work."
        eyebrow="Runs"
        title="Runs"
      />
      <RunList title="Active runs" runs={activeRuns} />
    </>
  );
}

function JobsPage() {
  return (
    <>
      <PageHeader
        description="Configured job definitions for this instance."
        eyebrow="Jobs"
        title="Jobs"
      />
      <AppState
        description="No job definitions are available yet."
        title="No jobs loaded"
        tone="empty"
      />
    </>
  );
}

function UsersPage() {
  return (
    <>
      <PageHeader
        description="Accounts with access to this Vectis console."
        eyebrow="Users"
        title="Users"
      />
      <AppState
        description="No user accounts are available yet."
        title="No users loaded"
        tone="empty"
      />
    </>
  );
}

function NotFoundPage() {
  return (
    <>
      <PageHeader eyebrow="Console" title="Page not found" />
      <AppState
        description="Choose another destination from the primary navigation."
        title="No route matched"
        tone="error"
      />
    </>
  );
}

function navigateAfterAuth() {
  navigateTo(safeNextPath() ?? "/");
}
