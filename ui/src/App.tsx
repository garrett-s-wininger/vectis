import { FormEvent, useEffect, useState } from "react";
import "./index.css";
import { Button } from "./components/Button";
import { completeSetup, login, logout } from "./api/auth";

type View =
  | { kind: "setup" }
  | { kind: "login" }
  | { kind: "console" };

export function App() {
  const [view, setView] = useState<View>(() => viewFromLocation());
  const [setupValues, setSetupValues] = useState({
    bootstrapToken: "",
    adminUsername: "admin",
    adminPassword: ""
  });

  const [loginValues, setLoginValues] = useState({
    username: "",
    password: ""
  });

  const [submitting, setSubmitting] = useState(false);
  const [formError, setFormError] = useState("");

  useEffect(() => {
    const onPopState = () => setView(viewFromLocation());
    window.addEventListener("popstate", onPopState);
    return () => window.removeEventListener("popstate", onPopState);
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

  async function signOut() {
    try {
      await logout();
    } finally {
      navigateTo("/login");
    }
  }

  if (view.kind === "setup") {
    return (
      <main className="app-frame app-frame--centered">
        <section className="auth-panel" aria-labelledby="setup-title">
          <p className="eyebrow">Vectis Console</p>
          <h1 id="setup-title">Complete setup</h1>
          <form className="auth-form" onSubmit={handleSetup}>
            <label className="field">
              <span>Bootstrap token</span>
              <input
                autoComplete="off"
                name="bootstrapToken"
                onChange={(event) =>
                  setSetupValues({
                    ...setupValues,
                    bootstrapToken: event.target.value
                  })
                }
                required
                type="password"
                value={setupValues.bootstrapToken}
              />
            </label>
            <label className="field">
              <span>Admin username</span>
              <input
                autoComplete="username"
                name="adminUsername"
                onChange={(event) =>
                  setSetupValues({
                    ...setupValues,
                    adminUsername: event.target.value
                  })
                }
                required
                type="text"
                value={setupValues.adminUsername}
              />
            </label>
            <label className="field">
              <span>Admin password</span>
              <input
                autoComplete="new-password"
                minLength={8}
                name="adminPassword"
                onChange={(event) =>
                  setSetupValues({
                    ...setupValues,
                    adminPassword: event.target.value
                  })
                }
                required
                type="password"
                value={setupValues.adminPassword}
              />
            </label>
            {formError ? <p className="form-error">{formError}</p> : null}
            <Button disabled={submitting} type="submit">
              {submitting ? "Creating admin" : "Create admin"}
            </Button>
          </form>
        </section>
      </main>
    );
  }

  if (view.kind === "login") {
    return (
      <main className="app-frame app-frame--centered">
        <section className="auth-panel" aria-labelledby="login-title">
          <p className="eyebrow">Vectis Console</p>
          <h1 id="login-title">Sign in</h1>
          <form className="auth-form" onSubmit={handleLogin}>
            <label className="field">
              <span>Username</span>
              <input
                autoComplete="username"
                name="username"
                onChange={(event) =>
                  setLoginValues({
                    ...loginValues,
                    username: event.target.value
                  })
                }
                required
                type="text"
                value={loginValues.username}
              />
            </label>
            <label className="field">
              <span>Password</span>
              <input
                autoComplete="current-password"
                name="password"
                onChange={(event) =>
                  setLoginValues({
                    ...loginValues,
                    password: event.target.value
                  })
                }
                required
                type="password"
                value={loginValues.password}
              />
            </label>
            {formError ? <p className="form-error">{formError}</p> : null}
            <Button disabled={submitting} type="submit">
              {submitting ? "Signing in" : "Sign in"}
            </Button>
          </form>
        </section>
      </main>
    );
  }

  return (
    <main className="app-frame">
      <section className="console-panel" aria-labelledby="console-title">
        <div>
          <p className="eyebrow">Vectis Console</p>
          <h1 id="console-title">Runs dashboard</h1>
          <p>
            The browser shell is ready for authenticated workflows.
          </p>
        </div>
        <Button onClick={signOut}>Sign out</Button>
      </section>
    </main>
  );
}

function viewFromLocation(): View {
  switch (window.location.pathname) {
    case "/setup":
      return { kind: "setup" };
    case "/login":
      return { kind: "login" };
    default:
      return { kind: "console" };
  }
}

function navigateAfterAuth() {
  navigateTo(safeNextPath() ?? "/");
}

function navigateTo(path: string) {
  window.history.pushState(null, "", path);
  window.dispatchEvent(new PopStateEvent("popstate"));
}

function safeNextPath() {
  const next = new URLSearchParams(window.location.search).get("next");
  if (!next) {
    return null;
  }

  try {
    const url = new URL(next, window.location.origin);
    if (url.origin !== window.location.origin) {
      return null;
    }
    if (url.pathname === "/login" || url.pathname === "/setup") {
      return null;
    }

    return `${url.pathname}${url.search}${url.hash}`;
  } catch {
    return null;
  }
}
