import { act, fireEvent, render, screen, waitFor } from "@testing-library/react";
import { App } from "./App";

describe("App", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    window.history.replaceState(null, "", "/");
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  it("renders the setup route selected by the BFF", () => {
    window.history.replaceState(null, "", "/setup?next=%2Fruns%2F123");

    render(<App />);

    expect(screen.getByRole("heading", { name: "Complete setup" })).toBeInTheDocument();
  });

  it("completes setup and navigates to next", async () => {
    window.history.replaceState(null, "", "/setup?next=%2Fruns");
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ username: "admin" }), { status: 200 }));

    render(<App />);

    fireEvent.change(screen.getByLabelText("Bootstrap token"), {
      target: { value: "bootstrap-token" }
    });

    fireEvent.change(screen.getByLabelText("Admin username"), {
      target: { value: "admin" }
    });

    fireEvent.change(screen.getByLabelText("Admin password"), {
      target: { value: "password123" }
    });

    fireEvent.click(screen.getByRole("button", { name: "Create admin" }));

    expect(await screen.findByRole("heading", { name: "Runs" })).toBeInTheDocument();

    expect(window.location.pathname).toBe("/runs");
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/ui/api/setup/complete",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("renders the login route selected by the BFF", () => {
    window.history.replaceState(null, "", "/login");

    render(<App />);

    expect(screen.getByRole("heading", { name: "Sign in" })).toBeInTheDocument();
  });

  it("logs in and navigates to next", async () => {
    window.history.replaceState(null, "", "/login?next=%2Fusers");
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ user_id: 1, username: "admin" }), {
        status: 200
      })
    );

    render(<App />);

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "admin" }
    });

    fireEvent.change(screen.getByLabelText("Password"), {
      target: { value: "password123" }
    });

    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("heading", { name: "Users" })).toBeInTheDocument();

    expect(window.location.pathname).toBe("/users");
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/ui/api/login",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("loads live stored jobs after login when the API data source is enabled", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    window.history.replaceState(null, "", "/login?next=%2Fjobs");
    fetchMock
      .mockResolvedValueOnce(
        new Response(JSON.stringify({ user_id: 1, username: "admin" }), {
          status: 200
        })
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify([
            {
              id: 1,
              name: "/",
              path: "/",
              break_inheritance: false
            }
          ]),
          { status: 200 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            data: [
              {
                name: "live-smoke",
                namespace: "/",
                definition: {
                  id: "live-smoke",
                  root: { id: "root", uses: "builtins/shell" }
                }
              }
            ]
          }),
          { status: 200 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            data: []
          }),
          { status: 200 }
        )
      );

    render(<App />);

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "admin" }
    });

    fireEvent.change(screen.getByLabelText("Password"), {
      target: { value: "password123" }
    });

    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("heading", { name: "Jobs" })).toBeInTheDocument();
    expect(await screen.findByText("live-smoke")).toBeInTheDocument();
    expect(screen.getByText("admin")).toBeInTheDocument();
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/jobs?limit=200",
      expect.objectContaining({ credentials: "same-origin" })
    );
  });

  it("ignores unsafe next values", async () => {
    window.history.replaceState(null, "", "/login?next=https%3A%2F%2Fevil.test");
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ user_id: 1, username: "admin" }), {
        status: 200
      })
    );

    render(<App />);

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "admin" }
    });

    fireEvent.change(screen.getByLabelText("Password"), {
      target: { value: "password123" }
    });

    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    expect(await screen.findByRole("heading", { name: "Health" })).toBeInTheDocument();

    expect(window.location.pathname).toBe("/");
  });

  it("renders app routes directly after BFF authorization", async () => {
    window.history.replaceState(null, "", "/runs/run-1240");

    render(<App />);

    expect(await screen.findByRole("heading", { name: "api-test-suite #1240" })).toBeInTheDocument();
    expect(screen.getByRole("link", { name: "Runs" })).toHaveAttribute("aria-current", "page");
  });

  it("navigates app routes without a page load", async () => {
    render(<App />);

    fireEvent.click(screen.getByRole("link", { name: "Users" }));

    expect(await screen.findByRole("heading", { name: "Users" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/users");
  });

  it("renders cluster health without namespace scoping", async () => {
    window.history.replaceState(null, "", "/health");

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Health" })).toBeInTheDocument();

    expect(screen.queryByLabelText("Namespace")).not.toBeInTheDocument();
    expect(screen.getByText("prod-west")).toBeInTheDocument();
    expect(screen.getByText("Offline")).toBeInTheDocument();
  });

  it("drills into individual cell health", async () => {
    window.history.replaceState(null, "", "/health");

    render(<App />);

    await screen.findByRole("heading", { name: "Health" });

    fireEvent.click(screen.getByRole("button", { name: "Inspect edge" }));

    expect(screen.getByRole("heading", { name: "edge dashboard" })).toBeInTheDocument();
    expect(screen.getByText("Lag 2m 14s")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/health/cell-edge");
  });

  it("creates, disables, and removes a user in the mock", async () => {
    window.history.replaceState(null, "", "/users");

    render(<App />);

    await screen.findByRole("heading", { name: "Users" });

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "taylor" }
    });

    fireEvent.change(screen.getByLabelText("Role"), {
      target: { value: "Operator" }
    });

    fireEvent.click(screen.getByRole("button", { name: "Add user" }));

    expect(await screen.findByText("taylor")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Disable taylor" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Disable taylor" }));

    expect(screen.getByRole("button", { name: "Activate taylor" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Remove taylor" }));

    expect(screen.queryByText("taylor")).not.toBeInTheDocument();
  });

  it("triggers a mock job run and shows it in runs", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: /docs-publish/ }));
    fireEvent.click(screen.getAllByRole("button", { name: "Trigger docs-publish" })[0]);
    fireEvent.click(screen.getByRole("link", { name: "Runs" }));

    expect(await screen.findByText("#1241")).toBeInTheDocument();
    expect(screen.getByText(/manual · Queued/)).toBeInTheDocument();
  });

  it("opens a job detail page from the jobs list", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: /docs-publish/ }));
    fireEvent.click(screen.getAllByRole("button", { name: "Open docs-publish" })[0]);

    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();
    expect(screen.getByText("Publishes documentation updates from the reviewed docs repository.")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Definition" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-docs-publish");

    fireEvent.click(screen.getByRole("button", { name: "Jobs" }));

    expect(await screen.findByRole("heading", { name: "Jobs" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs");
  });

  it("returns from job config to the detail page that opened it", async () => {
    window.history.replaceState(null, "", "/jobs/job-docs-publish");

    render(<App />);

    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Config docs-publish" }));

    expect(await screen.findByRole("heading", { name: "Configure" })).toBeInTheDocument();
    expect(screen.getByText("Review and adjust the stored definition, state, and triggers.")).toBeInTheDocument();
    expect(await screen.findByRole("region", { name: "Configure" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-docs-publish/config");
    expect(new URLSearchParams(window.location.search).get("returnTo")).toBe("/jobs/job-docs-publish");

    fireEvent.click(screen.getAllByRole("button", { name: "Cancel" })[0]);

    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-docs-publish");
  });

  it("opens a run detail placeholder from the runs list", async () => {
    window.history.replaceState(null, "", "/runs");

    render(<App />);

    await screen.findByRole("heading", { name: "Runs" });

    fireEvent.click(screen.getByRole("button", { name: "Open run api-test-suite #1240" }));

    expect(await screen.findByRole("heading", { name: "api-test-suite #1240" })).toBeInTheDocument();
    expect(screen.getByText("run-1240")).toBeInTheDocument();
    expect(screen.getByText("Stored")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/runs/run-1240");

    fireEvent.click(screen.getByRole("tab", { name: "Definition" }));

    expect(screen.getByText(/go test \.\/internal\/api/)).toBeInTheDocument();
  });

  it("submits an ephemeral run from the runs page", async () => {
    window.history.replaceState(null, "", "/runs");

    render(<App />);

    await screen.findByRole("heading", { name: "Runs" });

    fireEvent.click(screen.getByRole("button", { name: "Run once" }));
    fireEvent.change(screen.getByLabelText("Job definition JSON"), {
      target: {
        value: JSON.stringify({
          id: "database-backfill",
          root: { id: "root", uses: "builtins/shell" }
        })
      }
    });

    fireEvent.click(screen.getByRole("button", { name: "Submit run" }));

    expect(await screen.findByRole("heading", { name: "database-backfill #1241" })).toBeInTheDocument();
    expect(screen.getByText("Ephemeral")).toBeInTheDocument();
    expect(screen.getByText("inline definition")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("tab", { name: "Definition" }));

    expect(screen.getByText(/"id": "database-backfill"/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("link", { name: "Jobs" }));

    expect(screen.queryByText("database-backfill")).not.toBeInTheDocument();
  });

  it("renders a run detail not found state", async () => {
    window.history.replaceState(null, "", "/runs/missing");

    render(<App />);

    expect(await screen.findByRole("heading", { level: 1, name: "Run not found" })).toBeInTheDocument();
    expect(screen.getByText("No run matched missing.")).toBeInTheDocument();
  });

  it("creates, edits, and deletes a stored job in the mock", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "cache-warmup" }
    });

    fireEvent.change(screen.getByLabelText("Cadence"), {
      target: { value: "Hourly" }
    });

    fireEvent.change(screen.getByLabelText("JSON"), {
      target: {
        value: JSON.stringify({
          id: "cache-warmup",
          root: { id: "root", uses: "builtins/shell" }
        })
      }
    });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(await screen.findByText("cache-warmup")).toBeInTheDocument();
    expect(screen.getAllByText("database").length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: /cache-warmup/ }));
    fireEvent.click(screen.getAllByRole("button", { name: "Config cache-warmup" })[0]);
    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "cache-prime" }
    });
    fireEvent.click(screen.getByLabelText("State"));
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect((await screen.findAllByText("cache-prime")).length).toBeGreaterThan(0);
    expect(screen.queryByRole("button", { name: "Trigger cache-prime" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Delete cache-prime" })).not.toBeInTheDocument();
  });

  it("opens job creation through browser navigation and backs out", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(await screen.findByRole("region", { name: "Create" })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/create");

    act(() => {
      window.history.back();
    });

    await waitFor(() => expect(window.location.pathname).toBe("/jobs"));
    await waitFor(() => expect(screen.queryByRole("region", { name: "Create" })).not.toBeInTheDocument());
  });

  it("scopes jobs by selected namespace", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByLabelText("Namespace"));
    fireEvent.click(screen.getByRole("button", { name: "/prod" }));

    expect(screen.getByText("worker-image")).toBeInTheDocument();
    expect(screen.queryByText("api-test-suite")).not.toBeInTheDocument();
  });

  it("creates and deletes an empty namespace in the mock", async () => {
    window.history.replaceState(null, "", "/namespaces");

    render(<App />);

    await screen.findByRole("heading", { name: "Namespaces" });

    expect(screen.getByRole("button", { name: "Delete /" })).toBeDisabled();

    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "sandbox" }
    });
    fireEvent.click(screen.getByRole("button", { name: "Create namespace" }));

    expect(await screen.findByRole("button", { name: "Delete /sandbox" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Delete /sandbox" }));

    expect(screen.queryByRole("button", { name: "Delete /sandbox" })).not.toBeInTheDocument();
  });

  it("logs out and returns to login", async () => {
    window.history.replaceState(null, "", "/runs/123");
    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            auth_enabled: true,
            principal: { kind: "local_user", username: "admin" }
          }),
          { status: 200 }
        )
      )
      .mockResolvedValueOnce(new Response(null, { status: 204 }));

    render(<App />);

    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));

    expect(await screen.findByRole("heading", { name: "Sign in" })).toBeInTheDocument();

    expect(window.location.pathname).toBe("/login");
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/ui/api/logout",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("hides sign out when the BFF reports auth disabled", async () => {
    window.history.replaceState(null, "", "/jobs");
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          auth_enabled: false,
          principal: { kind: "auth_disabled", username: "Anonymous" }
        }),
        { status: 200 }
      )
    );

    render(<App />);

    expect(await screen.findByText("Anonymous")).toBeInTheDocument();
    expect(screen.queryByText("Auth disabled")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Sign out" })).not.toBeInTheDocument();
  });
});
