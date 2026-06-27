import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
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
        new Response(JSON.stringify([{ id: 1, username: "admin", enabled: true }]), {
          status: 200
        })
      )
      .mockResolvedValueOnce(new Response(JSON.stringify([]), { status: 200 }))
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

    expect(await screen.findByRole("heading", { level: 1, name: "api-test-suite (#1240)" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Investigation Summary" })).toBeInTheDocument();
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

  it("loads health directly from cell status without console fan-out", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    window.history.replaceState(null, "", "/health");

    fetchMock
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            auth_enabled: false,
            principal: { kind: "auth_disabled", username: "Anonymous" }
          }),
          { status: 200 }
        )
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            cells: [
              {
                cell_id: "local",
                ingress_required: false,
                ingress_configured: false,
                ingress_reachable: false,
                status: "local",
                queued: 0,
                stuck: 0,
                catalog_pending: 0,
                catalog_failed: 0,
                catalog_total: 0
              }
            ]
          }),
          { status: 200 }
        )
      );

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Health" })).toBeInTheDocument();
    expect(screen.getByText("local")).toBeInTheDocument();

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(fetchMock).toHaveBeenNthCalledWith(1, "/ui/api/context", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/v1/cells/status", expect.any(Object));
  });

  it("drills into individual cell health", async () => {
    window.history.replaceState(null, "", "/health");

    render(<App />);

    await screen.findByRole("heading", { name: "Health" });

    fireEvent.click(screen.getByRole("button", { name: "Inspect edge" }));

    expect(screen.getByRole("heading", { name: "edge" })).toBeInTheDocument();
    expect(screen.getByText("Lag 2m 14s")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/health/cell-edge");
  });

  it("creates, disables, and removes a user in the mock", async () => {
    window.history.replaceState(null, "", "/users");

    render(<App />);

    await screen.findByRole("heading", { name: "Users" });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    fireEvent.change(screen.getByLabelText("Username"), {
      target: { value: "taylor" }
    });

    fireEvent.change(screen.getByLabelText("Role"), {
      target: { value: "Operator" }
    });

    fireEvent.click(screen.getByRole("button", { name: "Add User" }));

    expect(await screen.findByText("taylor")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Initial Password" })).toBeInTheDocument();
    expect(screen.getByText("mock-generated-password")).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Dismiss" }));

    fireEvent.click(screen.getByRole("button", { name: "View taylor" }));
    expect(await screen.findByRole("heading", { level: 1, name: "taylor" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Disable" }));
    expect(await screen.findByRole("button", { name: "Activate" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
    const deleteDialog = screen.getByRole("dialog", { name: "Remove User" });
    expect(deleteDialog).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Cancel" }));
    expect(screen.queryByRole("dialog", { name: "Remove User" })).not.toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 1, name: "taylor" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
    fireEvent.click(
      within(screen.getByRole("dialog", { name: "Remove User" })).getByRole("button", { name: "Remove User" })
    );

    await waitFor(() => {
      expect(screen.queryByText("taylor")).not.toBeInTheDocument();
    });
  });

  it("opens and exits user detail routes", async () => {
    window.history.replaceState(null, "", "/users");

    render(<App />);

    await screen.findByRole("heading", { name: "Users" });

    fireEvent.click(screen.getByRole("button", { name: "View mira" }));

    expect(await screen.findByRole("heading", { level: 1, name: "mira" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/users/user-mira");

    fireEvent.click(screen.getByRole("button", { name: "Users" }));

    expect(await screen.findByRole("heading", { name: "Users" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/users");
  });

  it("triggers a mock job run and shows it in runs", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: "View docs-publish" }));
    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Run docs-publish" }));
    fireEvent.click(screen.getByRole("link", { name: "Runs" }));

    expect(await screen.findByText("Run #1241")).toBeInTheDocument();
    expect(screen.getAllByText("Waiting").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Cell").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Trigger").length).toBeGreaterThan(0);
    expect(screen.getByText("UI")).toBeInTheDocument();
    expect(screen.getAllByText("Actor").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Queued").length).toBeGreaterThan(0);
  });

  it("opens a job detail page from the jobs list", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: "View docs-publish" }));

    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();
    expect(screen.getByText("Publishes documentation updates from the reviewed docs repository.")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Definition" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-docs-publish");

    fireEvent.click(screen.getByRole("button", { name: "View all runs for docs-publish" }));

    expect(await screen.findByRole("heading", { name: "Runs" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "All Runs" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/runs");
    expect(new URLSearchParams(window.location.search).get("job")).toBe("docs-publish");

    window.history.replaceState(null, "", "/jobs/job-docs-publish");
    window.dispatchEvent(new PopStateEvent("popstate"));

    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Jobs" }));

    expect(await screen.findByRole("heading", { name: "Jobs" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs");
    expect(screen.getByText("docs-publish")).toBeInTheDocument();
    expect(screen.queryByText("api-test-suite")).not.toBeInTheDocument();
  });

  it("returns from job config to the detail page that opened it", async () => {
    window.history.replaceState(null, "", "/jobs/job-docs-publish");

    render(<App />);

    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Edit docs-publish" }));

    expect(await screen.findByRole("heading", { name: "Configure Job" })).toBeInTheDocument();
    expect(screen.getByText("Update editable settings, trigger policy, and definition JSON.")).toBeInTheDocument();
    expect(await screen.findByRole("region", { name: "Definition" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-docs-publish/config");
    expect(new URLSearchParams(window.location.search).get("returnTo")).toBe("/jobs/job-docs-publish");

    fireEvent.click(screen.getAllByRole("button", { name: "Cancel" })[0]);

    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-docs-publish");
  });

  it("opens a run detail page from the runs list", async () => {
    window.history.replaceState(null, "", "/runs");

    render(<App />);

    await screen.findByRole("heading", { name: "Runs" });

    fireEvent.click(screen.getByRole("button", { name: "Open run api-test-suite #1240" }));

    expect(await screen.findByRole("heading", { level: 1, name: "api-test-suite (#1240)" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Investigation Summary" })).toBeInTheDocument();
    expect(screen.getByText("run-1240")).toBeInTheDocument();
    expect(screen.getByText("Saved")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/runs/run-1240");
    expect(screen.getByRole("heading", { name: "Task Logs" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Timeline" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Job Definition" })).toBeInTheDocument();
    expect(screen.getByText("Submitted via Manual by mira.")).toBeInTheDocument();
    expect(screen.getByText("Assigned ID run-1240.")).toBeInTheDocument();
    expect(screen.getByText("Worker selected on local.")).toBeInTheDocument();
    expect(screen.getByText(/\+0s/)).toBeInTheDocument();
    expect(screen.getByText(/\+5s/)).toBeInTheDocument();
    expect(screen.getByText("Streaming")).toBeInTheDocument();
    expect(screen.getByText(/go test \.\/internal\/api/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "api-test-suite" }));

    expect(await screen.findByRole("heading", { level: 1, name: "api-test-suite" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-api-test-suite");
  });

  it("submits an ephemeral run from the runs page", async () => {
    window.history.replaceState(null, "", "/runs");

    render(<App />);

    await screen.findByRole("heading", { name: "Runs" });

    fireEvent.click(screen.getByRole("button", { name: "Run Once" }));
    fireEvent.change(screen.getByLabelText("Job definition JSON"), {
      target: {
        value: JSON.stringify({
          id: "database-backfill",
          root: { id: "root", uses: "builtins/shell" }
        })
      }
    });

    fireEvent.click(screen.getByRole("button", { name: "Submit run" }));

    expect(await screen.findByRole("heading", { level: 1, name: "database-backfill (#1241)" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Investigation Summary" })).toBeInTheDocument();
    expect(screen.getByText("Ephemeral")).toBeInTheDocument();
    expect(screen.getByText("Inline")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Submitted Definition" })).toBeInTheDocument();
    expect(screen.getByText(/"id": "database-backfill"/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("link", { name: "Jobs" }));

    expect(screen.queryByText("database-backfill")).not.toBeInTheDocument();
  });

  it("renders a run detail not found state", async () => {
    window.history.replaceState(null, "", "/runs/missing");

    render(<App />);

    expect(await screen.findByRole("heading", { level: 1, name: "Run Not Found" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "No Run Found" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "View Runs" })).toBeInTheDocument();
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

    fireEvent.change(screen.getByLabelText("Payload"), {
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

    fireEvent.click(screen.getByRole("button", { name: "View cache-warmup" }));
    expect(await screen.findByRole("heading", { name: "cache-warmup" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Edit cache-warmup" }));
    fireEvent.click(screen.getByLabelText("State"));
    fireEvent.click(screen.getByRole("button", { name: "Save" }));

    expect((await screen.findAllByText("cache-warmup")).length).toBeGreaterThan(0);
    expect(await screen.findByText("Paused")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Run cache-warmup" })).not.toBeInTheDocument();
  });

  it("opens job creation through browser navigation and backs out", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(await screen.findByRole("region", { name: "Definition" })).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/create");

    act(() => {
      window.history.back();
    });

    await waitFor(() => expect(window.location.pathname).toBe("/jobs"));
    await waitFor(() => expect(screen.queryByRole("region", { name: "Definition" })).not.toBeInTheDocument());
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

    expect(screen.getByRole("button", { name: "View /" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Delete /" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "sandbox" }
    });

    fireEvent.change(screen.getByLabelText("Description"), {
      target: { value: "Temporary test boundary." }
    });

    fireEvent.click(screen.getByRole("button", { name: "Create Namespace" }));

    expect(await screen.findByRole("button", { name: "View /sandbox" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Delete /sandbox" })).not.toBeInTheDocument();
    expect(screen.getByText("Temporary test boundary.")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "View /sandbox" }));
    expect(await screen.findByRole("heading", { name: "/sandbox" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Delete" }));

    await waitFor(() => {
      expect(screen.queryByRole("button", { name: "View /sandbox" })).not.toBeInTheDocument();
    });

    expect(window.location.pathname).toBe("/namespaces");
    expect(screen.getByRole("heading", { name: "Namespaces" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Namespace Not Found" })).not.toBeInTheDocument();
  });

  it("keeps malformed namespace routes on the namespace not-found page", async () => {
    window.history.replaceState(null, "", "/namespaces/missing");

    render(<App />);

    expect(await screen.findByRole("heading", { name: "Namespace Not Found" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "View Namespaces" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Go to Jobs" })).not.toBeInTheDocument();
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
