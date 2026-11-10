import { unrestrictedAccess, type ConsoleAccess } from "./domain/access";
import { act, fireEvent, render, screen, waitFor, within } from "@testing-library/react";
import { App } from "./App";
import * as dataSource from "./data/consoleDataSource";
import * as sourceJobs from "./data/sourceJobs";

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

  const operatorAccess: ConsoleAccess = {
    actions: ["job:read", "run:read", "run:trigger", "run:operator", "api:any"],
    namespaces: { "/team": ["job:read", "run:read", "run:trigger", "run:operator"] }
  };

  function scopedAPI(permissions: ConsoleAccess = operatorAccess) {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    fetchMock.mockImplementation(async (path: string) => {
      const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), { status });
      if (path === "/ui/api/context")
        return json({ auth_enabled: true, principal: { kind: "local_user", username: "viewer" } });

      if (path === "/api/v1/access") return json(permissions);
      if (path === "/api/v1/namespaces")
        return json([{ id: 2, name: "team", path: "/team", break_inheritance: false }]);

      if (path === "/api/v1/source-repositories?namespace=%2Fteam")
        return json([{ repository_id: "team-repo", namespace: "/team", enabled: true }]);

      if (path.startsWith("/api/v1/jobs?"))
        return json({ jobs: [{ job_id: "team-build", path: ".vectis/build.json" }] });

      if (path === "/api/v1/runs?limit=200") return json({ data: [] });
      return json({ message: "Permission denied" }, 403);
    });
  }

  it("opens scoped jobs despite forbidden administration APIs and selects an accessible namespace", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/jobs");
    render(<App />);

    expect(await screen.findByText("team-build")).toBeInTheDocument();
    expect(screen.queryByText("Unable to Load Console")).not.toBeInTheDocument();
    expect(document.querySelector('a[href="/users"]')).toBeNull();
    expect(screen.getByLabelText("Namespace: /team")).toBeInTheDocument();
  });

  it("lets operators open Run Once while hiding job creation and administrative navigation", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/jobs");
    render(<App />);

    await screen.findByText("team-build");
    expect(screen.queryByRole("button", { name: "Create" })).not.toBeInTheDocument();
    expect(document.querySelector('a[href="/health"]')).toBeNull();
    expect(document.querySelector('a[href="/users"]')).toBeNull();
    act(() => {
      window.history.pushState(null, "", "/runs");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });

    fireEvent.click(await screen.findByRole("button", { name: "Run Once" }));
    expect(screen.getByLabelText("Choose definition JSON file")).toBeInTheDocument();
  });

  it("waits for permissions before offering interactive controls", async () => {
    scopedAPI();
    const scopedFetch = fetchMock.getMockImplementation()!;

    let finish!: (response: Response) => void;
    const pending = new Promise<Response>((resolve) => {
      finish = resolve;
    });

    fetchMock.mockImplementation((path: string, options: RequestInit) =>
      path === "/api/v1/access" ? pending : scopedFetch(path, options)
    );

    window.history.replaceState(null, "", "/jobs");
    render(<App />);
    expect(await screen.findByRole("heading", { name: "Loading Permissions" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Create" })).not.toBeInTheDocument();

    await act(async () => {
      finish(new Response(JSON.stringify(operatorAccess), { status: 200 }));
    });

    expect(await screen.findByText("team-build")).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Create" })).not.toBeInTheDocument();
  });

  it("keeps controls unavailable and reports failure when permissions cannot be loaded", async () => {
    scopedAPI();
    const scopedFetch = fetchMock.getMockImplementation()!;

    fetchMock.mockImplementation((path: string, options: RequestInit) =>
      path === "/api/v1/access"
        ? Promise.resolve(
            new Response(JSON.stringify({ message: "Permissions temporarily unavailable" }), { status: 503 })
          )
        : scopedFetch(path, options)
    );

    window.history.replaceState(null, "", "/jobs");
    render(<App />);

    await screen.findByText("team-build");
    expect(screen.queryByRole("button", { name: "Create" })).not.toBeInTheDocument();
    expect(screen.getByRole("alert")).toHaveTextContent("Unable to load permissions");
  });

  it("refreshes access after creating a namespace without reloading on each route", async () => {
    const source = dataSource.createConsoleDataSource();
    const initial: ConsoleAccess = {
      actions: unrestrictedAccess.actions,
      namespaces: { "/": unrestrictedAccess.actions }
    };

    source.initialAccess = initial;
    source.loadAccess = vi
      .fn()
      .mockResolvedValueOnce(initial)
      .mockResolvedValueOnce({
        ...initial,
        namespaces: { ...initial.namespaces, "/new-team": unrestrictedAccess.actions }
      });

    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);

    try {
      window.history.replaceState(null, "", "/namespaces");
      render(<App />);

      fireEvent.click(await screen.findByRole("button", { name: "Create" }));
      fireEvent.change(screen.getByLabelText("Name"), { target: { value: "new-team" } });
      fireEvent.click(screen.getByRole("button", { name: "Create Namespace" }));
      fireEvent.click(await screen.findByText("/new-team"));
      expect(await screen.findByRole("button", { name: "Configure" })).toBeInTheDocument();
      expect(source.loadAccess).toHaveBeenCalledTimes(2);
    } finally {
      create.mockRestore();
    }
  });

  it("keeps namespace creation and editing unavailable to operators", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/namespaces");
    render(<App />);

    await screen.findByRole("heading", { name: "Namespaces" });
    expect(screen.queryByRole("button", { name: "Create" })).not.toBeInTheDocument();

    act(() => {
      window.history.pushState(null, "", "/namespaces/2");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });

    expect(screen.queryByRole("button", { name: "Configure" })).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Delete" })).not.toBeInTheDocument();
  });

  it("explains an operator's direct Health link without requesting administrative diagnostics", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/health");
    render(<App />);

    expect(await screen.findByRole("heading", { name: "Administrative Access Required" })).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([path]) => path === "/api/v1/cells/status")).toBe(false);
  });

  it("blocks an operator's direct job-create URL", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/jobs/create");
    render(<App />);

    expect(await screen.findByRole("heading", { name: "Job Editing Access Required" })).toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Create Job" })).not.toBeInTheDocument();
  });

  it("does not restore a remembered root scope when the account can only see a team namespace", async () => {
    scopedAPI();
    window.history.replaceState({ vectisNavigation: { key: "restricted-run-list", namespace: "/" } }, "", "/runs");
    render(<App />);

    await waitFor(() => expect(screen.getByLabelText("Namespace: /team")).toBeInTheDocument());
  });

  it("shows an access message for a forbidden Users deep link", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/users/12");
    render(<App />);

    expect(await screen.findByRole("heading", { name: "Access Required" })).toBeInTheDocument();
    expect(screen.queryByText("User Not Found")).not.toBeInTheDocument();
  });

  it("does not present forbidden namespace bindings as an empty access list", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/namespaces/2");
    render(<App />);

    expect(
      await screen.findByText("Your account cannot view the users or role bindings for this namespace.")
    ).toBeInTheDocument();

    expect(screen.queryByText("No direct role bindings.")).not.toBeInTheDocument();
    expect(screen.queryByLabelText("Namespace role bindings")).not.toBeInTheDocument();
  });

  it("shows a health request failure instead of reporting an empty cluster", async () => {
    scopedAPI(unrestrictedAccess);
    window.history.replaceState(null, "", "/health");
    render(<App />);

    expect(await screen.findByRole("heading", { name: "Unable to Load Cell Status" })).toBeInTheDocument();
    expect(screen.getByText("Permission denied")).toBeInTheDocument();
  });

  it("clears health data when signing out before a restricted account signs in", async () => {
    scopedAPI();
    const restrictedFetch = fetchMock.getMockImplementation()!;
    let healthReads = 0;
    let signedOut = false;

    fetchMock.mockImplementation((path: string, options: RequestInit) => {
      if (path === "/api/v1/cells/status" && healthReads++ === 0)
        return Promise.resolve(new Response(JSON.stringify({ cells: [] }), { status: 200 }));

      if (path === "/api/v1/access" && !signedOut)
        return Promise.resolve(new Response(JSON.stringify(unrestrictedAccess), { status: 200 }));

      if (path === "/ui/api/logout") {
        signedOut = true;
        return Promise.resolve(new Response(null, { status: 204 }));
      }

      if (path === "/ui/api/login")
        return Promise.resolve(new Response(JSON.stringify({ user_id: 2, username: "operator" }), { status: 200 }));

      return restrictedFetch(path, options);
    });

    window.history.replaceState(null, "", "/health");
    render(<App />);

    await screen.findByRole("heading", { name: "Health" });
    fireEvent.click(screen.getByRole("button", { name: "Sign out" }));

    await screen.findByRole("heading", { name: "Sign in" });
    fireEvent.change(screen.getByLabelText("Username"), { target: { value: "operator" } });
    fireEvent.change(screen.getByLabelText("Password"), { target: { value: "operator-password" } });
    fireEvent.click(screen.getByRole("button", { name: "Sign in" }));

    await screen.findByText("team-build");
    expect(window.location.pathname).toBe("/jobs");

    act(() => {
      window.history.pushState(null, "", "/health");
      window.dispatchEvent(new PopStateEvent("popstate"));
    });

    expect(await screen.findByRole("heading", { name: "Administrative Access Required" })).toBeInTheDocument();
    expect(screen.queryByRole("heading", { name: "Health" })).not.toBeInTheDocument();
  });

  it("opens profile without loading console resources", async () => {
    scopedAPI();
    window.history.replaceState(null, "", "/profile");
    render(<App />);

    expect(screen.getByRole("heading", { name: "Profile" })).toBeInTheDocument();
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith("/ui/api/context", expect.any(Object)));
    expect(fetchMock.mock.calls.every(([path]) => path === "/ui/api/context" || path === "/api/v1/access")).toBe(true);
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
    expect(fetchMock).toHaveBeenCalledWith(
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
    expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/login",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("keeps a source job visible after creation and refresh without reloading on every route", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    window.history.replaceState(null, "", "/jobs");
    let created = false;
    fetchMock.mockImplementation(async (path: string, init?: RequestInit) => {
      let body: unknown;

      if (path === "/api/v1/access") body = unrestrictedAccess;
      else if (path === "/ui/api/context") body = { auth_enabled: false, principal: { username: "local" } };
      else if (path === "/api/v1/namespaces") body = [{ id: 1, name: "/", path: "/", break_inheritance: false }];
      else if (path === "/api/v1/source-repositories")
        body = [
          {
            repository_id: "demo",
            authoring: { write_definitions: true },
            namespace: "/",
            default_ref: "HEAD",
            enabled: true
          }
        ];
      else if (path === "/api/v1/jobs" && init?.method === "POST") {
        expect(JSON.parse(String(init.body))).toMatchObject({ repository_id: "demo", job_id: "new-source-job" });
        created = true;
        body = {};
      } else if (path.startsWith("/api/v1/jobs?"))
        body = { jobs: created ? [{ job_id: "new-source-job", path: ".vectis/jobs/new-source-job.json" }] : [] };
      else if (path === "/api/v1/source-repositories/demo/jobs/new-source-job/lifecycle?namespace=%2F")
        body = {
          source_commit: "1111111111111111111111111111111111111111",
          source_definition: { root: { uses: "builtins/script", with: { script: "echo actual source" } } },
          effective_definition: { root: { uses: "builtins/script", with: { script: "echo actual source" } } }
        };
      else if (path === "/api/v1/source-repositories/demo/namespaces") body = [{ namespace: "/" }];
      else if (path.startsWith("/api/v1/jobs/new-source-job?"))
        body = {
          definition: {
            id: "new-source-job",
            root: { uses: "builtins/script", with: { script: "echo actual source" } }
          }
        };
      else if (path === "/api/v1/runs?limit=200") body = { data: [] };
      else if (path === "/api/v1/users" || path === "/api/v1/namespaces/1/bindings") body = [];
      else throw new Error(`Unexpected request: ${path}`);

      return new Response(JSON.stringify(body), { status: 200 });
    });

    const first = render(<App />);
    await screen.findByRole("button", { name: "Create" });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    fireEvent.change(screen.getByLabelText("Name"), { target: { value: "new-source-job" } });
    fireEvent.change(screen.getByLabelText("Repository ID"), { target: { value: "demo" } });
    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    expect(await screen.findByText("new-source-job")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "View new-source-job" }));
    expect(await screen.findByText(/echo actual source/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Edit source for new-source-job" }));
    expect(await screen.findByLabelText("Payload")).toHaveValue(
      JSON.stringify({ root: { uses: "builtins/script", with: { script: "echo actual source" } } }, null, 2)
    );

    expect(
      fetchMock.mock.calls.filter(([path]) => String(path).startsWith("/api/v1/jobs/new-source-job?")).length
    ).toBe(1);

    fireEvent.click(screen.getByRole("link", { name: "Runs" }));

    await screen.findByRole("heading", { name: "Runs" });
    fireEvent.click(screen.getByRole("link", { name: "Jobs" }));
    expect(await screen.findByText("new-source-job")).toBeInTheDocument();

    const consoleLoads = () => fetchMock.mock.calls.filter(([path]) => path === "/api/v1/source-repositories").length;
    expect(consoleLoads()).toBe(2);

    first.unmount();
    render(<App />);
    expect(await screen.findByText("new-source-job")).toBeInTheDocument();
    expect(consoleLoads()).toBe(3);
  });

  it.each([false, true])("waits for a source definition before configuring a job (failure=%s)", async (failed) => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    window.history.replaceState(null, "", "/jobs/remote/config");

    let finish!: (response: Response) => void;
    const definitionResponse = new Promise<Response>((resolve) => {
      finish = resolve;
    });

    fetchMock.mockImplementation(async (path: string) => {
      let body: unknown;
      if (path === "/api/v1/access") body = unrestrictedAccess;
      else if (path === "/ui/api/context") body = { auth_enabled: true, principal: { username: "admin" } };
      else if (path === "/api/v1/source-repositories")
        body = [{ repository_id: "demo", namespace: "/", default_ref: "main", authoring: { write_definitions: true } }];
      else if (path.startsWith("/api/v1/jobs?"))
        body = { jobs: [{ job_id: "remote", path: ".vectis/jobs/remote.json" }] };
      else if (path.startsWith("/api/v1/jobs/remote?")) return definitionResponse;
      else if (path === "/api/v1/runs?limit=200") body = { data: [] };
      else if (path === "/api/v1/namespaces" || path === "/api/v1/users") body = [];
      else throw new Error(`Unexpected request: ${path}`);

      return new Response(JSON.stringify(body), { status: 200 });
    });

    render(<App />);
    await screen.findByRole("heading", { name: "Loading Job Definition" });
    expect(screen.queryByLabelText("Payload")).not.toBeInTheDocument();
    expect(screen.queryByRole("button", { name: "Save" })).not.toBeInTheDocument();

    const definition = {
      id: "remote",
      root: { id: "root", uses: "builtins/script", with: { script: "echo remote definition" } }
    };

    await act(async () => {
      finish(
        new Response(JSON.stringify(failed ? { message: "Repository unavailable" } : { definition }), {
          status: failed ? 503 : 200
        })
      );
    });

    if (failed) {
      expect(await screen.findByRole("heading", { name: "Unable to Load Job Definition" })).toBeInTheDocument();
      expect(screen.getByText("Repository unavailable")).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "Save" })).not.toBeInTheDocument();
    } else {
      expect(await screen.findByLabelText("Payload")).toHaveValue(JSON.stringify({ root: definition.root }, null, 2));
    }

    expect(fetchMock.mock.calls.filter(([path]) => String(path).startsWith("/api/v1/jobs/remote?")).length).toBe(1);
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
      .mockResolvedValueOnce(new Response(JSON.stringify(unrestrictedAccess), { status: 200 }))
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            auth_enabled: true,
            principal: { kind: "local_user", username: "admin" },
            docs_url: "https://docs.example.test/"
          }),
          { status: 200 }
        )
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
          JSON.stringify([
            {
              repository_id: "vectis-local",
              namespace: "/",
              default_ref: "main",
              enabled: true
            }
          ]),
          { status: 200 }
        )
      )
      .mockResolvedValueOnce(
        new Response(JSON.stringify([{ id: 1, username: "admin", enabled: true }]), {
          status: 200
        })
      )
      .mockResolvedValueOnce(
        new Response(
          JSON.stringify({
            jobs: [{ job_id: "live-smoke", path: ".vectis/jobs/live-smoke.json" }]
          }),
          { status: 200 }
        )
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

    fireEvent.click(screen.getByText("Help"));
    expect(screen.getByRole("link", { name: "Documentation (opens in a new tab)" })).toHaveAttribute(
      "href",
      "https://docs.example.test/docs/getting-started"
    );

    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/jobs?limit=200&repository_id=vectis-local&namespace=%2F&ref=main",
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

    expect(await screen.findByRole("heading", { name: "Jobs" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs");
  });

  it("renders app routes directly after BFF authorization", async () => {
    window.history.replaceState(null, "", "/runs/run-1240");

    render(<App />);

    expect(await screen.findByRole("heading", { level: 1, name: "api-test-suite (#1240)" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Run summary" })).toBeInTheDocument();
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
    expect(await screen.findByText("prod-west")).toBeInTheDocument();
    expect(screen.getByText("Offline")).toBeInTheDocument();
  });

  it("loads health directly from cell status without console fan-out", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    window.history.replaceState(null, "", "/health");

    fetchMock
      .mockResolvedValueOnce(new Response(JSON.stringify(unrestrictedAccess), { status: 200 }))
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
    expect(await screen.findByText("local")).toBeInTheDocument();

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/ui/api/context", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(3, "/api/v1/cells/status", expect.any(Object));
  });

  it("keeps Health mounted from permission discovery through loading and background refresh", async () => {
    scopedAPI(unrestrictedAccess);
    const original = fetchMock.getMockImplementation()!;
    let grant!: (response: Response) => void;
    let finish!: (response: Response) => void;

    fetchMock.mockImplementation((path: string, options: RequestInit) => {
      if (path === "/api/v1/access")
        return new Promise<Response>((resolve) => {
          grant = resolve;
        });

      if (path === "/api/v1/cells/status")
        return new Promise<Response>((resolve) => {
          finish = resolve;
        });

      return original(path, options);
    });

    window.history.replaceState(null, "", "/health");
    render(<App />);

    const heading = screen.getByRole("heading", { name: "Health" });
    const panel = screen.getByLabelText("Cluster health");
    expect(panel).toHaveAttribute("aria-busy", "true");
    expect(screen.queryByText("No cells registered.")).not.toBeInTheDocument();

    await act(async () => grant(new Response(JSON.stringify(unrestrictedAccess), { status: 200 })));
    expect(screen.getByRole("heading", { name: "Health" })).toBe(heading);
    expect(screen.getByLabelText("Cluster health")).toBe(panel);

    const response = () =>
      new Response(JSON.stringify({ cells: [{ cell_id: "local", status: "local", queued: 0, stuck: 0 }] }), {
        status: 200
      });

    await act(async () => finish(response()));
    expect(screen.getByRole("heading", { name: "Health" })).toBe(heading);
    expect(panel).toHaveAttribute("aria-busy", "false");
    expect(screen.getByText("local")).toBeInTheDocument();

    const visit = (path: string) =>
      act(() => {
        window.history.pushState(null, "", path);
        window.dispatchEvent(new PopStateEvent("popstate"));
      });

    visit("/profile");
    visit("/health");

    const cachedHeading = screen.getByRole("heading", { name: "Health" });
    const cell = screen.getByRole("button", { name: "Inspect local" });
    expect(screen.getByLabelText("Cluster health")).toHaveAttribute("aria-busy", "false");

    await act(async () =>
      finish(new Response(JSON.stringify({ message: "Temporarily unavailable" }), { status: 503 }))
    );

    expect(screen.getByRole("heading", { name: "Health" })).toBe(cachedHeading);
    expect(screen.getByRole("button", { name: "Inspect local" })).toBe(cell);
    expect(screen.getByRole("alert")).toHaveTextContent("Temporarily unavailable");

    visit("/profile");
    expect(screen.queryByRole("alert")).not.toBeInTheDocument();
  });

  it("drills into individual cell health", async () => {
    window.history.replaceState(null, "", "/health");

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Inspect edge" }));

    expect(screen.getByRole("heading", { name: "edge" })).toBeInTheDocument();
    expect(screen.getByText("Lag 2m 14s")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/health/cell-edge");
  });

  it("keeps actual API role bindings visible through disable, activate, and deletion", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    window.history.replaceState(null, "", "/users/2");

    let enabled = true;
    let deleted = false;

    fetchMock.mockImplementation(async (path: string, options: RequestInit) => {
      const json = (body: unknown) => new Response(JSON.stringify(body), { status: 200 });

      if (path === "/api/v1/access") return json(unrestrictedAccess);
      if (path === "/ui/api/context")
        return json({ auth_enabled: true, principal: { kind: "local_user", username: "admin" } });

      if (path === "/api/v1/namespaces") return json([{ id: 1, name: "/", path: "/", break_inheritance: false }]);
      if (path === "/api/v1/source-repositories") return json([]);
      if (path === "/api/v1/runs?limit=200") return json({ data: [] });
      if (path === "/api/v1/users")
        return json([
          { id: 1, username: "admin", enabled: true },
          ...(deleted ? [] : [{ id: 2, username: "mira", enabled }])
        ]);

      if (path === "/api/v1/namespaces/1/bindings")
        return json([
          { local_user_id: 1, role: "admin" },
          { local_user_id: 2, role: "operator" }
        ]);

      if (path === "/api/v1/users/2" && options.method === "PUT") {
        enabled = JSON.parse(String(options.body)).enabled;
        return new Response(null, { status: 204 });
      }

      if (path === "/api/v1/users/2" && options.method === "DELETE") {
        deleted = true;
        return new Response(null, { status: 204 });
      }

      throw new Error(`Unexpected request: ${options.method ?? "GET"} ${path}`);
    });

    render(<App />);

    fireEvent.click(await screen.findByRole("button", { name: "Disable" }));
    await screen.findByRole("button", { name: "Enable" });
    expect(screen.getByRole("group", { name: "Role bindings" })).toHaveTextContent("Operator");
    expect(screen.getByRole("button", { name: "Update Role" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Enable" }));
    await screen.findByRole("button", { name: "Disable" });
    expect(screen.getByRole("group", { name: "Role bindings" })).toHaveTextContent("Operator");
    expect(screen.getByRole("button", { name: "Update Role" })).toBeDisabled();

    fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm removal" }));
    expect(await screen.findByRole("button", { name: "View admin" })).toHaveTextContent("Admin");
    expect(fetchMock.mock.calls.filter(([path]) => path === "/api/v1/namespaces/1/bindings")).toHaveLength(1);
  });

  it("preserves namespace roles when status and deletion return account-only rows", async () => {
    const source = dataSource.createConsoleDataSource();
    const original = await source.loadConsole();
    const accountRows = original.users.map((user) => ({
      ...user,
      role: "Unassigned" as const,
      roleBindings: undefined
    }));

    source.updateUserStatus = async (id, status) =>
      accountRows.map((user) => (user.id === id ? { ...user, status } : user));

    source.deleteUser = async (id) => accountRows.filter((user) => user.id !== id);
    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);

    try {
      window.history.replaceState(null, "", "/users/user-mira");
      render(<App />);

      fireEvent.click(await screen.findByRole("button", { name: "Disable" }));
      await screen.findByRole("button", { name: "Enable" });
      expect(screen.getByLabelText("Role bindings")).toHaveTextContent("Operator");
      expect(screen.getByLabelText("Role bindings")).toHaveTextContent("Trigger");

      fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
      fireEvent.click(screen.getByRole("button", { name: "Confirm removal" }));
      expect(await screen.findByRole("button", { name: "View admin" })).toHaveTextContent("Admin");
      expect(screen.queryByRole("button", { name: "View mira" })).not.toBeInTheDocument();
    } finally {
      create.mockRestore();
    }
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
    expect(await screen.findByRole("button", { name: "Enable" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
    expect(screen.queryByRole("dialog", { name: "Remove User" })).not.toBeInTheDocument();

    fireEvent.keyDown(screen.getByRole("button", { name: "Confirm removal" }), { key: "Escape" });
    expect(screen.getByRole("button", { name: "Remove User" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 1, name: "taylor" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
    fireEvent.click(screen.getByRole("button", { name: "Confirm removal" }));

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

  it("opens the newly triggered job run directly", async () => {
    window.history.replaceState(null, "", "/jobs?namespace=%2Fteam-a%2Fedge");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: "View docs-publish" }));
    expect(await screen.findByRole("heading", { name: "docs-publish" })).toBeInTheDocument();
    fireEvent.click(screen.getByRole("button", { name: "Run docs-publish" }));

    await waitFor(() => expect(window.location.pathname).toBe("/runs/run-1241"));
    expect(await screen.findByRole("heading", { name: "docs-publish (#1241)" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("link", { name: "Runs" }));

    expect(await screen.findByText("Run #1241")).toBeInTheDocument();
    expect(screen.getAllByText("Waiting").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Cell").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Trigger").length).toBeGreaterThan(0);
    expect(screen.getByText("UI")).toBeInTheDocument();
    expect(screen.getAllByText("Actor").length).toBeGreaterThan(0);
    expect(screen.getAllByText("Queued").length).toBeGreaterThan(0);
  });

  it("refreshes a cached queued run and its waiting graph when it has already completed", async () => {
    const source = dataSource.createConsoleDataSource();
    const data = await source.loadConsole();
    const cached = {
      ...data.runs[0],
      status: "queued" as const,
      tasks: [{ taskID: "root", taskKey: "root", name: "Root", status: "pending" as const, attempts: [] }]
    };

    source.loadConsole = async () => ({ ...data, runs: [cached] });
    const load = vi.fn().mockResolvedValue({
      ...cached,
      status: "succeeded",
      tasks: [{ ...cached.tasks[0], status: "succeeded" }]
    });

    source.loadRun = load;
    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    window.history.replaceState(null, "", `/runs/${cached.id}`);

    try {
      render(<App />);

      expect(await screen.findByRole("button", { name: "Root: Succeeded" })).toBeInTheDocument();
      expect(within(screen.getByRole("region", { name: "Run summary" })).getByText("Succeeded")).toBeInTheDocument();

      expect(load).toHaveBeenCalledOnce();
    } finally {
      create.mockRestore();
    }
  });

  it("opens a replay as a new run with a link back to the original", async () => {
    window.history.replaceState(null, "", "/runs/run-1238");
    render(<App />);
    fireEvent.click(await screen.findByRole("button", { name: "Run again" }));

    await waitFor(() => expect(window.location.pathname).toBe("/runs/run-1241"));
    expect(await screen.findByRole("link", { name: /^View original run/ })).toHaveAttribute("href", "/runs/run-1238");
    expect(screen.getByRole("button", { name: "Cancel run" })).toBeEnabled();

    fireEvent.click(screen.getByRole("link", { name: /^View original run/ }));
    await waitFor(() => expect(window.location.pathname).toBe("/runs/run-1238"));
    expect(screen.getByRole("heading", { level: 1, name: "worker-image (#1238)" })).toBeVisible();
  });

  it("does not flash a log error when the stream closes before the run watcher reports success", async () => {
    const source = dataSource.createConsoleDataSource();
    const data = await source.loadConsole();
    const original = await source.loadRun(data.runs[0].id);
    const tasks = original.tasks?.map((task) => ({
      ...task,
      status: "succeeded" as const,
      attempts: task.attempts.map((attempt) => ({ ...attempt, status: "succeeded" as const }))
    }));

    const running = { ...original, status: "running" as const, tasks };
    const finished = { ...original, status: "succeeded" as const, tasks };
    source.loadConsole = async () => ({ ...data, runs: [running] });
    source.loadRun = vi.fn().mockResolvedValueOnce(running).mockResolvedValue(finished);

    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    const sources: FakeSource[] = [];

    class FakeSource {
      onopen?: () => void;
      onerror?: () => void;
      close = vi.fn();
      constructor() {
        sources.push(this);
      }
    }

    vi.stubGlobal("EventSource", FakeSource);
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    const errors: string[] = [];
    const observer = new MutationObserver((records) => {
      for (const record of records)
        for (const node of record.addedNodes) {
          if (node.textContent?.includes("Log Stream Unavailable")) errors.push(node.textContent);
        }
    });

    try {
      window.history.replaceState(null, "", `/runs/${running.id}`);
      const view = render(<App />);
      const summary = await screen.findByRole("region", { name: "Run summary" });
      await waitFor(() => expect(source.loadRun).toHaveBeenCalledOnce());
      expect(summary).toHaveTextContent("Running");

      observer.observe(view.container, { childList: true, subtree: true });
      act(() => {
        sources.at(-1)!.onopen?.();
        sources.at(-1)!.onerror?.();
      });

      expect(screen.getByText("Live log stream closed. Waiting for run status…")).toBeVisible();
      await waitFor(() => expect(summary).toHaveTextContent("Succeeded"), { timeout: 4000 });
      expect(screen.getByText("Run succeeded. Live log stream closed.")).toBeVisible();
      expect(screen.queryByRole("alert")).not.toBeInTheDocument();
      expect(errors).toEqual([]);
    } finally {
      observer.disconnect();
      create.mockRestore();
    }
  });

  it("loads an uncached original run before navigating and leaves the replay visible on a failed read", async () => {
    const source = dataSource.createConsoleDataSource();
    const replayID = await source.replayRun("run-1238", "test-replay");
    const data = await source.loadConsole();
    source.loadConsole = async () => ({ ...data, runs: data.runs.filter((run) => run.id !== "run-1238") });

    const loadRun = source.loadRun.bind(source);
    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });

    let fail = true;
    source.loadRun = vi.fn(async (id, jobs) => {
      if (id === "run-1238") {
        if (fail) {
          fail = false;
          throw new Error("Read temporarily unavailable");
        }

        await ready;
      }

      return loadRun(id, jobs);
    });

    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    try {
      window.history.replaceState(null, "", `/runs/${replayID}`);
      render(<App />);

      const link = await screen.findByRole("link", { name: /^View original run/ });
      const summary = screen.getByRole("region", { name: "Run summary" });
      fireEvent.click(link);

      expect(await screen.findByText("Read temporarily unavailable")).toBeVisible();
      expect(window.location.pathname).toBe(`/runs/${replayID}`);
      await waitFor(() => expect(link).not.toHaveAttribute("aria-busy"));

      fireEvent.click(link);
      await waitFor(() => expect(link).toHaveAttribute("aria-busy", "true"));

      expect(screen.getByRole("region", { name: "Run summary" })).toBe(summary);
      expect(screen.queryByText("Loading Run")).not.toBeInTheDocument();

      await act(async () => release());
      await waitFor(() => expect(window.location.pathname).toBe("/runs/run-1238"));

      expect(screen.getByRole("heading", { level: 1, name: "worker-image (#1238)" })).toBeVisible();
      expect(screen.queryByText("Read temporarily unavailable")).not.toBeInTheDocument();
    } finally {
      release();
      create.mockRestore();
    }
  });

  it("keeps the original page while preparing a replay and navigates without a loading-page flash", async () => {
    const source = dataSource.createConsoleDataSource();
    const loadRun = source.loadRun.bind(source);

    let release!: () => void;
    const ready = new Promise<void>((resolve) => {
      release = resolve;
    });

    source.loadRun = vi.fn(async (id, jobs) => {
      if (id === "run-1241") await ready;
      return loadRun(id, jobs);
    });

    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    const addedContent: string[] = [];
    const observer = new MutationObserver((records) => {
      for (const record of records) {
        for (const node of record.addedNodes) addedContent.push(node.textContent ?? "");
      }
    });

    try {
      window.history.replaceState(null, "", "/runs/run-1238");
      const view = render(<App />);
      const button = await screen.findByRole("button", { name: "Run again" });
      const summary = screen.getByRole("region", { name: "Run summary" });

      observer.observe(view.container, { childList: true, subtree: true });
      fireEvent.click(button);

      await waitFor(() => expect(source.loadRun).toHaveBeenCalledWith("run-1241", expect.any(Array)));
      expect(window.location.pathname).toBe("/runs/run-1238");
      expect(screen.getByRole("region", { name: "Run summary" })).toBe(summary);
      expect(screen.getByRole("button", { name: "Starting run…" })).toBeDisabled();

      await act(async () => release());
      await waitFor(() => expect(window.location.pathname).toBe("/runs/run-1241"));
      expect(screen.getByRole("heading", { level: 1, name: "worker-image (#1239)" })).toBeVisible();
      expect(screen.getByRole("region", { name: "Run summary" })).toHaveTextContent("run-1241");
      expect(addedContent.some((text) => text.includes("Loading Run"))).toBe(false);
    } finally {
      release();
      observer.disconnect();
      create.mockRestore();
    }
  });

  it("still opens an accepted replay when its initial detail read fails", async () => {
    const source = dataSource.createConsoleDataSource();
    const loadRun = source.loadRun.bind(source);

    let failed = false;
    source.loadRun = vi.fn(async (id, jobs) => {
      if (id === "run-1241" && !failed) {
        failed = true;
        throw new Error("Temporary read failure");
      }

      return loadRun(id, jobs);
    });

    const replay = vi.spyOn(source, "replayRun");
    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    try {
      window.history.replaceState(null, "", "/runs/run-1238");
      render(<App />);

      fireEvent.click(await screen.findByRole("button", { name: "Run again" }));
      expect(await screen.findByRole("heading", { level: 1, name: "worker-image (#1239)" })).toBeVisible();
      expect(window.location.pathname).toBe("/runs/run-1241");
      expect(replay).toHaveBeenCalledOnce();
      expect(screen.queryByText("Temporary read failure")).not.toBeInTheDocument();
    } finally {
      create.mockRestore();
    }
  });

  it("opens a job detail page from the jobs list", async () => {
    window.history.replaceState(null, "", "/jobs?namespace=%2Fteam-a%2Fedge");

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

    fireEvent.click(screen.getByRole("button", { name: "Edit source for docs-publish" }));

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
    window.history.replaceState(null, "", "/runs?namespace=%2Fteam-a");

    render(<App />);

    await screen.findByRole("heading", { name: "Runs" });

    fireEvent.click(screen.getByRole("button", { name: "Open run api-test-suite #1240" }));

    expect(await screen.findByRole("heading", { level: 1, name: "api-test-suite (#1240)" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Run summary" })).toBeInTheDocument();
    expect(screen.getByText("run-1240")).toBeInTheDocument();
    expect(screen.getByText("Saved")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/runs/run-1240");
    expect(screen.getByRole("heading", { name: "Run Logs" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Timeline" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Job Definition" })).toBeInTheDocument();
    expect(screen.getByText("Submitted via Manual by mira.")).toBeInTheDocument();
    expect(screen.getByText("Run #1240 saved.")).toBeInTheDocument();
    expect(screen.getByText("Worker selected on local.")).toBeInTheDocument();
    expect(screen.getByText(/\+0s/)).toBeInTheDocument();
    expect(screen.getByText(/\+5s/)).toBeInTheDocument();
    expect(screen.getByText("Streaming")).toBeInTheDocument();

    fireEvent.click(screen.getByText("Job Definition"));
    expect(await screen.findByText(/go test \.\/internal\/api/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "api-test-suite" }));

    expect(await screen.findByRole("heading", { level: 1, name: "api-test-suite" })).toBeInTheDocument();
    expect(window.location.pathname).toBe("/jobs/job-api-test-suite");
  });

  it("submits an ephemeral run from the runs page", async () => {
    window.history.replaceState(null, "", "/runs");

    render(<App />);

    await screen.findByRole("heading", { name: "Runs" });

    fireEvent.click(screen.getByRole("button", { name: "Run Once" }));
    fireEvent.change(screen.getByLabelText("Definition JSON"), {
      target: {
        value: JSON.stringify({
          id: "database-backfill",
          root: { id: "root", uses: "builtins/script" }
        })
      }
    });

    fireEvent.click(screen.getByRole("button", { name: "Submit run" }));

    expect(await screen.findByRole("heading", { level: 1, name: "database-backfill (#1241)" })).toBeInTheDocument();
    expect(screen.getByRole("heading", { level: 2, name: "Run summary" })).toBeInTheDocument();
    expect(screen.getByText("Ephemeral")).toBeInTheDocument();
    expect(screen.getByText("Inline")).toBeInTheDocument();
    expect(screen.getByRole("heading", { name: "Submitted Definition" })).toBeInTheDocument();

    fireEvent.click(screen.getByText("Submitted Definition"));
    expect(await screen.findByText(/"id": "database-backfill"/)).toBeInTheDocument();

    fireEvent.click(screen.getByRole("link", { name: "Jobs" }));

    expect(screen.queryByText("database-backfill")).not.toBeInTheDocument();
  });

  it("keeps Run Once visible until acceptance and navigates directly from a filtered list", async () => {
    window.history.replaceState(null, "", "/runs?job=filtered-job");
    const source = dataSource.createConsoleDataSource();
    const submit = source.submitEphemeralRun;

    let accept!: () => void;
    const accepted = new Promise<void>((resolve) => {
      accept = resolve;
    });

    source.submitEphemeralRun = async (input) => {
      await accepted;
      return submit(input);
    };

    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    const navigate = vi.spyOn(window.history, "pushState");
    try {
      render(<App />);
      await screen.findByRole("heading", { name: "Runs" });
      fireEvent.click(screen.getByRole("button", { name: "Run Once" }));

      const editor = screen.getByLabelText("Definition JSON");
      fireEvent.change(editor, { target: { value: '{"id":"direct-run","root":{"uses":"builtins/script"}}' } });
      fireEvent.click(screen.getByRole("button", { name: "Submit run" }));
      expect(screen.getByLabelText("Definition JSON")).toBe(editor);
      expect(navigate).not.toHaveBeenCalled();

      await act(async () => accept());
      await screen.findByRole("heading", { level: 1, name: "direct-run (#1241)" });
      expect(navigate).toHaveBeenCalledOnce();
      expect(navigate).toHaveBeenCalledWith(
        expect.objectContaining({
          vectisNavigation: expect.objectContaining({
            runList: expect.objectContaining({ href: "/runs?job=filtered-job&namespace=%2F" })
          })
        }),
        "",
        "/runs/run-1241"
      );
    } finally {
      create.mockRestore();
      navigate.mockRestore();
    }
  });

  it("renders a run detail not found state", async () => {
    window.history.replaceState(null, "", "/runs/missing");

    render(<App />);

    expect(await screen.findByRole("heading", { level: 1, name: "Run Not Found" })).toBeInTheDocument();
    expect(screen.getByRole("region", { name: "No Run Found" })).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "View Runs" })).toBeInTheDocument();
  });

  it("creates and edits a source job in the mock", async () => {
    window.history.replaceState(null, "", "/jobs");

    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));
    fireEvent.change(screen.getByLabelText("Name"), {
      target: { value: "cache-warmup" }
    });
    fireEvent.change(screen.getByLabelText("Repository ID"), { target: { value: "vectis-local" } });

    fireEvent.change(screen.getByLabelText("Payload"), {
      target: {
        value: JSON.stringify({
          id: "cache-warmup",
          root: { id: "root", uses: "builtins/script" }
        })
      }
    });

    fireEvent.click(screen.getByRole("button", { name: "Create" }));

    expect(await screen.findByText("cache-warmup")).toBeInTheDocument();
    expect(screen.getAllByText(/vectis-local/).length).toBeGreaterThan(0);

    fireEvent.click(screen.getByRole("button", { name: "View cache-warmup" }));
    expect(await screen.findByRole("heading", { name: "cache-warmup" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Edit source for cache-warmup" }));
    expect(screen.queryByLabelText("State")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Save" }));
    expect((await screen.findAllByText("cache-warmup")).length).toBeGreaterThan(0);
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

    fireEvent.click(screen.getByLabelText("Namespace: Root"));
    fireEvent.click(screen.getByRole("treeitem", { name: "/prod" }));

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

  it("changes a password through the real API source and returns to sign-in", async () => {
    scopedAPI();
    const original = fetchMock.getMockImplementation()!;

    fetchMock.mockImplementation((path: string, options: RequestInit) => {
      if (path === "/api/v1/users/change-password" || path === "/ui/api/logout")
        return Promise.resolve(new Response(null, { status: 204 }));
      return original(path, options);
    });

    window.history.replaceState(null, "", "/profile");
    render(<App />);

    await screen.findByLabelText("Current password");
    fireEvent.change(screen.getByLabelText("Current password"), { target: { value: "current-password" } });
    fireEvent.change(screen.getByLabelText("New password"), { target: { value: "new-password" } });
    fireEvent.change(screen.getByLabelText("Confirm new password"), { target: { value: "new-password" } });
    fireEvent.click(screen.getByRole("button", { name: "Change password" }));
    expect(await screen.findByText("Password changed. Sign in with your new password.")).toBeInTheDocument();
    expect(window.location.pathname).toBe("/login");
    expect(screen.getByLabelText("Password")).toHaveValue("");
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/users/change-password",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ current_password: "current-password", new_password: "new-password" })
      })
    );

    expect(fetchMock).toHaveBeenCalledWith("/ui/api/logout", expect.objectContaining({ method: "POST" }));
  });

  it("hides Health for an admin of a child namespace", async () => {
    scopedAPI({ ...operatorAccess, namespaces: { "/team": [...operatorAccess.namespaces["/team"], "admin:*"] } });
    window.history.replaceState(null, "", "/health");
    render(<App />);

    expect(await screen.findByRole("heading", { name: "Administrative Access Required" })).toBeInTheDocument();
    expect(fetchMock.mock.calls.some(([path]) => path === "/api/v1/cells/status")).toBe(false);
    expect(screen.queryByRole("link", { name: "Namespaces" })).not.toBeInTheDocument();

    fireEvent.click(screen.getByLabelText("Namespace: /team"));
    expect(screen.getByRole("link", { name: "Manage namespaces" })).toHaveAttribute("href", "/namespaces");
  });

  it("loads repository diagnostics directly without console fan-out", async () => {
    scopedAPI(unrestrictedAccess);
    const original = fetchMock.getMockImplementation()!;

    fetchMock.mockImplementation((path: string, options: RequestInit) => {
      const json = (value: unknown) => Promise.resolve(new Response(JSON.stringify(value), { status: 200 }));
      if (path === "/api/v1/source-repositories/builds?namespace=%2Fteam")
        return json({
          repository_id: "builds",
          namespace: "/team",
          enabled: true,
          source_kind: "local_checkout",
          declared: true,
          default_ref: "main",
          sync: { status: "never" }
        });

      if (path === "/api/v1/source-repositories/builds/status?namespace=%2Fteam")
        return json({ status: "ok", default_ref_resolved: true, git_repository: true });

      if (path === "/api/v1/source-repositories/builds/jobs?limit=100&namespace=%2Fteam")
        return json({ jobs: [], resolved_commit: "abc", requested_ref: "main", truncated: false });

      return original(path, options);
    });

    window.history.replaceState(null, "", "/repositories/builds?namespace=%2Fteam");
    render(<App />);

    expect(await screen.findByText("No valid job definitions discovered.")).toBeInTheDocument();
    expect(fetchMock.mock.calls.map(([path]) => path).sort()).toEqual(
      [
        "/ui/api/context",
        "/api/v1/access",
        "/api/v1/source-repositories/builds?namespace=%2Fteam",
        "/api/v1/source-repositories/builds/status?namespace=%2Fteam",
        "/api/v1/source-repositories/builds/jobs?limit=100&namespace=%2Fteam"
      ].sort()
    );
  });

  it("opens and triggers the repository-qualified job when IDs match in different repositories", async () => {
    const source = dataSource.createConsoleDataSource();
    const snapshot = await source.loadConsole();
    const first = {
      ...snapshot.jobs[0],
      id: "build",
      name: "build",
      repository: "first",
      namespacePath: "/",
      definition: "{}"
    };

    const second = { ...first, repository: "second", sourceDetail: ".vectis/second.json" };
    source.loadConsole = async () => ({ ...snapshot, jobs: [first, second] });
    source.triggerRun = vi.fn().mockResolvedValue("new-run");

    const factory = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ auth_enabled: true, principal: { username: "admin" } }), { status: 200 })
    );

    window.history.replaceState(null, "", "/jobs/build?repository=second&namespace=%2F");
    try {
      render(<App />);

      expect(await screen.findByRole("link", { name: "second" })).toHaveAttribute(
        "href",
        "/repositories/second?namespace=%2F"
      );
      expect(screen.getAllByText(".vectis/second.json")).toHaveLength(1);

      fireEvent.click(screen.getByRole("button", { name: "Run build" }));
      await waitFor(() => expect(source.triggerRun).toHaveBeenCalledWith("build", "second", "/"));
    } finally {
      factory.mockRestore();
    }
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
    expect(screen.getByText("Auth disabled")).not.toBeVisible();
    expect(screen.queryByRole("button", { name: "Sign out" })).not.toBeInTheDocument();
  });

  it("updates run context through history without reloading the console or run", async () => {
    const source = dataSource.createConsoleDataSource();
    const data = await source.loadConsole();
    const run = data.runs.find((item) => item.id === "run-1238")!;
    const loadConsole = vi.fn().mockResolvedValue(data);
    const loadRun = vi.fn().mockResolvedValue(run);

    source.loadConsole = loadConsole;
    source.loadRun = loadRun;
    const create = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    const initial = "/runs/run-1238?task=run-1238%3Abuild&attempt=1&keep=yes#run-graph-title";

    window.history.replaceState(null, "", initial);

    try {
      render(<App />);
      const build = await screen.findByRole("button", { name: "Build image: Succeeded" });
      await waitFor(() => expect(loadRun).toHaveBeenCalledOnce());
      expect(build).toHaveAttribute("aria-pressed", "true");

      fireEvent.click(screen.getByRole("button", { name: "Scan image: Succeeded" }));
      expect(new URLSearchParams(window.location.search).get("task")).toBe("scan");
      expect(new URLSearchParams(window.location.search).get("attempt")).toBe("1");
      expect(new URLSearchParams(window.location.search).get("keep")).toBe("yes");
      expect(window.location.hash).toBe("#run-graph-title");
      expect(screen.getByRole("button", { name: "Build image: Succeeded" })).toBe(build);

      await act(async () => {
        window.history.replaceState(null, "", initial);
        window.dispatchEvent(new PopStateEvent("popstate"));
      });

      expect(build).toHaveAttribute("aria-pressed", "true");
      expect(screen.getByLabelText(/^Attempt:/)).toHaveTextContent("Attempt 1");
      expect(loadConsole).toHaveBeenCalledOnce();
      expect(loadRun).toHaveBeenCalledOnce();
    } finally {
      create.mockRestore();
    }
  });

  it("uses one workspace selector across jobs, runs, and repositories with browser history", async () => {
    window.history.replaceState(null, "", "/jobs?namespace=%2Fteam-a");
    render(<App />);

    await screen.findByText("api-test-suite");
    expect(screen.getAllByRole("button", { name: /^Namespace:/ })).toHaveLength(1);

    const runs = screen.getByRole("link", { name: "Runs" });
    expect(runs).toHaveAttribute("href", "/runs?namespace=%2Fteam-a");

    fireEvent.click(runs);
    await screen.findByRole("heading", { name: "Runs" });
    expect(screen.getByLabelText("Namespace: /team-a")).toBeInTheDocument();

    fireEvent.click(screen.getByRole("link", { name: "Repositories" }));
    await screen.findByRole("button", { name: "Inspect github.com/vectis/api" });
    expect(screen.getByLabelText("Namespace: /team-a")).toBeInTheDocument();

    act(() => window.history.back());
    await waitFor(() => expect(window.location.pathname).toBe("/runs"));
    expect(screen.getByLabelText("Namespace: /team-a")).toBeInTheDocument();
  });

  it("treats Root as its own workspace and keeps instance administration separate", async () => {
    window.history.replaceState(null, "", "/jobs?namespace=%2F");
    render(<App />);

    await screen.findByRole("heading", { name: "Jobs" });
    expect(screen.queryByText("api-test-suite")).not.toBeInTheDocument();

    fireEvent.click(screen.getByRole("link", { name: "Repository connections" }));
    await screen.findByRole("heading", { name: "Repository connections" });
    expect(screen.queryByText("Instance administration")).not.toBeInTheDocument();
    expect(screen.getByRole("navigation", { name: "Administration" })).toBeInTheDocument();
  });

  it("opens a shared job in the selected workspace and offers only its repositories for creation", async () => {
    const source = dataSource.createConsoleDataSource();
    const snapshot = await source.loadConsole();
    const shared = {
      ...snapshot.jobs[0],
      id: "shared-build",
      name: "shared-build",
      repository: "shared",
      namespacePath: "/",
      definition: "{}"
    };

    source.loadConsole = async () => ({
      ...snapshot,
      jobs: [shared, { ...shared, namespacePath: "/team-a" }],
      repositories: [
        { id: "shared", namespacePath: "/", canAuthor: true },
        { id: "shared", namespacePath: "/team-a", canAuthor: true },
        { id: "other", namespacePath: "/prod", canAuthor: true }
      ]
    });

    const factory = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    window.history.replaceState(null, "", "/jobs?namespace=%2Fteam-a");

    try {
      render(<App />);

      fireEvent.click(await screen.findByRole("button", { name: "View shared-build" }));
      expect(new URLSearchParams(window.location.search).get("namespace")).toBe("/team-a");

      fireEvent.click(screen.getByRole("navigation", { name: "Primary" }).querySelector('a[href^="/jobs"]')!);
      fireEvent.click(await screen.findByRole("button", { name: "Create" }));
      expect(screen.getByLabelText("Repository ID")).toHaveValue("shared");
      expect(screen.queryByRole("option", { name: "other" })).not.toBeInTheDocument();
    } finally {
      factory.mockRestore();
    }
  });

  it("updates effective access and opens the selected namespace without another workspace control", async () => {
    const source = dataSource.createConsoleDataSource();
    source.loadAccess = async () => ({
      actions: unrestrictedAccess.actions,
      namespaces: { "/": unrestrictedAccess.actions, "/team-a": ["job:read", "run:read"] }
    });

    const factory = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    window.history.replaceState(null, "", "/jobs?namespace=%2F");
    try {
      render(<App />);

      fireEvent.click(await screen.findByLabelText("Namespace: Root"));
      fireEvent.click(screen.getByRole("treeitem", { name: "/team-a" }));
      fireEvent.click(screen.getByLabelText("Namespace: /team-a"));
      expect(screen.getByLabelText("Effective access: Viewer")).toBeInTheDocument();
      expect(screen.getByRole("link", { name: "Manage namespaces" })).toHaveAttribute("href", "/namespaces");

      const details = screen.getByRole("link", { name: "View namespace" });
      expect(details).toHaveAttribute("href", "/namespaces?namespace=%2Fteam-a");

      fireEvent.click(details);
      await screen.findByRole("heading", { name: "/team-a" });
      expect(screen.getByText("Application delivery jobs owned by Team A.")).toBeInTheDocument();
      expect(screen.getByRole("navigation", { name: "Administration" })).toBeInTheDocument();
    } finally {
      factory.mockRestore();
    }
  });

  it("keeps namespace management out of a viewer's navigation", async () => {
    const source = dataSource.createConsoleDataSource();
    source.loadAccess = async () => ({
      actions: ["job:read", "run:read"],
      namespaces: { "/team-a": ["job:read", "run:read"] }
    });

    const factory = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    window.history.replaceState(null, "", "/jobs?namespace=%2Fteam-a");

    try {
      render(<App />);

      fireEvent.click(await screen.findByLabelText("Namespace: /team-a"));
      expect(screen.getByRole("link", { name: "View namespace" })).toBeInTheDocument();
      expect(screen.queryByRole("link", { name: "Manage namespaces" })).not.toBeInTheDocument();
      expect(screen.queryByRole("navigation", { name: "Administration" })).not.toBeInTheDocument();
    } finally {
      factory.mockRestore();
    }
  });

  it("does not offer namespace details when a scoped session only permits reading runs", async () => {
    const source = dataSource.createConsoleDataSource();
    source.loadAccess = async () => ({ actions: ["run:read"], namespaces: { "/team-a": ["run:read"] } });

    const factory = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    window.history.replaceState(null, "", "/runs?namespace=%2Fteam-a");

    try {
      render(<App />);

      fireEvent.click(await screen.findByLabelText("Namespace: /team-a"));
      expect(screen.queryByRole("link", { name: "Manage namespaces" })).not.toBeInTheDocument();
      expect(screen.getByLabelText("Effective access: Custom")).toBeInTheDocument();
      expect(screen.queryByRole("link", { name: "View namespace" })).not.toBeInTheDocument();
    } finally {
      factory.mockRestore();
    }
  });

  it("shows a missing namespace for an unknown details link instead of an unrelated namespace", async () => {
    window.history.replaceState(null, "", "/namespaces?namespace=%2Fmissing");
    render(<App />);

    await screen.findByRole("heading", { name: "Namespace Not Found" });
    expect(screen.queryByRole("heading", { name: "Namespaces" })).not.toBeInTheDocument();
  });

  it("moves a newly owned source job into one namespace without leaving duplicate catalog entries", async () => {
    const source = dataSource.createConsoleDataSource();
    const data = await source.loadConsole();
    const first = {
      ...data.jobs[0],
      id: "owned-build",
      name: "owned-build",
      repository: "monorepo",
      namespacePath: "/team-a",
      definition: '{"root":{"id":"root","uses":"builtins/script","with":{"script":"true"}}}',
      canAuthor: false
    };

    source.loadConsole = vi.fn(async () => ({ ...data, jobs: [first, { ...first, namespacePath: "/prod" }] }));
    source.loadJob = vi.fn(async (job) => job);
    let lifecycle: sourceJobs.SourceJobLifecycle = {
      source_commit: "base",
      source_definition: JSON.parse(first.definition),
      effective_definition: JSON.parse(first.definition)
    };

    const client: sourceJobs.SourceJobClient = {
      load: vi.fn(async () => structuredClone(lifecycle)),
      namespaces: vi.fn().mockResolvedValue([{ path: "/prod", label: "/prod" }]),
      assign: vi.fn(async (namespace) => {
        const registration: sourceJobs.SourceJobRegistration = {
          repository_id: "monorepo",
          job_id: "owned-build",
          namespace_id: 4,
          namespace,
          path: ".vectis/build.json",
          ref: "",
          revision: 1
        };

        lifecycle = { ...lifecycle, registration };
        return registration;
      }),
      activate: vi.fn(),
      clear: vi.fn()
    };

    const factory = vi.spyOn(dataSource, "createConsoleDataSource").mockReturnValue(source);
    const jobFactory = vi.spyOn(sourceJobs, "createSourceJobClient").mockReturnValue(client);
    fetchMock.mockImplementation(
      async () =>
        new Response(JSON.stringify({ auth_enabled: false, principal: { username: "admin" } }), { status: 200 })
    );

    window.history.replaceState(null, "", "/jobs/owned-build?repository=monorepo&namespace=%2Fteam-a");
    try {
      render(<App />);

      fireEvent.click(await screen.findByRole("button", { name: "Assign owner" }));
      await waitFor(() => expect(new URLSearchParams(window.location.search).get("namespace")).toBe("/prod"));
      expect(await screen.findByRole("heading", { level: 1, name: "owned-build" })).toBeInTheDocument();

      act(() => {
        window.history.pushState(null, "", "/jobs?namespace=%2Fteam-a");
        window.dispatchEvent(new PopStateEvent("popstate"));
      });

      expect(await screen.findByRole("heading", { name: "Create One Today" })).toBeInTheDocument();
      expect(screen.queryByRole("button", { name: "View owned-build" })).not.toBeInTheDocument();

      act(() => {
        window.history.pushState(null, "", "/jobs?namespace=%2Fprod");
        window.dispatchEvent(new PopStateEvent("popstate"));
      });

      expect(await screen.findByRole("button", { name: "View owned-build" })).toBeInTheDocument();
      expect(screen.getAllByRole("button", { name: "View owned-build" })).toHaveLength(1);
    } finally {
      jobFactory.mockRestore();
      factory.mockRestore();
    }
  });
});
