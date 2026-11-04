import { createConsoleDataSource } from "./consoleDataSource";

describe("console data source", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  function restrictedConsole({ usersStatus = 403, bindingsStatus = 403, rootStatus = 403 } = {}) {
    fetchMock.mockImplementation(async (path: string) => {
      if (path === "/api/v1/namespaces")
        return jsonResponse([{ id: 2, name: "team", path: "/team", break_inheritance: false }]);

      if (path === "/api/v1/users")
        return jsonResponse(usersStatus === 200 ? [] : { message: "Users unavailable" }, { status: usersStatus });

      if (path === "/api/v1/source-repositories")
        return jsonResponse(rootStatus === 200 ? [] : { message: "Root unavailable" }, { status: rootStatus });

      if (path === "/api/v1/source-repositories?namespace=%2Fteam")
        return jsonResponse([{ repository_id: "team-repo", namespace: "/team", enabled: true }]);

      if (path.startsWith("/api/v1/jobs?"))
        return jsonResponse({ jobs: [{ job_id: "build", path: ".vectis/build.json" }] });

      if (path === "/api/v1/namespaces/2/bindings")
        return jsonResponse(bindingsStatus === 200 ? [] : { message: "Bindings unavailable" }, {
          status: bindingsStatus
        });

      if (path === "/api/v1/runs?limit=200") return jsonResponse({ data: [] });
      throw new Error(`Unexpected request: ${path}`);
    });
  }

  it("loads scoped jobs when root repositories, users, and bindings are forbidden", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    restrictedConsole();
    const data = await createConsoleDataSource().loadConsole();

    expect(data.jobs).toEqual([expect.objectContaining({ id: "build", namespacePath: "/team" })]);
    expect(data.repositories).toEqual([expect.objectContaining({ id: "team-repo" })]);
    expect(data.denied).toEqual({ users: true, bindings: ["/team"] });
    expect(data.namespaces[0].role).toBe("Not reported");
    expect(data.users).toEqual([]);
  });

  it("loads repositories from child namespaces even when root is accessible", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    restrictedConsole({ usersStatus: 200, bindingsStatus: 200, rootStatus: 200 });
    const data = await createConsoleDataSource().loadConsole();

    expect(data.jobs).toHaveLength(1);
    expect(data.denied).toEqual({ users: false, bindings: [] });
  });

  it.each([
    [{ usersStatus: 500 }, "Users unavailable"],
    [{ usersStatus: 401 }, "Users unavailable"],
    [{ bindingsStatus: 500 }, "Bindings unavailable"],
    [{ rootStatus: 500 }, "Root unavailable"]
  ])(
    "surfaces server and authentication failures rather than treating them as denied access (%j)",
    async (statuses, message) => {
      vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
      restrictedConsole(statuses);
      await expect(createConsoleDataSource().loadConsole()).rejects.toThrow(message);
    }
  );

  it("requests cancellation and creates a replay with an idempotency key", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 202 }))
      .mockResolvedValueOnce(jsonResponse({ run_id: "new-run" }));

    const source = createConsoleDataSource();
    await source.cancelRun("original/run");

    expect(await source.replayRun("original/run", "stable-key")).toBe("new-run");
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/runs/original%2Frun/cancel",
      expect.objectContaining({ method: "POST" })
    );

    const [path, options] = fetchMock.mock.calls[1];
    expect(path).toBe("/api/v1/runs/original%2Frun/replay");
    expect(new Headers(options.headers).get("Idempotency-Key")).toBe("stable-key");
  });

  it("loads stored jobs and namespaces from the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse([
          {
            id: 1,
            name: "/",
            path: "/",
            description: "Default namespace boundary.",
            break_inheritance: false
          }
        ])
      )
      .mockResolvedValueOnce(
        jsonResponse([
          {
            repository_id: "vectis-local",
            namespace: "/",
            default_ref: "main",
            enabled: true
          }
        ])
      )
      .mockResolvedValueOnce(
        jsonResponse([
          {
            id: 7,
            username: "root",
            enabled: true,
            created_at: "2026-05-30T12:00:00Z"
          }
        ])
      )
      .mockResolvedValueOnce(
        jsonResponse({
          repository_id: "vectis-local",
          requested_ref: "main",
          jobs: [{ job_id: "smoke-test", path: ".vectis/jobs/smoke-test.json" }]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse([
          {
            local_user_id: 7,
            role: "admin"
          }
        ])
      )
      .mockResolvedValueOnce(
        jsonResponse({
          data: [
            {
              run_id: "run-1",
              submitted_by: "mira",
              job_id: "smoke-test",
              namespace: "/",
              run_index: 4,
              created_at: "2026-05-31T11:59:55Z",
              definition_version: 3,
              status: "succeeded",
              started_at: "2026-05-31T12:00:00Z",
              finished_at: "2026-05-31T12:00:42Z"
            }
          ]
        })
      );

    const data = await createConsoleDataSource().loadConsole();

    expect(data.namespaces).toEqual([
      {
        id: 1,
        name: "/",
        path: "/",
        description: "Default namespace boundary.",
        breakInheritance: false,
        role: "Not reported"
      }
    ]);

    expect(data.jobs[0]).toMatchObject({
      id: "smoke-test",
      name: "smoke-test",
      namespacePath: "/",
      repository: "vectis-local",
      branch: "main",
      sourceKind: "repo",
      sourceDetail: ".vectis/jobs/smoke-test.json",
      triggers: [{ kind: "manual", detail: "On demand" }],
      lastRunStatus: "succeeded"
    });

    expect(data.runs[0]).toMatchObject({
      id: "run-1",
      jobName: "smoke-test",
      runNumber: 4,
      commit: "v3",
      createdAt: "2026-05-31T11:59:55Z",
      definitionVersion: 3,
      finishedAt: "2026-05-31T12:00:42Z",
      status: "succeeded",
      startedAt: "2026-05-31T12:00:00Z",
      submittedBy: "mira",
      trigger: "api",
      duration: "42s"
    });
    expect(data.users[0]).toMatchObject({
      id: "7",
      username: "root",
      role: "Admin",
      roleBindings: [
        {
          namespaceID: 1,
          namespacePath: "/",
          role: "Admin",
          userID: "7"
        }
      ],
      status: "active",
      tokens: 0
    });
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/v1/source-repositories", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(3, "/api/v1/users", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      "/api/v1/jobs?limit=200&repository_id=vectis-local&namespace=%2F&ref=main",
      expect.any(Object)
    );

    expect(fetchMock).toHaveBeenNthCalledWith(5, "/api/v1/namespaces/1/bindings", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(6, "/api/v1/runs?limit=200", expect.any(Object));
  });

  it("does not invent a latest run status for API jobs with no runs", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse([
          {
            id: 1,
            name: "/",
            path: "/",
            break_inheritance: false
          }
        ])
      )
      .mockResolvedValueOnce(
        jsonResponse([
          {
            repository_id: "vectis-local",
            namespace: "/",
            default_ref: "main",
            enabled: true
          }
        ])
      )
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(
        jsonResponse({
          jobs: [{ job_id: "test-run", path: ".vectis/jobs/test-run.json" }]
        })
      )
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const data = await createConsoleDataSource().loadConsole();

    expect(data.runs).toEqual([]);
    expect(data.jobs[0]).toMatchObject({
      id: "test-run",
      lastRunStatus: undefined
    });
  });

  it("loads live cell status from the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock.mockResolvedValueOnce(
      jsonResponse({
        cells: [
          {
            cell_id: "local",
            ingress_required: false,
            ingress_configured: false,
            ingress_reachable: false,
            status: "local",
            queued: 3,
            stuck: 1,
            catalog_pending: 2,
            catalog_failed: 1,
            catalog_total: 5
          },
          {
            cell_id: "edge",
            ingress_required: true,
            ingress_configured: true,
            ingress_reachable: true,
            status: "ready",
            queued: 0,
            stuck: 0,
            catalog_pending: 0,
            catalog_failed: 0,
            catalog_total: 0
          }
        ]
      })
    );

    const cells = await createConsoleDataSource().loadCells();

    expect(fetchMock).toHaveBeenCalledWith("/api/v1/cells/status", expect.any(Object));
    expect(cells[0]).toMatchObject({
      id: "local",
      name: "local",
      endpoint: "Local process",
      region: "Name",
      status: "degraded",
      activeRuns: 1,
      queueDepth: 3,
      stuckRuns: 1,
      catalogPending: 2,
      catalogFailed: 1,
      catalogTotal: 5,
      workersOnline: 0,
      workersTotal: 0
    });
    expect(cells[0].components).toEqual(
      expect.arrayContaining([
        expect.objectContaining({ label: "Ingress", state: "healthy" }),
        expect.objectContaining({ label: "Reconciler", detail: "1 stuck", state: "degraded" }),
        expect.objectContaining({ label: "Catalog", detail: "2 pending, 1 failed, 5 total", state: "degraded" })
      ])
    );
    expect(cells[1]).toMatchObject({
      endpoint: "Route configured",
      region: "Name",
      status: "healthy"
    });
  });

  it("creates stored jobs through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 201 }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().createJob({
      branch: "",
      definition: JSON.stringify({ id: "legacy-name", root: { id: "root", uses: "builtins/script" } }),
      name: "database-vacuum",
      namespacePath: "/",
      repository: "vectis-local"
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/jobs",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          namespace: "/",
          repository_id: "vectis-local",
          job_id: "database-vacuum",
          job: {
            root: { id: "root", uses: "builtins/script" }
          }
        })
      })
    );
  });

  it("rejects reusable job writes without a source repository before any request", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const input = {
      branch: "",
      definition: JSON.stringify({ root: {} }),
      name: "missing-source",
      namespacePath: "/",
      repository: " ",
      schedule: "Manual",
      status: "enabled" as const
    };
    const source = createConsoleDataSource();
    await expect(source.createJob(input)).rejects.toThrow("source repository is required");
    await expect(source.updateJob(input.name, input)).rejects.toThrow("source repository is required");
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it("creates namespaces through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(jsonResponse({ id: 2, name: "platform", path: "/platform", break_inheritance: false }))
      .mockResolvedValueOnce(
        jsonResponse([
          {
            id: 1,
            name: "/",
            path: "/",
            break_inheritance: false
          },
          {
            id: 2,
            name: "platform",
            description: "Platform-owned definitions.",
            path: "/platform",
            parent_id: 1,
            break_inheritance: false
          }
        ])
      )
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const data = await createConsoleDataSource().createNamespace({
      description: "Platform-owned definitions.",
      name: "platform",
      parentID: 1
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/namespaces",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          name: "platform",
          description: "Platform-owned definitions.",
          parent_id: 1
        })
      })
    );

    expect(data.namespaces[1]).toMatchObject({
      description: "Platform-owned definitions.",
      name: "platform",
      parentID: 1,
      path: "/platform"
    });
  });

  it("deletes namespaces through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(
        jsonResponse([
          {
            id: 1,
            name: "/",
            path: "/",
            break_inheritance: false
          }
        ])
      )
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().deleteNamespace(2);

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/namespaces/2",
      expect.objectContaining({
        method: "DELETE"
      })
    );
  });

  it("updates namespaces through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          id: 2,
          name: "platform",
          path: "/platform",
          description: "Updated platform detail.",
          parent_id: 1,
          break_inheritance: false
        })
      )
      .mockResolvedValueOnce(
        jsonResponse([
          {
            id: 1,
            name: "/",
            path: "/",
            break_inheritance: false
          },
          {
            id: 2,
            name: "platform",
            path: "/platform",
            description: "Updated platform detail.",
            parent_id: 1,
            break_inheritance: false
          }
        ])
      )
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const data = await createConsoleDataSource().updateNamespace(2, {
      description: "Updated platform detail."
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/namespaces/2",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({
          description: "Updated platform detail."
        })
      })
    );

    expect(data.namespaces[1]).toMatchObject({
      description: "Updated platform detail.",
      path: "/platform"
    });
  });

  it("updates stored job definitions without injecting form-only description text", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().updateJob("database-vacuum", {
      branch: "",
      definition: JSON.stringify({
        id: "database-vacuum",
        root: { id: "root", uses: "builtins/script" }
      }),
      name: "ignored-by-update",
      repository: "vectis-local"
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/jobs/database-vacuum?repository_id=vectis-local",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({
          root: { id: "root", uses: "builtins/script" }
        })
      })
    );
  });

  it.each([
    { run_id: "created-run", runs: [{ run_id: "another-run" }] },
    { runs: [{ run_id: "created-run" }, { run_id: "another-run" }] }
  ])("returns the triggered run ID without refreshing the console: %j", async (response) => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    fetchMock.mockResolvedValueOnce(jsonResponse(response));

    expect(await createConsoleDataSource().triggerRun("docs-publish", "registered-repo")).toBe("created-run");
    expect(fetchMock).toHaveBeenCalledTimes(1);
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/jobs/trigger/docs-publish",
      expect.objectContaining({ method: "POST", body: JSON.stringify({ repository_id: "registered-repo" }) })
    );
  });

  it("refreshes a run and tasks without rediscovering the repository catalog", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ run_id: "run-1", job_id: "job", status: "running", run_index: 1 }))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const run = await createConsoleDataSource().loadRun("run-1", []);
    expect(run.status).toBe("running");
    expect(fetchMock).toHaveBeenCalledTimes(2);
    expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/v1/runs/run-1", expect.anything());
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/v1/runs/run-1/tasks?limit=200", expect.anything());
  });

  it("submits ephemeral runs through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const definition = JSON.stringify({
      id: "ad-hoc-backfill",
      root: { id: "root", uses: "builtins/script" }
    });

    fetchMock
      .mockResolvedValueOnce(jsonResponse({ id: "ephemeral-job", run_id: "run-ephemeral-1" }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(
        jsonResponse({
          data: [
            { run_id: "unrelated-newer-run", job_id: "different-job", namespace: "/", status: "running" },
            {
              run_id: "run-ephemeral-1",
              job_id: "ephemeral-job",
              namespace: "/",
              run_index: 1,
              created_at: "2026-05-31T12:00:00Z",
              definition_version: 1,
              status: "queued",
              owning_cell: "local"
            }
          ]
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          run_id: "run-ephemeral-1",
          job_id: "ephemeral-job",
          namespace: "/",
          run_index: 1,
          created_at: "2026-05-31T12:00:00Z",
          status: "queued",
          owning_cell: "local"
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          data: [{ task_id: "task-root", task_key: "root", name: "root", status: "pending", attempts: [] }]
        })
      );

    const source = createConsoleDataSource();
    const data = await source.submitEphemeralRun({
      definition,
      namespacePath: "/platform",
      submittedBy: "admin"
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/jobs/run",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ namespace: "/platform", job: JSON.parse(definition) })
      })
    );

    expect(data.runs[0]).toMatchObject({
      id: "run-ephemeral-1",
      jobName: "ephemeral-job",
      namespacePath: "/",
      source: "ephemeral",
      status: "queued",
      submittedBy: undefined,
      trigger: "api",
      tasks: [expect.objectContaining({ taskID: "task-root" })]
    });

    expect(data.runs[1].id).toBe("unrelated-newer-run");
    expect(fetchMock).toHaveBeenCalledWith("/api/v1/runs/run-ephemeral-1/tasks?limit=200", expect.anything());

    fetchMock
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(
        jsonResponse({
          data: [
            {
              run_id: "run-ephemeral-1",
              job_id: "ephemeral-job",
              namespace: "/",
              run_index: 1,
              created_at: "2026-05-31T12:00:00Z",
              definition_version: 1,
              status: "queued",
              owning_cell: "local"
            }
          ]
        })
      );

    const reloadedData = await source.loadConsole();

    expect(reloadedData.runs[0]).toMatchObject({
      id: "run-ephemeral-1",
      source: "ephemeral"
    });
  });

  it("preserves wrapped Run Once options while applying the selected namespace", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const job = { root: { id: "root", uses: "builtins/script" } };

    fetchMock
      .mockResolvedValueOnce(jsonResponse({ id: "ephemeral-job", run_id: "run-1" }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockRejectedValueOnce(new Error("Catalog catching up"))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().submitEphemeralRun({
      namespacePath: "/team",
      definition: JSON.stringify({ job, namespace: "/other", cell_id: "worker-cell" })
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/jobs/run",
      expect.objectContaining({ body: JSON.stringify({ job, namespace: "/team", cell_id: "worker-cell" }) })
    );
  });

  it("uses the accepted run fallback when detail reads are temporarily unavailable", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const definition = JSON.stringify({
      id: "ad-hoc-backfill",
      root: { id: "root", uses: "builtins/script" }
    });

    fetchMock
      .mockResolvedValueOnce(jsonResponse({ id: "ephemeral-job", run_id: "run-ephemeral-1" }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockRejectedValueOnce(new Error("Catalog catching up"))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const data = await createConsoleDataSource().submitEphemeralRun({
      definition,
      namespacePath: "/platform",
      submittedBy: "admin"
    });

    expect(data.runs[0]).toMatchObject({
      id: "run-ephemeral-1",
      jobName: "ad-hoc-backfill",
      namespacePath: "/platform",
      source: "ephemeral",
      status: "queued",
      submittedBy: "admin",
      trigger: "ui"
    });

    expect(data.runs[0].tasks).toBeUndefined();
  });

  it("creates users with the selected namespace role through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    fetchMock.mockImplementation(async (path: string, options: RequestInit) => {
      if (path === "/api/v1/users" && options.method === "POST")
        return jsonResponse({ id: 12, username: "taylor", enabled: true, initial_password: "generated-secret" });

      if (path === "/api/v1/namespaces") return jsonResponse([{ id: 2, name: "team", path: "/team" }]);
      if (path === "/api/v1/users") return jsonResponse([{ id: 12, username: "taylor", enabled: true }]);
      if (path.startsWith("/api/v1/source-repositories")) return jsonResponse([]);
      if (path === "/api/v1/namespaces/2/bindings") return jsonResponse([{ local_user_id: 12, role: "operator" }]);
      if (path === "/api/v1/runs?limit=200") return jsonResponse({ data: [] });

      throw new Error(`Unexpected request: ${path}`);
    });

    const result = await createConsoleDataSource().createUser({ username: "taylor", namespaceID: 2, role: "Operator" });
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/users",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({ username: "taylor", initial_binding: { namespace_id: 2, role: "operator" } })
      })
    );

    expect(result.credential).toEqual({ password: "generated-secret", username: "taylor" });
    expect(result.users[0]).toMatchObject({
      id: "12",
      username: "taylor",
      status: "active",
      role: "Operator",
      roleBindings: [expect.objectContaining({ namespaceID: 2, role: "Operator" })]
    });
  });

  it("updates a binding in place rather than revoking it first", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    fetchMock
      .mockResolvedValueOnce(jsonResponse({ local_user_id: 12, role: "viewer" }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().grantRoleBinding("12", 2, "Viewer", "Operator");
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/namespaces/2/bindings/12",
      expect.objectContaining({ method: "PATCH", body: JSON.stringify({ previous_role: "operator", role: "viewer" }) })
    );

    expect(fetchMock.mock.calls.some(([, options]) => options.method === "DELETE")).toBe(false);
  });

  it("updates user status through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(jsonResponse([{ id: 12, username: "taylor", enabled: false }]));

    const users = await createConsoleDataSource().updateUserStatus("12", "disabled");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/users/12",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({
          enabled: false
        })
      })
    );

    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/v1/users", expect.any(Object));
    expect(users[0]).toMatchObject({
      id: "12",
      status: "disabled"
    });
  });

  it("deletes users through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock.mockResolvedValueOnce(new Response(null, { status: 204 })).mockResolvedValueOnce(jsonResponse([]));

    await createConsoleDataSource().deleteUser("12");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/users/12",
      expect.objectContaining({
        method: "DELETE"
      })
    );

    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/v1/users", expect.any(Object));
  });

  it("grants role bindings through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(jsonResponse({ local_user_id: 12, role: "operator" }, { status: 201 }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().grantRoleBinding("12", 2, "Operator");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/namespaces/2/bindings",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          local_user_id: 12,
          role: "operator"
        })
      })
    );
  });

  it("revokes role bindings through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().revokeRoleBinding("12", 2, "Viewer");

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/namespaces/2/bindings/12?role=viewer",
      expect.objectContaining({
        method: "DELETE"
      })
    );
  });

  it.each([200, 403])("loads an inline run when root repository discovery returns %i", async (status) => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          run_id: "run-inline-1",
          job_id: "inline-job",
          namespace: "/",
          run_index: 1,
          status: "queued",
          created_at: "2026-05-31T12:00:00Z",
          definition_version: 1,
          definition: {
            id: "ad-hoc-backfill",
            root: { id: "root", uses: "builtins/script" }
          },
          owning_cell: "local"
        })
      )
      .mockResolvedValueOnce(jsonResponse(status === 200 ? [] : { message: "Root unavailable" }, { status }))
      .mockResolvedValueOnce(
        jsonResponse({
          data: [
            {
              task_id: "run-inline-1:root",
              task_key: "root",
              name: "ad-hoc-backfill",
              status: "pending",
              attempts: [
                {
                  attempt: 1,
                  attempt_id: "run-inline-1:root:attempt:1",
                  cell_id: "local",
                  execution_id: "run-inline-1:root:execution:1",
                  execution_status: "accepted",
                  started_at: "2026-05-31T12:00:03Z",
                  status: "failed"
                },
                {
                  attempt: 2,
                  attempt_id: "run-inline-1:root:attempt:2",
                  cell_id: "local",
                  execution_id: "run-inline-1:root:execution:2",
                  execution_status: "accepted",
                  status: "accepted"
                }
              ]
            }
          ]
        })
      );

    const run = await createConsoleDataSource().loadRun("run-inline-1");

    expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/v1/runs/run-inline-1", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/v1/source-repositories", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(3, "/api/v1/runs/run-inline-1/tasks?limit=200", expect.any(Object));
    expect(run).toMatchObject({
      id: "run-inline-1",
      jobName: "ad-hoc-backfill",
      namespacePath: "/",
      source: "ephemeral",
      status: "queued",
      definitionVersion: 1,
      tasks: [
        {
          attempts: [
            {
              attempt: 1,
              attemptID: "run-inline-1:root:attempt:1",
              cellID: "local",
              executionID: "run-inline-1:root:execution:1",
              executionStatus: "accepted",
              startedAt: "2026-05-31T12:00:03Z",
              status: "failed"
            },
            {
              attempt: 2,
              attemptID: "run-inline-1:root:attempt:2",
              executionID: "run-inline-1:root:execution:2",
              status: "accepted"
            }
          ],
          name: "ad-hoc-backfill",
          status: "pending",
          taskID: "run-inline-1:root",
          taskKey: "root"
        }
      ]
    });
    expect(run.definition).toContain('"id": "ad-hoc-backfill"');
  });
});

function jsonResponse(body: unknown, init?: ResponseInit) {
  return new Response(JSON.stringify(body), {
    status: init?.status ?? 200,
    headers: {
      "Content-Type": "application/json",
      ...init?.headers
    },
    statusText: init?.statusText
  });
}
