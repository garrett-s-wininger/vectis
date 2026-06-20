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
        jsonResponse({
          data: [
            {
              name: "smoke-test",
              namespace: "/",
              definition: {
                description: "Runs smoke tests before deployment.",
                id: "smoke-test",
                root: {
                  id: "root",
                  uses: "builtins/shell"
                }
              }
            }
          ]
        })
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
        role: "Admin"
      }
    ]);

    expect(data.jobs[0]).toMatchObject({
      id: "smoke-test",
      name: "smoke-test",
      namespacePath: "/",
      description: "Runs smoke tests before deployment.",
      sourceKind: "db",
      sourceDetail: "Stored in Vectis",
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
      submittedBy: "anonymous",
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
    expect(fetchMock).toHaveBeenNthCalledWith(3, "/api/v1/users", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(4, "/api/v1/namespaces/1/bindings", expect.any(Object));
    expect(fetchMock).toHaveBeenNthCalledWith(5, "/api/v1/runs?limit=200", expect.any(Object));
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
        jsonResponse({
          data: [
            {
              name: "test-run",
              namespace: "/",
              definition: {
                id: "test-run",
                root: {
                  id: "root",
                  uses: "builtins/shell"
                }
              }
            }
          ]
        })
      )
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const data = await createConsoleDataSource().loadConsole();

    expect(data.runs).toEqual([]);
    expect(data.jobs[0]).toMatchObject({
      id: "test-run",
      lastRunStatus: undefined
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
      definition: JSON.stringify({ root: { id: "root", uses: "builtins/shell" } }),
      description: "Shown in the editor, not written into the definition.",
      name: "database-vacuum",
      namespacePath: "/",
      repository: "",
      schedule: "Manual",
      status: "enabled"
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/jobs",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          namespace: "/",
          job: {
            root: { id: "root", uses: "builtins/shell" },
            id: "database-vacuum"
          }
        })
      })
    );
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
            path: "/platform",
            description: "Platform-owned definitions.",
            parent_id: 1,
            break_inheritance: false
          }
        ])
      )
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
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
        root: { id: "root", uses: "builtins/shell" }
      }),
      description: "Do not write this back into JSON.",
      name: "ignored-by-update",
      repository: "",
      schedule: "Manual",
      status: "enabled"
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/jobs/database-vacuum",
      expect.objectContaining({
        method: "PUT",
        body: JSON.stringify({
          root: { id: "root", uses: "builtins/shell" },
          id: "database-vacuum"
        })
      })
    );
  });

  it("submits ephemeral runs through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const definition = JSON.stringify({
      id: "ad-hoc-backfill",
      root: { id: "root", uses: "builtins/shell" }
    });

    fetchMock
      .mockResolvedValueOnce(jsonResponse({ id: "ephemeral-job", run_id: "run-ephemeral-1" }))
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
        body: definition
      })
    );

    expect(data.runs[0]).toMatchObject({
      id: "run-ephemeral-1",
      jobName: "ephemeral-job",
      namespacePath: "/",
      source: "ephemeral",
      status: "queued",
      submittedBy: "anonymous",
      trigger: "api"
    });

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

  it("uses the post-submit fallback only when the runs endpoint has not caught up", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const definition = JSON.stringify({
      id: "ad-hoc-backfill",
      root: { id: "root", uses: "builtins/shell" }
    });

    fetchMock
      .mockResolvedValueOnce(jsonResponse({ id: "ephemeral-job", run_id: "run-ephemeral-1" }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
      .mockResolvedValueOnce(jsonResponse([]))
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
  });

  it("creates users through the API source", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(
        jsonResponse({
          id: 12,
          username: "taylor",
          enabled: true,
          initial_password: "generated-secret"
        })
      )
      .mockResolvedValueOnce(jsonResponse([{ id: 12, username: "taylor", enabled: true }]));

    const result = await createConsoleDataSource().createUser({
      username: "taylor",
      role: "Viewer"
    });

    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/users",
      expect.objectContaining({
        method: "POST",
        body: JSON.stringify({
          username: "taylor"
        })
      })
    );

    expect(fetchMock).toHaveBeenNthCalledWith(2, "/api/v1/users", expect.any(Object));
    expect(result.credential).toEqual({
      password: "generated-secret",
      username: "taylor"
    });
    expect(result.users[0]).toMatchObject({
      id: "12",
      username: "taylor",
      status: "active"
    });
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

    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(jsonResponse([]));

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

  it("loads an inline run by id through the API source", async () => {
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
            root: { id: "root", uses: "builtins/shell" }
          },
          owning_cell: "local"
        })
      )
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const run = await createConsoleDataSource().loadRun("run-inline-1");

    expect(fetchMock).toHaveBeenNthCalledWith(1, "/api/v1/runs/run-inline-1", expect.any(Object));
    expect(run).toMatchObject({
      id: "run-inline-1",
      jobName: "ad-hoc-backfill",
      namespacePath: "/",
      source: "ephemeral",
      status: "queued",
      definitionVersion: 1
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
