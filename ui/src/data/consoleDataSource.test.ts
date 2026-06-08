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
      )
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    const data = await createConsoleDataSource().loadConsole();

    expect(data.namespaces).toEqual([
      {
        id: 1,
        name: "/",
        path: "/",
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
    expect(fetchMock).toHaveBeenNthCalledWith(3, "/api/v1/runs?limit=200", expect.any(Object));
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

  it("updates stored job definitions without injecting form-only description text", async () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");

    fetchMock
      .mockResolvedValueOnce(new Response(null, { status: 204 }))
      .mockResolvedValueOnce(jsonResponse([]))
      .mockResolvedValueOnce(jsonResponse({ data: [] }))
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

function jsonResponse(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: {
      "Content-Type": "application/json"
    }
  });
}
