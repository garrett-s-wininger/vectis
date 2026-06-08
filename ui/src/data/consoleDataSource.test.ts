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
        breakInheritance: false,
        role: "Admin"
      }
    ]);

    expect(data.jobs[0]).toMatchObject({
      id: "smoke-test",
      name: "smoke-test",
      namespacePath: "/",
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
      .mockResolvedValueOnce(jsonResponse({ data: [] }));

    await createConsoleDataSource().createJob({
      branch: "",
      definition: JSON.stringify({ root: { id: "root", uses: "builtins/shell" } }),
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
});

function jsonResponse(body: unknown) {
  return new Response(JSON.stringify(body), {
    status: 200,
    headers: {
      "Content-Type": "application/json"
    }
  });
}
