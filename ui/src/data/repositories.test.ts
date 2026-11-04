import { createRepositorySource } from "./repositories";

describe("repository API source", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    vi.stubGlobal("fetch", fetchMock);
    fetchMock.mockReset();
  });

  afterEach(() => {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  });

  it("encodes repository IDs, namespace queries, and pinned pagination", async () => {
    fetchMock.mockImplementation(async () => new Response(JSON.stringify({ jobs: [] }), { status: 200 }));
    const api = createRepositorySource();
    await api.list("/team west");
    await api.get("repo/name");
    await api.status("repo/name");
    await api.jobs("repo/name", "next+cursor", "a1b2");
    await api.sync("repo/name");

    expect(fetchMock.mock.calls.map(([path]) => path)).toEqual([
      "/api/v1/source-repositories?namespace=%2Fteam+west",
      "/api/v1/source-repositories/repo%2Fname",
      "/api/v1/source-repositories/repo%2Fname/status",
      "/api/v1/source-repositories/repo%2Fname/jobs?limit=100&cursor=next%2Bcursor&ref=a1b2",
      "/api/v1/source-repositories/repo%2Fname/sync"
    ]);

    expect(fetchMock).toHaveBeenLastCalledWith(
      expect.any(String),
      expect.objectContaining({ method: "POST", credentials: "same-origin" })
    );
  });

  it("preserves API errors for permission and missing repository states", async () => {
    fetchMock.mockResolvedValue(
      new Response(JSON.stringify({ code: "authorization_denied", message: "Permission denied" }), { status: 403 })
    );

    await expect(createRepositorySource().get("private")).rejects.toMatchObject({
      code: "authorization_denied",
      status: 403
    });
  });
});
