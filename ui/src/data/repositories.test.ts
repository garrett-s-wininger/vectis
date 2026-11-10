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

it("sends public connection registration and edits as scoped JSON mutations", async () => {
  vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
  const fetchMock = vi
    .fn()
    .mockImplementation(async () => new Response(JSON.stringify({ repository_id: "public" }), { status: 200 }));

  vi.stubGlobal("fetch", fetchMock);

  try {
    const api = createRepositorySource(undefined, true);
    const settings = {
      canonical_url: "https://example.test/public.git",
      default_ref: "HEAD",
      credential_ref: "",
      enabled: true
    };

    await api.create({
      ...settings,
      repository_id: "public",
      namespace: "/team",
      source_kind: "local_checkout",
      checkout_mode: "managed",
      authoring_mode: "read_only"
    });

    await api.update("repo/name", settings);
    await api.validate({
      ...settings,
      repository_id: "public",
      namespace: "/team",
      source_kind: "local_checkout",
      checkout_mode: "managed",
      authoring_mode: "read_only"
    });

    await api.validate(settings, "repo/name");
    expect(fetchMock).toHaveBeenNthCalledWith(
      1,
      "/api/v1/source-repositories",
      expect.objectContaining({ method: "POST", body: expect.stringContaining('"credential_ref":""') })
    );

    expect(fetchMock).toHaveBeenNthCalledWith(
      2,
      "/api/v1/source-repositories/repo%2Fname?scope=all",
      expect.objectContaining({ method: "PUT", body: JSON.stringify(settings) })
    );

    expect(fetchMock).toHaveBeenNthCalledWith(
      3,
      "/api/v1/source-repositories",
      expect.objectContaining({ method: "POST", body: expect.stringContaining('"validate_only":true') })
    );

    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      "/api/v1/source-repositories/repo%2Fname?scope=all",
      expect.objectContaining({ method: "PUT", body: JSON.stringify({ ...settings, validate_only: true }) })
    );
  } finally {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  }
});

it("deletes encoded connection IDs with a bodyless request and accepts 204", async () => {
  vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
  const fetchMock = vi.fn().mockResolvedValue(new Response(null, { status: 204 }));
  vi.stubGlobal("fetch", fetchMock);

  try {
    await createRepositorySource(undefined, true).remove("repo/name");
    expect(fetchMock).toHaveBeenCalledWith(
      "/api/v1/source-repositories/repo%2Fname?scope=all",
      expect.objectContaining({ method: "DELETE", credentials: "same-origin" })
    );

    expect(fetchMock.mock.calls[0][1].body).toBeUndefined();
  } finally {
    vi.unstubAllEnvs();
    vi.unstubAllGlobals();
  }
});
