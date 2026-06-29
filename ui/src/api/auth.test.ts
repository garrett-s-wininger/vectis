import { completeSetup, loadUIContext, login, logout } from "./auth";

describe("auth API client", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("submits setup completion", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ username: "admin" }), {
        status: 200
      })
    );

    await expect(
      completeSetup({
        bootstrap_token: "bootstrap",
        admin_username: "admin",
        admin_password: "password"
      })
    ).resolves.toEqual({ username: "admin" });
    expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/setup/complete",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("submits login", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(JSON.stringify({ user_id: 7, username: "admin" }), {
        status: 200
      })
    );

    await expect(login({ username: "admin", password: "password" })).resolves.toEqual({
      user_id: 7,
      username: "admin"
    });
  });

  it("logs out without reading a response body", async () => {
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

    await expect(logout()).resolves.toBeUndefined();
    expect(fetchMock).toHaveBeenCalledWith(
      "/ui/api/logout",
      expect.objectContaining({ credentials: "same-origin", method: "POST" })
    );
  });

  it("loads UI context", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          auth_enabled: false,
          principal: { kind: "auth_disabled", username: "Anonymous" }
        }),
        { status: 200 }
      )
    );

    await expect(loadUIContext()).resolves.toEqual({
      auth_enabled: false,
      principal: { kind: "auth_disabled", username: "Anonymous" }
    });

    expect(fetchMock).toHaveBeenCalledWith("/ui/api/context", expect.objectContaining({ credentials: "same-origin" }));
  });

  it("raises API errors", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          code: "invalid_bootstrap_token",
          message: "invalid bootstrap token"
        }),
        { status: 401 }
      )
    );

    await expect(
      completeSetup({
        bootstrap_token: "wrong",
        admin_username: "admin",
        admin_password: "password"
      })
    ).rejects.toMatchObject({
      status: 401,
      code: "invalid_bootstrap_token",
      message: "invalid bootstrap token"
    });
  });
});
