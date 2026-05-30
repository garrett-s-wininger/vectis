import { requestJSON, requestNoContent } from "./client";

describe("API client", () => {
  const fetchMock = vi.fn();

  beforeEach(() => {
    fetchMock.mockReset();
    vi.stubGlobal("fetch", fetchMock);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it("requests JSON with same-origin credentials", async () => {
    fetchMock.mockResolvedValueOnce(new Response(JSON.stringify({ ok: true }), { status: 200 }));

    await expect(
      requestJSON<{ ok: boolean }>("/api/example", {
        method: "POST",
        body: JSON.stringify({ value: 1 })
      })
    ).resolves.toEqual({ ok: true });

    const [, init] = fetchMock.mock.calls[0];
    expect(init).toEqual(
      expect.objectContaining({
        credentials: "same-origin",
        method: "POST"
      })
    );

    expect((init.headers as Headers).get("Accept")).toBe("application/json");
    expect((init.headers as Headers).get("Content-Type")).toBe("application/json");
  });

  it("supports no-content requests", async () => {
    fetchMock.mockResolvedValueOnce(new Response(null, { status: 204 }));

    await expect(requestNoContent("/api/session", { method: "DELETE" })).resolves.toBeUndefined();
  });

  it("raises API error responses", async () => {
    fetchMock.mockResolvedValueOnce(
      new Response(
        JSON.stringify({
          code: "not_authenticated",
          message: "session required"
        }),
        { status: 401 }
      )
    );

    await expect(requestJSON("/api/protected")).rejects.toMatchObject({
      status: 401,
      code: "not_authenticated",
      message: "session required"
    });
  });

  it("uses fallback error details for non-JSON failures", async () => {
    fetchMock.mockResolvedValueOnce(new Response("nope", { status: 502 }));

    await expect(requestNoContent("/api/protected")).rejects.toMatchObject({
      status: 502,
      code: "request_failed",
      message: "Request failed"
    });
  });
});
