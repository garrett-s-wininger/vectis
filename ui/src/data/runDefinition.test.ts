import { loadRunDefinition } from "./runDefinition";
it("loads the captured run definition using the authorized run endpoint", async () => {
  const fetch = vi
    .fn()
    .mockResolvedValue(
      new Response(JSON.stringify({ run_id: "run /?", definition: { root: {} }, definition_version: 2 }))
    );

  vi.stubGlobal("fetch", fetch);

  try {
    const result = await loadRunDefinition("run /?");
    expect(result.definition_version).toBe(2);
    expect(fetch).toHaveBeenCalledWith(
      "/api/v1/runs/run%20%2F%3F/definition",
      expect.objectContaining({ credentials: "same-origin" })
    );
  } finally {
    vi.unstubAllGlobals();
  }
});
