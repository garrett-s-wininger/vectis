import { artifactDownloadURL, artifactSizeLabel, loadRunArtifactPage } from "./runArtifacts";

afterEach(() => vi.unstubAllGlobals());

it("maps manifest fields and sends escaped pagination/filter queries with credentials and cancellation", async () => {
  const fetch = vi.fn().mockResolvedValue(
    new Response(
      JSON.stringify({
        data: [
          {
            id: 42,
            name: "report",
            path: "reports/test.txt",
            size_bytes: 1024,
            content_type: "text/plain",
            cell_id: "local",
            task_id: "run:report",
            task_attempt_id: "attempt-2",
            execution_id: "execution-2"
          }
        ],
        next_cursor: 42
      })
    )
  );

  vi.stubGlobal("fetch", fetch);
  const controller = new AbortController();
  const result = await loadRunArtifactPage("run /?", {
    cursor: 7,
    taskID: "run:report /",
    attempt: 2,
    signal: controller.signal
  });

  expect(fetch).toHaveBeenCalledWith(
    "/api/v1/runs/run%20%2F%3F/artifacts?limit=100&cursor=7&task_id=run%3Areport+%2F&attempt=2",
    expect.objectContaining({ credentials: "same-origin", signal: controller.signal })
  );

  expect(result).toEqual({
    data: [
      {
        id: 42,
        name: "report",
        path: "reports/test.txt",
        sizeBytes: 1024,
        contentType: "text/plain",
        cellID: "local",
        taskID: "run:report",
        taskAttemptID: "attempt-2",
        executionID: "execution-2"
      }
    ],
    nextCursor: 42
  });
});

it("uses the authenticated download route and encodes artifact names as one segment", () => {
  expect(artifactDownloadURL("run/1", 'a/b ?#".txt')).toBe(
    "/api/v1/runs/run%2F1/artifacts/a%2Fb%20%3F%23%22.txt/download"
  );

  expect(artifactSizeLabel(0)).toBe("0 B");
  expect(artifactSizeLabel(1024)).toBe("1 KiB");
  expect(artifactSizeLabel(1048576)).toBe("1 MiB");
});
