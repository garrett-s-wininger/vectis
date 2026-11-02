import { parseRunLogEvent, streamRunLogs } from "./runLogs";

describe("run logs", () => {
  it("parses stdout log events", () => {
    expect(
      parseRunLogEvent(
        JSON.stringify({
          timestamp: "2026-05-31T12:00:00Z",
          stream: 0,
          sequence: 12,
          data: "hello"
        })
      )
    ).toEqual({
      timestamp: "2026-05-31T12:00:00Z",
      stream: "stdout",
      sequence: 12,
      data: "hello"
    });
  });

  it("parses stderr and control streams", () => {
    expect(parseRunLogEvent(JSON.stringify({ stream: 1, sequence: 13, data: "failed" }))).toMatchObject({
      stream: "stderr",
      data: "failed"
    });

    expect(parseRunLogEvent(JSON.stringify({ stream: 2, sequence: 14, data: '{"event":"completed"}' }))).toMatchObject({
      stream: "control"
    });
  });

  it("ignores malformed events", () => {
    expect(parseRunLogEvent("{bad")).toBeNull();
    expect(parseRunLogEvent(JSON.stringify({ sequence: 1 }))).toBeNull();
  });
});

describe("log stream completion", () => {
  afterEach(() => {
    vi.unstubAllGlobals();
    vi.unstubAllEnvs();
  });

  it.each([
    { stream: 2, data: '{"event":"completed"}' },
    { stream: 2, completed: 1, data: "" }
  ])("does not report normal stream closure as an error: %j", (completion) => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const sources: FakeSource[] = [];

    class FakeSource {
      onmessage?: (event: { data: string }) => void;
      onerror?: () => void;
      close = vi.fn();
      constructor() {
        sources.push(this);
      }
    }

    vi.stubGlobal("EventSource", FakeSource);
    const error = vi.fn();
    const complete = vi.fn();
    const stop = streamRunLogs("run-1", vi.fn(), error, complete);
    const source = sources[0];

    source.onmessage!({ data: JSON.stringify(completion) });
    source.onerror!();
    stop();

    expect(error).not.toHaveBeenCalled();
    expect(complete).toHaveBeenCalledOnce();
    expect(source.close).toHaveBeenCalledOnce();
  });

  it("still reports a connection failure before completion", () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const sources: FakeSource[] = [];

    class FakeSource {
      onerror?: () => void;
      close = vi.fn();
      constructor() {
        sources.push(this);
      }
    }

    vi.stubGlobal("EventSource", FakeSource);

    const error = vi.fn();
    streamRunLogs("run-1", vi.fn(), error);
    sources[0].onerror!();

    expect(error).toHaveBeenCalledWith("Log stream is unavailable.", false);
  });

  it("keeps the run stream open after a task finishes", () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const sources: FakeSource[] = [];
    const close = vi.fn();

    class FakeSource {
      onmessage?: (event: { data: string }) => void;
      onerror?: () => void;
      close = close;
      constructor() {
        sources.push(this);
      }
    }

    vi.stubGlobal("EventSource", FakeSource);
    const onEntry = vi.fn();
    const stop = streamRunLogs("run-1", onEntry, vi.fn());
    sources[0].onmessage!({
      data: JSON.stringify({ stream: 2, execution_id: "execution-1", data: '{"event":"completed"}' })
    });

    expect(close).not.toHaveBeenCalled();

    sources[0].onmessage!({ data: JSON.stringify({ stream: 0, execution_id: "execution-2", data: "later output" }) });
    expect(onEntry).toHaveBeenCalledWith(expect.objectContaining({ executionID: "execution-2", data: "later output" }));

    sources[0].onmessage!({ data: JSON.stringify({ stream: 2, data: '{"event":"completed"}' }) });
    expect(close).toHaveBeenCalledOnce();

    stop();
  });

  it("distinguishes an established stream closing from a failed initial connection", () => {
    vi.stubEnv("VITE_CONSOLE_DATA_SOURCE", "api");
    const sources: FakeSource[] = [];

    class FakeSource {
      onopen?: () => void;
      onerror?: () => void;
      close = vi.fn();
      constructor() {
        sources.push(this);
      }
    }

    vi.stubGlobal("EventSource", FakeSource);

    const error = vi.fn();
    streamRunLogs("cancelled-run", vi.fn(), error);
    sources[0].onopen!();
    sources[0].onerror!();

    expect(error).toHaveBeenCalledWith("Log stream is unavailable.", true);
    expect(sources[0].close).toHaveBeenCalledOnce();
  });

  it("rejects null events and preserves execution IDs", () => {
    expect(parseRunLogEvent("null")).toBeNull();
    expect(parseRunLogEvent(JSON.stringify({ data: "x", execution_id: "execution-1" }))).toMatchObject({
      executionID: "execution-1"
    });
  });
});
