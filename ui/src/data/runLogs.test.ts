import { parseRunLogEvent } from "./runLogs";

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
