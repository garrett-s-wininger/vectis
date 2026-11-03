import { runInvestigationFromSearch, runInvestigationPath } from "./runInvestigation";
import { routeFromPath } from "./routes";

describe("run investigation URLs", () => {
  it("defaults to the whole run and follows the latest attempt", () => {
    expect(runInvestigationFromSearch("")).toEqual({ attempt: "latest", task: undefined });
    expect(runInvestigationFromSearch("?attempt=latest&task=")).toEqual({ attempt: "latest", task: undefined });
  });

  it("restores run-scoped task keys and pins a specific attempt", () => {
    const task = "fanout/build + checks:linux/arm&64?#";
    const search = `?task=${encodeURIComponent(task)}&attempt=2`;
    expect(routeFromPath("/runs/run-1", search).runInvestigation).toEqual({ task, attempt: 2 });
    expect(runInvestigationPath("/runs/run-1", "", "", { task, attempt: 2 })).toBe(
      `/runs/run-1?${new URLSearchParams({ task: task, attempt: "2" })}`
    );
  });

  it.each(["0", "-1", "1.5", "Infinity", "NaN", "1e2", "01", "9007199254740992", "invalid"])(
    "ignores invalid attempt %s without losing the selected task",
    (attempt) => {
      expect(runInvestigationFromSearch(`?task=build&attempt=${attempt}`)).toEqual({
        task: "build",
        attempt: "latest"
      });
    }
  );

  it("clears just the investigation fields and preserves other query parameters and the fragment", () => {
    expect(
      runInvestigationPath("/runs/run-1", "?task=build&attempt=1&other=a%26b", "#logs", { attempt: "latest" })
    ).toBe("/runs/run-1?other=a%26b#logs");
  });

  it("replaces existing context without duplicate parameters", () => {
    const path = runInvestigationPath("/runs/run-1", "?task=old&task=duplicate&attempt=1&attempt=3", "", {
      task: "new",
      attempt: 2
    });

    expect(path).toBe("/runs/run-1?task=new&attempt=2");
  });
});
