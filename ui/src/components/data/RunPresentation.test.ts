import { describe, expect, it } from "vitest";
import { runActorLabel, runDurationLabel, runTriggerLabel } from "./RunPresentation";

describe("run presentation", () => {
  it("labels run timing by status", () => {
    expect(runDurationLabel("queued")).toBe("Waiting");
    expect(runDurationLabel("running")).toBe("Elapsed");
    expect(runDurationLabel("succeeded")).toBe("Duration");
  });

  it("separates trigger source from actor principal", () => {
    expect(runTriggerLabel({ trigger: "api", submittedBy: "anonymous" })).toBe("API");
    expect(runActorLabel("anonymous")).toBe("Anonymous");
    expect(runTriggerLabel({ trigger: "ui", submittedBy: "admin" })).toBe("UI");
    expect(runActorLabel("admin")).toBe("admin");
  });

  it("infers scheduled system runs from cron principals", () => {
    expect(runTriggerLabel({ submittedBy: "cron" })).toBe("Schedule");
    expect(runActorLabel("cron")).toBe("SYSTEM");
  });
});
