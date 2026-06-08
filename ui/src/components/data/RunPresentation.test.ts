import { describe, expect, it } from "vitest";
import { runActorLabel, runDisplayName, runDurationLabel, runTriggerLabel } from "./RunPresentation";

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

  it("uses a readable label for generated inline run names", () => {
    expect(
      runDisplayName({
        id: "run-2a5ed99c-6d51-4c50-97e9-e31b23da7469",
        jobName: "2a5ed99c-6d51-4c50-97e9-e31b23da7469",
        source: "ephemeral"
      })
    ).toBe("97e9-e31b23da7469");
    expect(runDisplayName({ jobName: "database-backfill", source: "ephemeral" })).toBe("database-backfill");
    expect(runDisplayName({ jobName: "2a5ed99c-6d51-4c50-97e9-e31b23da7469", source: "stored" })).toBe(
      "2a5ed99c-6d51-4c50-97e9-e31b23da7469"
    );
  });
});
