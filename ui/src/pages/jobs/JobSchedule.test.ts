import { cronSpecForSchedule, cronSpecFromSchedule, scheduleMode } from "./JobSchedule";

describe("JobSchedule", () => {
  it("resolves cron specs for presets and custom schedules", () => {
    expect(cronSpecForSchedule("Custom", "*/10 * * * *")).toBe("*/10 * * * *");
    expect(cronSpecForSchedule("Nightly", "")).toBe("0 0 * * *");
    expect(cronSpecFromSchedule("Cron: */15 * * * *")).toBe("*/15 * * * *");
    expect(scheduleMode("Cron: */15 * * * *")).toBe("Custom");
  });
});
