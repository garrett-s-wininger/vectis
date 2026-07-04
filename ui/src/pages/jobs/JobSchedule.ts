const schedulePresetSpecs: Record<string, string> = {
  Hourly: "0 * * * *",
  Nightly: "0 0 * * *"
};

export function cronSpecFromSchedule(schedule: string) {
  if (!schedule.startsWith("Cron:")) {
    return schedulePresetSpecs[schedule] ?? "";
  }

  return schedule.replace(/^Cron:\s*/, "").trim() || "0 0 * * *";
}

export function cronSpecForSchedule(schedule: string, cronSpec: string) {
  return schedule === "Custom" ? cronSpec : (schedulePresetSpecs[schedule] ?? "");
}

export function scheduleMode(schedule: string) {
  return schedule.startsWith("Cron:") ? "Custom" : schedule;
}

export function schedulePresetSpec(schedule: string) {
  return schedulePresetSpecs[schedule];
}
