import { FormField, SelectField, ToggleField } from "../../components";
import { jobScheduleOptions } from "../../domain/consoleOptions";
import styles from "./JobTriggerControls.module.css";

type JobTriggerControlsProps = {
  cronSpec: string;
  manualEnabled: boolean;
  onCronSpecChange: (cronSpec: string) => void;
  onManualChange: (enabled: boolean) => void;
  onScheduleChange: (schedule: string) => void;
  schedule: string;
};

const schedulePresetSpecs: Record<string, string> = {
  Hourly: "0 * * * *",
  Nightly: "0 2 * * *"
};

export function cronSpecFromSchedule(schedule: string) {
  if (!schedule.startsWith("Cron:")) {
    return schedulePresetSpecs[schedule] ?? "";
  }

  return schedule.replace(/^Cron:\s*/, "").trim() || "0 2 * * *";
}

export function cronSpecForSchedule(schedule: string, cronSpec: string) {
  return schedule === "Custom" ? cronSpec : schedulePresetSpecs[schedule] ?? "";
}

export function scheduleMode(schedule: string) {
  return schedule.startsWith("Cron:") ? "Custom" : schedule;
}

export function schedulePresetSpec(schedule: string) {
  return schedulePresetSpecs[schedule];
}

export function JobTriggerControls({
  cronSpec,
  manualEnabled,
  onCronSpecChange,
  onManualChange,
  onScheduleChange,
  schedule
}: JobTriggerControlsProps) {
  return (
    <div className={styles.triggerConfigurator}>
      <div className={styles.triggerGroup}>
        <ToggleField
          checked={manualEnabled}
          label="Manual"
          name="jobManualTrigger"
          offText="Off"
          onChange={onManualChange}
          onText="Allowed"
        />
        <small>Allow users to start this job on demand.</small>
      </div>
      <div className={`${styles.triggerGroup} ${styles.scheduleGroup}`}>
        <div className={styles.scheduleFields}>
          <SelectField
            label="Schedule"
            name="jobSchedule"
            onChange={(event) => onScheduleChange(event.target.value)}
            options={jobScheduleOptions}
            value={schedule}
            wide
          />
          <FormField
            disabled={schedule !== "Custom"}
            label="Cron Spec"
            name="jobCronSpec"
            onChange={(event) => onCronSpecChange(event.target.value)}
            placeholder={schedule === "None" ? "No automatic schedule" : "0 2 * * *"}
            required={schedule === "Custom"}
            value={cronSpecForSchedule(schedule, cronSpec)}
            wide
          />
        </div>
        <small>Periodically run this job from a preset or custom cron schedule.</small>
      </div>
    </div>
  );
}
