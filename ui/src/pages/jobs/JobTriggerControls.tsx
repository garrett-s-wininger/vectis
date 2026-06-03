import { FormField, SelectField, ToggleField } from "../../components";
import { jobScheduleOptions } from "../../domain/consoleOptions";
import styles from "./JobTriggerControls.module.css";

type JobTriggerControlsProps = {
  cronSpec: string;
  cronSpecError?: string;
  manualEnabled: boolean;
  onCronSpecChange: (cronSpec: string) => void;
  onManualChange: (enabled: boolean) => void;
  onScheduleChange: (schedule: string) => void;
  schedule: string;
};

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
  cronSpecError,
  manualEnabled,
  onCronSpecChange,
  onManualChange,
  onScheduleChange,
  schedule
}: JobTriggerControlsProps) {
  return (
    <div className={styles.triggerConfigurator}>
      <div className={styles.triggerGroup}>
        <div className={styles.triggerIntro}>
          <h4>Manual</h4>
          <p>Allow users to start this job on demand.</p>
        </div>
        <ToggleField
          checked={manualEnabled}
          hideLabel
          label="Manual"
          name="jobManualTrigger"
          offText="Off"
          onChange={onManualChange}
          onText="Allowed"
        />
      </div>
      <div className={`${styles.triggerGroup} ${styles.scheduleGroup}`}>
        <div className={styles.triggerIntro}>
          <h4>Schedule</h4>
          <p>Periodically run this job from a preset or custom cron schedule.</p>
        </div>
        <div className={styles.scheduleFields}>
          <SelectField
            label="Cadence"
            name="jobSchedule"
            onChange={(event) => onScheduleChange(event.target.value)}
            options={jobScheduleOptions}
            value={schedule}
            wide
          />
          <FormField
            disabled={schedule !== "Custom"}
            error={cronSpecError}
            label="Cron Spec"
            name="jobCronSpec"
            onChange={(event) => onCronSpecChange(event.target.value)}
            placeholder={schedule === "None" ? "No automatic schedule" : "0 0 * * *"}
            required={schedule === "Custom"}
            reserveErrorSpace
            value={cronSpecForSchedule(schedule, cronSpec)}
            wide
          />
        </div>
      </div>
    </div>
  );
}
