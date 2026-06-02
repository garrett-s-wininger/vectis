import { fireEvent, render, screen } from "@testing-library/react";
import { JobTriggerControls, cronSpecForSchedule } from "./JobTriggerControls";

describe("JobTriggerControls", () => {
  it("reports manual, schedule, and custom cron changes", () => {
    const onManualChange = vi.fn();
    const onScheduleChange = vi.fn();
    const onCronSpecChange = vi.fn();

    render(
      <JobTriggerControls
        cronSpec="*/15 * * * *"
        manualEnabled
        onCronSpecChange={onCronSpecChange}
        onManualChange={onManualChange}
        onScheduleChange={onScheduleChange}
        schedule="Custom"
      />
    );

    fireEvent.click(screen.getByLabelText("Manual"));
    fireEvent.change(screen.getByLabelText("Schedule"), { target: { value: "Nightly" } });
    fireEvent.change(screen.getByLabelText("Cron Spec"), { target: { value: "0 4 * * *" } });

    expect(onManualChange).toHaveBeenCalledWith(false);
    expect(onScheduleChange).toHaveBeenCalledWith("Nightly");
    expect(onCronSpecChange).toHaveBeenCalledWith("0 4 * * *");
  });

  it("shows preset cron specs as disabled resolved values", () => {
    render(
      <JobTriggerControls
        cronSpec=""
        manualEnabled
        onCronSpecChange={() => undefined}
        onManualChange={() => undefined}
        onScheduleChange={() => undefined}
        schedule="Hourly"
      />
    );

    expect(screen.getByLabelText("Cron Spec")).toBeDisabled();
    expect(screen.getByLabelText("Cron Spec")).toHaveValue("0 * * * *");
    expect(cronSpecForSchedule("Custom", "*/10 * * * *")).toBe("*/10 * * * *");
  });
});
