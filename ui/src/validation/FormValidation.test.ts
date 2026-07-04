import { cronSpec, jsonObject, required } from "./FormValidation";

describe("form validation", () => {
  it("validates required text", () => {
    expect(required("worker-image")).toEqual({ valid: true });
    expect(required(" ")).toEqual({ message: "Enter a value.", valid: false });
  });

  it("validates JSON objects", () => {
    expect(jsonObject(`{"id":"job"}`)).toEqual({ valid: true });
    expect(jsonObject(`[1]`)).toEqual({ message: "Definition must be a JSON object.", valid: false });
    expect(jsonObject(`{`)).toEqual({ message: "Definition must be valid JSON.", valid: false });
  });

  it("validates five-field cron specs", () => {
    expect(cronSpec("*/15 0-12 * 1,6 1-5")).toEqual({ valid: true });
    expect(cronSpec("")).toEqual({ message: "Enter a cron spec.", valid: false });
    expect(cronSpec("* * *")).toEqual({
      message: "Use five cron fields: minute hour day month weekday.",
      valid: false
    });

    expect(cronSpec("60 * * * *")).toEqual({ message: "Minute must be between 0 and 59.", valid: false });
    expect(cronSpec("* 24 * * *")).toEqual({ message: "Hour must be between 0 and 23.", valid: false });
    expect(cronSpec("* * * * 8")).toEqual({ message: "Weekday must be between 0 and 7.", valid: false });
  });
});
