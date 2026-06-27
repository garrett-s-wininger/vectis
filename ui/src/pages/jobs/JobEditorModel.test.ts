import { emptyJobForm, jobInputFromValues } from "./JobEditorModel";

describe("JobEditorModel", () => {
  it("serializes manual-only jobs for the API", () => {
    expect(jobInputFromValues({ ...emptyJobForm, manualEnabled: true, schedule: "None" })).toEqual(
      expect.objectContaining({ schedule: "Manual" })
    );
  });

  it("starts with a runnable shell definition by default", () => {
    const input = jobInputFromValues({ ...emptyJobForm, name: "hello-vectis" });
    const definition = JSON.parse(input.definition) as {
      root?: {
        uses?: string;
        with?: {
          command?: string;
        };
      };
    };

    expect(definition.root?.uses).toBe("builtins/shell");
    expect(definition.root?.with?.command).toBe("echo 'Hello from Vectis'");
  });
});
