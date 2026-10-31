import { emptyJobForm, jobInputFromValues, valuesFromJob } from "./JobEditorModel";
import { createMockConsoleDataSnapshot } from "../../mocks/consoleData";

describe("JobEditorModel", () => {
  it("submits only source-authoring fields", () => {
    expect(jobInputFromValues({ ...emptyJobForm, name: "build", repository: "demo" })).toEqual({
      name: "build",
      repository: "demo",
      branch: "",
      definition: emptyJobForm.definition
    });
  });
  it("uses the loaded source definition when configuring a job", () => {
    const job = createMockConsoleDataSnapshot().jobs[0];
    const definition = JSON.parse(valuesFromJob(job).definition);
    expect(definition).not.toHaveProperty("id");
    expect(definition.root).toEqual(JSON.parse(job.definition!).root);
    expect(valuesFromJob({ ...job, definition: undefined }).definition).toBe("");
  });
});
