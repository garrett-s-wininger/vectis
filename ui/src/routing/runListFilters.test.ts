import { runListFiltersFromSearch, runListFiltersPath } from "./runListFilters";
import { routeFromPath } from "./routes";

it("restores all run filters from the URL", () => {
  expect(routeFromPath("/runs", "?job=Run+once&status=failed&source=ephemeral")).toMatchObject({
    runJobName: "Run once",
    runFilters: { job: "Run once", status: "failed", source: "ephemeral" }
  });
});

it("ignores unsupported status and source values", () => {
  expect(runListFiltersFromSearch("?status=__proto__&source=constructor")).toEqual({
    job: "",
    status: "all",
    source: "all"
  });
});

it("updates a filter without discarding other filters or unrelated URL context", () => {
  expect(
    runListFiltersPath("?keep=a%26b&job=old&status=running&source=stored", "#runs", {
      job: "Run once",
      status: "failed",
      source: "ephemeral"
    })
  ).toBe("/runs?keep=a%26b&job=Run+once&status=failed&source=ephemeral#runs");
});

it("clears all three filters in one URL update", () => {
  expect(
    runListFiltersPath("?job=build&status=failed&source=stored&keep=yes", "", {
      job: "",
      status: "all",
      source: "all"
    })
  ).toBe("/runs?keep=yes");
});

it("keeps spaces while typing a multiword job filter", () => {
  const path = runListFiltersPath("", "", { job: "Run ", status: "all", source: "all" });
  expect(runListFiltersFromSearch(new URL(path, "http://ui.test").search).job).toBe("Run ");
});
