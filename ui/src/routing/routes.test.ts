import { routeFromPath, safeNextPath } from "./routes";

describe("routes", () => {
  it.each([
    ["/", "health", "/health"],
    ["/health", "health", "/health"],
    ["/runs", "runs", "/runs"],
    ["/runs/123", "runs", "/runs"],
    ["/jobs", "jobs", "/jobs"],
    ["/users/1", "users", "/users"],
    ["/namespaces", "namespaces", "/namespaces"],
    ["/setup", "setup", ""],
    ["/login", "login", ""],
    ["/missing", "notFound", ""]
  ])("maps %s", (pathname, kind, activeHref) => {
    expect(routeFromPath(pathname)).toMatchObject({ kind, activeHref });
  });

  it("extracts run IDs from run detail paths", () => {
    expect(routeFromPath("/runs/run-1240")).toMatchObject({
      kind: "runs",
      runID: "run-1240"
    });
  });

  it("returns same-origin next paths", () => {
    expect(
      safeNextPath("?next=%2Fruns%2F123%3Fstatus%3Drunning%23logs", "http://ui.test")
    ).toBe("/runs/123?status=running#logs");
  });

  it.each([
    ["?next=https%3A%2F%2Fevil.test"],
    ["?next=%2Flogin"],
    ["?next=%2Fsetup"],
    [""]
  ])("rejects unsafe next value %s", (search) => {
    expect(safeNextPath(search, "http://ui.test")).toBeNull();
  });
});
