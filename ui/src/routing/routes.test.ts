import { routeFromPath, safeNextPath, safeReturnPath } from "./routes";

describe("routes", () => {
  it.each([
    ["/", "health", "/health"],
    ["/health", "health", "/health"],
    ["/runs", "runs", "/runs"],
    ["/runs/123", "runs", "/runs"],
    ["/jobs", "jobs", "/jobs"],
    ["/jobs/create", "jobs", "/jobs"],
    ["/jobs/worker-image/config", "jobs", "/jobs"],
    ["/users/1", "users", "/users"],
    ["/namespaces", "namespaces", "/namespaces"],
    ["/profile", "profile", "/profile"],
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

  it("extracts run job filters from run list search", () => {
    expect(routeFromPath("/runs", "?job=docs-publish")).toMatchObject({
      kind: "runs",
      runJobName: "docs-publish"
    });
  });

  it("extracts job editor routes", () => {
    expect(routeFromPath("/jobs/create")).toMatchObject({
      kind: "jobs",
      jobEditor: { kind: "create" }
    });

    expect(routeFromPath("/jobs/worker-image/config")).toMatchObject({
      kind: "jobs",
      jobEditor: { kind: "edit", jobID: "worker-image" }
    });
  });

  it("extracts job detail routes", () => {
    expect(routeFromPath("/jobs/worker-image")).toMatchObject({
      kind: "jobs",
      jobID: "worker-image"
    });
  });

  it("returns same-origin next paths", () => {
    expect(safeNextPath("?next=%2Fruns%2F123%3Fstatus%3Drunning%23logs", "http://ui.test")).toBe(
      "/runs/123?status=running#logs"
    );
  });

  it.each([["?next=https%3A%2F%2Fevil.test"], ["?next=%2Flogin"], ["?next=%2Fsetup"], [""]])(
    "rejects unsafe next value %s",
    (search) => {
      expect(safeNextPath(search, "http://ui.test")).toBeNull();
    }
  );

  it("returns same-origin return paths", () => {
    expect(safeReturnPath("?returnTo=%2Fjobs%2Fworker-image", "http://ui.test")).toBe("/jobs/worker-image");
  });

  it.each([["?returnTo=https%3A%2F%2Fevil.test"], ["?returnTo=%2Flogin"], ["?returnTo=%2Fsetup"], [""]])(
    "rejects unsafe return value %s",
    (search) => {
      expect(safeReturnPath(search, "http://ui.test")).toBeNull();
    }
  );
});
