import { navigateTo, returnToRunList, runListNamespace } from "./navigation";

let main: HTMLElement;
beforeEach(() => {
  window.history.replaceState(null, "", "/runs?job=worker&status=succeeded&source=stored");
  main = document.createElement("main");
  document.body.append(main);
});

afterEach(() => main.remove());

it("carries the exact list location through task selection and subsequent run navigation", () => {
  main.scrollTop = 275;
  navigateTo("/runs/run-1");
  expect(window.history.state.vectisNavigation.runList).toEqual({
    href: "/runs?job=worker&status=succeeded&source=stored",
    scroll: { top: 275, left: 0 }
  });

  main.scrollTop = 610;
  navigateTo("/runs/run-1?task=build");
  navigateTo("/runs/run-2");
  returnToRunList();

  expect(window.location.pathname + window.location.search).toBe("/runs?job=worker&status=succeeded&source=stored");
  expect(window.history.state.vectisNavigation.scroll).toEqual({ top: 275, left: 0 });
});

it("preserves the panel position for context changes within the same run", () => {
  window.history.replaceState(null, "", "/runs/run-1");
  main.scrollTop = 170;
  navigateTo("/runs/run-1?task=build&attempt=1");

  expect(window.history.state.vectisNavigation.scroll).toEqual({ top: 170, left: 0 });
});

it("replaces a search entry without losing unrelated history state", () => {
  window.history.replaceState({ custom: "kept" }, "", "/runs");
  const length = window.history.length;

  navigateTo("/runs?job=worker", { replace: true });
  expect(window.history.length).toBe(length);
  expect(window.history.state.custom).toBe("kept");
});

it.each(["https://elsewhere.test/runs", "/users", "http://["])("rejects invalid list return locations: %s", (href) => {
  window.history.replaceState(
    { vectisNavigation: { key: "invalid", runList: { href, scroll: { top: 3, left: 0 } } } },
    "",
    "/runs/run-1"
  );

  returnToRunList();
  expect(window.location.pathname + window.location.search).toBe("/runs");
});

it("retains namespace scope when returning from a run", () => {
  navigateTo("/runs/run-1", { namespace: "/prod" });
  navigateTo("/runs/run-1?task=build");

  returnToRunList();
  expect(runListNamespace()).toBe("/prod");
});
