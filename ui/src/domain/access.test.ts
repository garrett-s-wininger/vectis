import { namespaceAccessLabel, unrestrictedAccess, type AccessAction, type ConsoleAccess } from "./access";

it.each<[string, AccessAction[]]>([
  ["Admin", ["job:read", "job:write", "run:read", "run:trigger", "run:operator", "admin:*"]],
  ["Operator", ["job:read", "run:read", "run:trigger", "run:operator"]],
  ["Trigger", ["job:read", "run:read", "run:trigger"]],
  ["Viewer", ["job:read", "run:read"]],
  ["Custom", ["run:read"]],
  ["Custom", ["admin:*"]],
  ["Custom", ["job:read", "job:write", "run:read"]],
  ["No access", []]
])("describes effective %s access without expanding the permissions", (label, actions) => {
  expect(
    namespaceAccessLabel(
      { actions: unrestrictedAccess.actions, namespaces: { "/team": [...actions, "api:any"] } },
      "/team"
    )
  ).toBe(label);
});

it("uses the selected namespace instead of global permissions or roles elsewhere", () => {
  const access: ConsoleAccess = {
    actions: unrestrictedAccess.actions,
    namespaces: { "/": unrestrictedAccess.actions, "/team": ["job:read", "run:read"] }
  };

  expect(namespaceAccessLabel(access, "/team")).toBe("Viewer");
  expect(namespaceAccessLabel(access, "/missing")).toBe("No access");
  expect(namespaceAccessLabel(unrestrictedAccess, "/team")).toBe("Admin");
  expect(namespaceAccessLabel(null, "/team")).toBeUndefined();
});
