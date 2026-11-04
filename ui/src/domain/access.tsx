import { createContext, useContext, useCallback, type ReactNode } from "react";

export type AccessAction =
  | "job:read"
  | "job:write"
  | "run:read"
  | "run:trigger"
  | "run:operator"
  | "admin:*"
  | "user:admin"
  | "api:any";

export type ConsoleAccess = { actions: AccessAction[]; namespaces: Record<string, AccessAction[]> };
const allActions: AccessAction[] = [
  "job:read",
  "job:write",
  "run:read",
  "run:trigger",
  "run:operator",
  "admin:*",
  "user:admin",
  "api:any"
];

export const unrestrictedAccess: ConsoleAccess = { actions: allActions, namespaces: { "*": allActions } };
export function hasAccess(access: ConsoleAccess | null, action: AccessAction, namespace?: string) {
  return namespace === undefined
    ? Boolean(access?.actions.includes(action))
    : Boolean((access?.namespaces[namespace] ?? access?.namespaces["*"])?.includes(action));
}

export function namespaceAccessLabel(access: ConsoleAccess | null, namespace: string): string | undefined {
  if (!access) return undefined;
  const actions = access.namespaces[namespace] ?? access.namespaces["*"] ?? [];
  const workspaceActions: AccessAction[] = [
    "job:read",
    "job:write",
    "run:read",
    "run:trigger",
    "run:operator",
    "admin:*"
  ];

  const effective = new Set(actions.filter((action) => workspaceActions.includes(action)));
  const roles: [string, AccessAction[]][] = [
    ["Admin", workspaceActions],
    ["Operator", ["job:read", "run:read", "run:trigger", "run:operator"]],
    ["Trigger", ["job:read", "run:read", "run:trigger"]],
    ["Viewer", ["job:read", "run:read"]]
  ];

  return (
    roles.find(
      ([, required]) => effective.size === required.length && required.every((action) => effective.has(action))
    )?.[0] ?? (effective.size ? "Custom" : "No access")
  );
}

const AccessContext = createContext<{ access: ConsoleAccess | null; error?: string }>({ access: unrestrictedAccess });
export function ConsoleAccessProvider({
  access,
  error,
  children
}: {
  access: ConsoleAccess | null;
  error?: string;
  children: ReactNode;
}) {
  return <AccessContext.Provider value={{ access, error }}>{children}</AccessContext.Provider>;
}

export function useConsoleAccess() {
  const { access, error } = useContext(AccessContext);
  const can = useCallback((action: AccessAction, namespace?: string) => hasAccess(access, action, namespace), [access]);
  return { can, access, ready: access !== null, error };
}
