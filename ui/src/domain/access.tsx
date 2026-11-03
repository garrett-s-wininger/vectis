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
  return { can, ready: access !== null, error };
}
