import type { ReactNode } from "react";

type TableActionsProps = {
  children: ReactNode;
};

export function TableActions({ children }: TableActionsProps) {
  return <div className="table-actions">{children}</div>;
}
