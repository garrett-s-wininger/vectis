import type { ReactNode } from "react";

type TableActionsProps = {
  children: ReactNode;
  className?: string;
};

export function TableActions({ children, className }: TableActionsProps) {
  return <div className={`table-actions ${className ?? ""}`.trim()}>{children}</div>;
}
