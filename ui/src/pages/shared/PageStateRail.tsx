import type { ReactNode } from "react";

type PageStateRailProps = {
  children: ReactNode;
};

export function PageStateRail({ children }: PageStateRailProps) {
  return <div className="app-state-rail">{children}</div>;
}
