import type { ReactNode } from "react";

type FilterBarProps = {
  filters: ReactNode;
  actions?: ReactNode;
};

export function FilterBar({ filters, actions }: FilterBarProps) {
  return (
    <div className="filter-bar">
      <div className="filter-bar__filters">{filters}</div>
      {actions ? <div className="filter-bar__actions">{actions}</div> : null}
    </div>
  );
}
