import type { KeyboardEvent, ReactNode } from "react";
import type { CSSProperties } from "react";
import styles from "./DataTable.module.css";

export type DataTableColumn<TRow> = {
  align?: "start" | "end";
  cell: (row: TRow) => ReactNode;
  header: string;
  hideOnMobile?: boolean;
  mobileOnly?: boolean;
  width?: CSSProperties["width"];
};

type DataTableProps<TRow> = {
  columns: DataTableColumn<TRow>[];
  emptyMessage?: string;
  getRowActionLabel?: (row: TRow) => string;
  getRowKey: (row: TRow) => string;
  isRowSelected?: (row: TRow) => boolean;
  onRowClick?: (row: TRow) => void;
  rows: TRow[];
};

function columnStyle(width: CSSProperties["width"] | undefined): CSSProperties | undefined {
  return width ? ({ "--column-width": width } as CSSProperties) : undefined;
}

export function DataTable<TRow>({
  columns,
  emptyMessage = "No records to show.",
  getRowActionLabel,
  getRowKey,
  isRowSelected,
  onRowClick,
  rows
}: DataTableProps<TRow>) {
  function handleRowKeyDown(event: KeyboardEvent<HTMLTableRowElement>, row: TRow) {
    if (!onRowClick || (event.key !== "Enter" && event.key !== " ")) {
      return;
    }

    event.preventDefault();
    onRowClick(row);
  }

  return (
    <div className={styles.root}>
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th
                data-align={column.align ?? "start"}
                data-mobile-hidden={column.hideOnMobile}
                data-mobile-only={column.mobileOnly}
                key={column.header}
                style={columnStyle(column.width)}
              >
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length > 0 ? (
            rows.map((row) => (
              <tr
                aria-label={getRowActionLabel?.(row)}
                aria-selected={isRowSelected?.(row) || undefined}
                data-clickable={onRowClick ? true : undefined}
                key={getRowKey(row)}
                onClick={onRowClick ? () => onRowClick(row) : undefined}
                onKeyDown={(event) => handleRowKeyDown(event, row)}
                role={onRowClick ? "button" : undefined}
                tabIndex={onRowClick ? 0 : undefined}
              >
                {columns.map((column) => (
                  <td
                    data-align={column.align ?? "start"}
                    data-label={column.header}
                    data-mobile-hidden={column.hideOnMobile}
                    data-mobile-only={column.mobileOnly}
                    key={column.header}
                    style={columnStyle(column.width)}
                  >
                    {column.cell(row)}
                  </td>
                ))}
              </tr>
            ))
          ) : (
            <tr>
              <td className={styles.empty} colSpan={columns.length}>
                {emptyMessage}
              </td>
            </tr>
          )}
        </tbody>
      </table>
    </div>
  );
}
