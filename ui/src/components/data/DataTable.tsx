import type { ReactNode } from "react";
import styles from "./DataTable.module.css";

export type DataTableColumn<TRow> = {
  align?: "start" | "end";
  cell: (row: TRow) => ReactNode;
  header: string;
};

type DataTableProps<TRow> = {
  columns: DataTableColumn<TRow>[];
  emptyMessage?: string;
  getRowKey: (row: TRow) => string;
  rows: TRow[];
};

export function DataTable<TRow>({
  columns,
  emptyMessage = "No records to show.",
  getRowKey,
  rows
}: DataTableProps<TRow>) {
  return (
    <div className={styles.root}>
      <table>
        <thead>
          <tr>
            {columns.map((column) => (
              <th data-align={column.align ?? "start"} key={column.header}>
                {column.header}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.length > 0 ? (
            rows.map((row) => (
              <tr key={getRowKey(row)}>
                {columns.map((column) => (
                  <td data-align={column.align ?? "start"} key={column.header}>
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
