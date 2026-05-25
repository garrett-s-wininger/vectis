import { render, screen } from "@testing-library/react";
import { DataTable, type DataTableColumn } from "./DataTable";

type Row = {
  id: string;
  name: string;
  status: string;
};

const columns: DataTableColumn<Row>[] = [
  { header: "Name", cell: (row) => row.name },
  { header: "Status", cell: (row) => row.status, align: "end" }
];

describe("DataTable", () => {
  it("renders rows and columns", () => {
    render(
      <DataTable
        columns={columns}
        getRowKey={(row) => row.id}
        rows={[{ id: "1", name: "api-test-suite", status: "Enabled" }]}
      />
    );

    expect(screen.getByRole("columnheader", { name: "Name" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "api-test-suite" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "Enabled" })).toHaveAttribute(
      "data-align",
      "end"
    );
  });

  it("renders an empty row", () => {
    render(
      <DataTable
        columns={columns}
        emptyMessage="No jobs."
        getRowKey={(row) => row.id}
        rows={[]}
      />
    );

    expect(screen.getByRole("cell", { name: "No jobs." })).toHaveAttribute(
      "colspan",
      "2"
    );
  });
});
