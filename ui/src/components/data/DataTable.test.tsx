import { fireEvent, render, screen } from "@testing-library/react";
import { DataTable, type DataTableColumn } from "./DataTable";

type Row = {
  id: string;
  name: string;
  status: string;
};

const columns: DataTableColumn<Row>[] = [
  { header: "Name", cell: (row) => row.name, width: "60%" },
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
    expect(screen.getByRole("columnheader", { name: "Name" })).toHaveStyle({ "--column-width": "60%" });
    expect(screen.getByRole("cell", { name: "api-test-suite" })).toBeInTheDocument();
    expect(screen.getByRole("cell", { name: "Enabled" })).toHaveAttribute("data-align", "end");
  });

  it("renders an empty row", () => {
    render(<DataTable columns={columns} emptyMessage="No jobs." getRowKey={(row) => row.id} rows={[]} />);

    expect(screen.getByRole("cell", { name: "No jobs." })).toHaveAttribute("colspan", "2");
  });

  it("marks selected rows", () => {
    render(
      <DataTable
        columns={columns}
        getRowKey={(row) => row.id}
        isRowSelected={(row) => row.id === "2"}
        rows={[
          { id: "1", name: "api-test-suite", status: "Enabled" },
          { id: "2", name: "docs-publish", status: "Paused" }
        ]}
      />
    );

    expect(screen.getByRole("row", { name: "docs-publish Paused" })).toHaveAttribute("aria-selected", "true");
    expect(screen.getByRole("row", { name: "api-test-suite Enabled" })).not.toHaveAttribute("aria-selected");
  });

  it("supports clickable rows with keyboard activation", () => {
    const onRowClick = vi.fn();

    render(
      <DataTable
        columns={columns}
        getRowActionLabel={(row) => `View ${row.name}`}
        getRowKey={(row) => row.id}
        onRowClick={onRowClick}
        rows={[
          { id: "1", name: "api-test-suite", status: "Enabled" },
          { id: "2", name: "docs-publish", status: "Paused" }
        ]}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "View api-test-suite" }));
    expect(onRowClick).toHaveBeenCalledWith({ id: "1", name: "api-test-suite", status: "Enabled" });

    fireEvent.keyDown(screen.getByRole("button", { name: "View docs-publish" }), { key: "Enter" });
    expect(onRowClick).toHaveBeenCalledWith({ id: "2", name: "docs-publish", status: "Paused" });
  });

  it("can hide the visible row indicator while keeping row clicks", () => {
    render(
      <DataTable
        columns={columns}
        getRowActionLabel={(row) => `View ${row.name}`}
        getRowKey={(row) => row.id}
        onRowClick={() => undefined}
        rows={[{ id: "1", name: "api-test-suite", status: "Enabled" }]}
        showRowIndicator={false}
      />
    );

    const row = screen.getByRole("button", { name: "View api-test-suite" });

    expect(row).toHaveAttribute("data-clickable", "true");
    expect(row).not.toHaveAttribute("data-row-indicator");
  });

  it("does not trigger row clicks from interactive cell content", () => {
    const onRowClick = vi.fn();
    const onButtonClick = vi.fn();

    render(
      <DataTable
        columns={[
          { header: "Name", cell: (row) => row.name },
          {
            header: "Actions",
            cell: () => (
              <button onClick={onButtonClick} type="button">
                Open menu
              </button>
            )
          }
        ]}
        getRowActionLabel={(row) => `View ${row.name}`}
        getRowKey={(row) => row.id}
        onRowClick={onRowClick}
        rows={[{ id: "1", name: "api-test-suite", status: "Enabled" }]}
      />
    );

    fireEvent.click(screen.getByRole("button", { name: "Open menu" }));

    expect(onButtonClick).toHaveBeenCalledOnce();
    expect(onRowClick).not.toHaveBeenCalled();
  });
});
