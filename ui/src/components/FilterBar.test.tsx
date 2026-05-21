import { render, screen } from "@testing-library/react";
import { Button } from "./Button";
import { FilterBar } from "./FilterBar";

describe("FilterBar", () => {
  it("renders filter controls and actions", () => {
    render(
      <FilterBar
        actions={<Button>Refresh</Button>}
        filters={
          <>
            <label htmlFor="run-search">Search</label>
            <input id="run-search" placeholder="Search runs" />
          </>
        }
      />
    );

    expect(screen.getByLabelText("Search")).toBeInTheDocument();
    expect(screen.getByPlaceholderText("Search runs")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Refresh" })).toBeInTheDocument();
  });
});
