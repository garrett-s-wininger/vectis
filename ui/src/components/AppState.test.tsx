import { render, screen } from "@testing-library/react";
import { AppState } from "./AppState";
import { Button } from "./Button";

describe("AppState", () => {
  it("renders state copy and actions", () => {
    render(
      <AppState
        actions={<Button>Retry</Button>}
        description="The request did not complete."
        title="Unable to load runs"
        tone="error"
      />
    );

    expect(
      screen.getByRole("region", { name: "Unable to load runs" })
    ).toBeInTheDocument();

    expect(screen.getByText("The request did not complete.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });
});
