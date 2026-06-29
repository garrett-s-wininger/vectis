import { render, screen } from "@testing-library/react";
import { AppState } from "./AppState";
import { Button } from "../primitives/Button";

describe("AppState", () => {
  it("renders state copy and actions", () => {
    render(
      <AppState
        actions={<Button>Retry</Button>}
        description="The request did not complete."
        title="Unable to Load Runs"
        tone="error"
      />
    );

    expect(screen.getByRole("region", { name: "Unable to Load Runs" })).toBeInTheDocument();

    expect(screen.getByText("The request did not complete.")).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Retry" })).toBeInTheDocument();
  });
});
