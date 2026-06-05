import { render, screen } from "@testing-library/react";
import { Button } from "./Button";

describe("Button", () => {
  it("renders an accessible button with a safe default type", () => {
    render(<Button>Refresh</Button>);

    const button = screen.getByRole("button", { name: "Refresh" });

    expect(button).toBeInTheDocument();
    expect(button).toHaveAttribute("type", "button");
  });

  it("supports shared visual variants", () => {
    render(
      <>
        <Button variant="quiet">Open</Button>
        <Button variant="danger">Delete</Button>
      </>
    );

    expect(screen.getByRole("button", { name: "Open" }).className).toContain("quiet");
    expect(screen.getByRole("button", { name: "Delete" }).className).toContain("danger");
  });
});
