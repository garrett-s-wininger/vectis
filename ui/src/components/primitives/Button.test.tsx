import { render, screen } from "@testing-library/react";
import { Button } from "./Button";

describe("Button", () => {
  it("renders an accessible button with a safe default type", () => {
    render(<Button>Refresh</Button>);

    const button = screen.getByRole("button", { name: "Refresh" });

    expect(button).toBeInTheDocument();
    expect(button).toHaveAttribute("type", "button");
  });
});
