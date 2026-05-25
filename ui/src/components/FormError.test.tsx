import { render, screen } from "@testing-library/react";
import { FormError } from "./FormError";

describe("FormError", () => {
  it("renders alert text when a message is present", () => {
    render(<FormError message="Invalid credentials" />);

    expect(screen.getByRole("alert")).toHaveTextContent("Invalid credentials");
  });

  it("renders nothing without a message", () => {
    const { container } = render(<FormError />);

    expect(container).toBeEmptyDOMElement();
  });
});
