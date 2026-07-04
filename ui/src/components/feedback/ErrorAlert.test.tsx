import { render, screen } from "@testing-library/react";
import { ErrorAlert } from "./ErrorAlert";

describe("ErrorAlert", () => {
  it("renders alert title and message", () => {
    render(<ErrorAlert message="The saved definition could not be enqueued." title="Action Failed" />);

    expect(screen.getByRole("alert")).toHaveTextContent("Action Failed");
    expect(screen.getByRole("alert")).toHaveTextContent("The saved definition could not be enqueued.");
  });

  it("renders nothing without a message", () => {
    const { container } = render(<ErrorAlert title="Action Failed" />);

    expect(container).toBeEmptyDOMElement();
  });
});
