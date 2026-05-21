import { render, screen } from "@testing-library/react";
import { ProgressMeter } from "./ProgressMeter";

describe("ProgressMeter", () => {
  it("renders an accessible progressbar with label, value, and detail", () => {
    render(
      <ProgressMeter label="Log filesystem usage" value={68} detail="219 GB free" />
    );

    const meter = screen.getByRole("progressbar", {
      name: "Log filesystem usage"
    });

    expect(meter).toHaveAttribute("aria-valuenow", "68");
    expect(screen.getByText("68%")).toBeInTheDocument();
    expect(screen.getByText("219 GB free")).toBeInTheDocument();
  });

  it("clamps values into the valid progressbar range", () => {
    render(<ProgressMeter label="Queue capacity" value={140} />);

    expect(screen.getByRole("progressbar")).toHaveAttribute("aria-valuenow", "100");
    expect(screen.getByText("100%")).toBeInTheDocument();
  });
});
