import { render, screen } from "@testing-library/react";
import { Clock } from "lucide-react";
import { OperationalFact } from "./OperationalFact";

describe("OperationalFact", () => {
  it("renders a labelled operational value", () => {
    render(<OperationalFact icon={Clock} label="Elapsed" value="4m 12s" />);

    expect(screen.getByText("Elapsed")).toBeInTheDocument();
    expect(screen.getByText("4m 12s")).toBeInTheDocument();
  });
});
