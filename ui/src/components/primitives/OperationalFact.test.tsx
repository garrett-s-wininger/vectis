import { render, screen } from "@testing-library/react";
import { Clock } from "lucide-react";
import { OperationalFact } from "./OperationalFact";

describe("OperationalFact", () => {
  it("renders a labelled operational value", () => {
    render(
      <dl>
        <OperationalFact icon={Clock} label="Elapsed" value="4m 12s" />
      </dl>
    );

    expect(screen.getByRole("term")).toHaveTextContent("Elapsed");
    expect(screen.getByRole("definition")).toHaveTextContent("4m 12s");
  });
});
