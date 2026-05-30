import { render, screen } from "@testing-library/react";
import { MetricCard } from "./MetricCard";

describe("MetricCard", () => {
  it("renders a labelled metric with supporting detail", () => {
    render(
      <MetricCard label="Queue pressure" value="3 queued" detail="0 idle workers" />
    );

    expect(screen.getByText("Queue pressure")).toBeInTheDocument();
    expect(screen.getByText("3 queued")).toBeInTheDocument();
    expect(screen.getByText("0 idle workers")).toBeInTheDocument();
  });

  it("defaults to the neutral tone", () => {
    render(<MetricCard label="Services" value="Healthy" />);

    expect(screen.getByText("Services").closest("article")).toBeInTheDocument();
  });
});
