import { render, screen } from "@testing-library/react";
import { SignalList, type SignalItem } from "./SignalList";

const signals: SignalItem[] = [
  {
    id: "registry",
    label: "Registry",
    detail: "one peer lagging",
    state: "degraded"
  },
  {
    id: "reconciler",
    label: "Reconciler",
    state: "healthy"
  }
];

describe("SignalList", () => {
  it("renders signal labels, details, and states", () => {
    render(<SignalList signals={signals} />);

    expect(screen.getByText("Registry")).toBeInTheDocument();
    expect(screen.getByText("one peer lagging")).toBeInTheDocument();
    expect(screen.getByText("Degraded")).toBeInTheDocument();
    expect(screen.getByText("Reconciler")).toBeInTheDocument();
    expect(screen.getByText("Healthy")).toBeInTheDocument();
  });

  it("renders an empty state when no signals are present", () => {
    render(<SignalList signals={[]} />);

    expect(screen.getByText("No signals to show.")).toBeInTheDocument();
  });
});
