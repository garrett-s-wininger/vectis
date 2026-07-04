import { render, screen } from "@testing-library/react";
import { StatusBadge, type RunStatus } from "./StatusBadge";

describe("StatusBadge", () => {
  it.each<RunStatus>(["queued", "running", "succeeded", "failed", "cancelled", "abandoned", "orphaned", "aborted"])(
    "renders %s as readable text",
    (status) => {
      render(<StatusBadge status={status} />);

      expect(screen.getByText(new RegExp(status, "i"))).toBeInTheDocument();
    }
  );
});
