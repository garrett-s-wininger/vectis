import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it } from "vitest";
import { NotFoundPage } from "./NotFoundPage";

describe("NotFoundPage", () => {
  it("offers a path back to jobs", () => {
    window.history.replaceState(null, "", "/missing");

    render(<NotFoundPage />);

    expect(screen.getByRole("region", { name: "Page Not Found" })).toBeInTheDocument();

    fireEvent.click(screen.getByRole("button", { name: "Go to Jobs" }));

    expect(window.location.pathname).toBe("/jobs");
  });
});
