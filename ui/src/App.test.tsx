import { render, screen } from "@testing-library/react";
import { App } from "./App";

describe("App", () => {
  it("renders the Vectis UI hello world", () => {
    render(<App />);

    expect(
      screen.getByRole("heading", { name: "Hello from Vectis UI" })
    ).toBeInTheDocument();
  });
});
