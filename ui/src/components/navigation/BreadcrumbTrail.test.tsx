import { fireEvent, render, screen } from "@testing-library/react";
import { BreadcrumbTrail } from "./BreadcrumbTrail";

it("omits a location trail that only repeats the current page", () => {
  render(<BreadcrumbTrail label="Jobs location" items={[{ label: "Jobs", current: true }]} />);
  expect(screen.queryByRole("navigation")).not.toBeInTheDocument();
});

it("keeps parent navigation and identifies the current page", () => {
  const onJobs = vi.fn();
  render(
    <BreadcrumbTrail
      label="Job location"
      items={[
        { label: "Jobs", onClick: onJobs },
        { label: "build", current: true }
      ]}
    />
  );

  fireEvent.click(screen.getByRole("button", { name: "Jobs" }));
  expect(onJobs).toHaveBeenCalledOnce();
  expect(screen.getByText("build")).toHaveAttribute("aria-current", "page");
});
