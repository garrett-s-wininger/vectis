import { fireEvent, render, screen } from "@testing-library/react";
import { WorkspacePicker } from "./WorkspacePicker";

const paths = ["/", "/team-a", "/team-a/platform/build/east/nightly", "/team-b/platform/build"];

it("exposes deep ancestry without granting selection of unregistered parents", () => {
  const select = vi.fn();
  render(
    <WorkspacePicker
      namespaces={paths}
      value="/team-a/platform/build/east/nightly"
      onChange={select}
      accessLabel="Operator"
      namespaceHref="/namespaces?namespace=%2Fteam-a"
    />
  );

  const trigger = screen.getByRole("button", { name: "Namespace: /team-a/platform/build/east/nightly" });
  fireEvent.click(trigger);
  expect(screen.getByRole("treeitem", { name: "/team-a/platform/build/east/nightly" })).toHaveAttribute(
    "aria-level",
    "6"
  );

  const parent = screen.getByRole("treeitem", { name: "/team-a/platform" });
  expect(parent).not.toHaveAttribute("aria-selected");

  fireEvent.click(parent);
  expect(select).not.toHaveBeenCalled();
  expect(screen.getByLabelText("Effective access: Operator")).toBeInTheDocument();

  fireEvent.keyDown(screen.getByLabelText("Search namespaces"), { key: "Escape" });
  expect(trigger).toHaveFocus();
  expect(screen.queryByRole("tree")).not.toBeInTheDocument();
});

it("searches full paths, selects a nested namespace by keyboard, and closes", () => {
  const select = vi.fn();
  render(<WorkspacePicker namespaces={paths} value="/" onChange={select} />);

  fireEvent.click(screen.getByRole("button", { name: "Namespace: Root" }));
  const search = screen.getByRole("textbox", { name: "Search namespaces" });
  expect(search).toHaveFocus();

  fireEvent.change(search, { target: { value: "nightly" } });
  expect(screen.getByRole("button", { name: "Expanded /team-a" })).toBeDisabled();
  expect(screen.queryByRole("treeitem", { name: "/team-b/platform/build" })).not.toBeInTheDocument();

  fireEvent.keyDown(search, { key: "ArrowDown" });
  const result = screen.getByRole("treeitem", { name: "/team-a/platform/build/east/nightly" });
  expect(result).toHaveFocus();

  fireEvent.keyDown(result, { key: "Enter" });
  expect(select).toHaveBeenCalledExactlyOnceWith("/team-a/platform/build/east/nightly");
  expect(screen.queryByRole("tree")).not.toBeInTheDocument();
});

it("expands branches without selecting them and reports an empty search", () => {
  const select = vi.fn();
  render(<WorkspacePicker namespaces={paths} value="/" onChange={select} />);

  fireEvent.click(screen.getByRole("button", { name: "Namespace: Root" }));
  fireEvent.click(screen.getByRole("button", { name: "Expand /team-a" }));
  expect(select).not.toHaveBeenCalled();
  expect(screen.getByRole("treeitem", { name: "/team-a/platform" })).toBeInTheDocument();

  fireEvent.change(screen.getByLabelText("Search namespaces"), { target: { value: "missing" } });
  expect(screen.getByRole("status")).toHaveTextContent("No matching namespaces.");
  expect(screen.queryByRole("tree")).not.toBeInTheDocument();
});

it("keeps management navigation inside the popout and closes it when followed", () => {
  const navigate = vi.fn((_href, event) => event.preventDefault());
  render(
    <WorkspacePicker
      namespaces={paths}
      value="/"
      onChange={vi.fn()}
      namespaceHref="/namespaces?namespace=%2F"
      namespaceManagementHref="/namespaces"
      onNavigate={navigate}
    />
  );

  expect(screen.queryByRole("link", { name: "Manage namespaces" })).not.toBeInTheDocument();

  fireEvent.click(screen.getByRole("button", { name: "Namespace: Root" }));
  expect(screen.getByRole("link", { name: "View namespace" })).toHaveAttribute("href", "/namespaces?namespace=%2F");

  fireEvent.click(screen.getByRole("link", { name: "Manage namespaces" }));
  expect(navigate).toHaveBeenCalledWith("/namespaces", expect.anything());
  expect(screen.queryByRole("tree")).not.toBeInTheDocument();
});

it("does not select a namespace when a branch control receives a keyboard event", () => {
  const select = vi.fn();
  render(<WorkspacePicker namespaces={paths} value="/" onChange={select} />);

  fireEvent.click(screen.getByRole("button", { name: "Namespace: Root" }));
  const branch = screen.getByRole("button", { name: "Expand /team-a" });
  fireEvent.keyDown(branch, { key: "Enter" });
  expect(select).not.toHaveBeenCalled();

  fireEvent.click(branch);
  expect(screen.getByRole("treeitem", { name: "/team-a/platform" })).toBeInTheDocument();
});
