import { fireEvent, render, screen } from "@testing-library/react";
import { InitialPasswordDialog } from "./InitialPasswordDialog";

it("focuses the credential controls, traps keyboard navigation, and restores focus on Escape", () => {
  const opener = document.createElement("button");
  document.body.append(opener);
  opener.focus();

  const dismiss = vi.fn();
  const credential = { username: "mira", password: "secret-password" };
  const { rerender, unmount } = render(<InitialPasswordDialog credential={credential} onDismissCredential={dismiss} />);
  const copy = screen.getByRole("button", { name: /Copy initial password/ });
  const close = screen.getByRole("button", { name: "Dismiss" });
  expect(copy).toHaveFocus();

  fireEvent.keyDown(copy, { key: "Tab", shiftKey: true });
  expect(close).toHaveFocus();

  fireEvent.keyDown(close, { key: "Tab" });
  expect(copy).toHaveFocus();

  fireEvent.keyDown(copy, { key: "Escape" });
  expect(dismiss).toHaveBeenCalledOnce();

  rerender(<InitialPasswordDialog credential={null} onDismissCredential={dismiss} />);
  expect(opener).toHaveFocus();
  unmount();

  opener.remove();
});
