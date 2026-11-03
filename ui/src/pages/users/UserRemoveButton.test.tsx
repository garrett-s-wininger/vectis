import { fireEvent, render, screen, waitFor } from "@testing-library/react";
import type { User } from "../../domain/console";
import { UserRemoveButton } from "./UserRemoveButton";

const user: User = { id: "12", username: "mira", role: "Operator", status: "active", lastSeen: "Today", tokens: 0 };

it("requires two clicks, with Escape and blur cancelling confirmation", () => {
  const remove = vi.fn();
  render(<UserRemoveButton user={user} onRemove={remove} />);

  fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
  expect(remove).not.toHaveBeenCalled();

  fireEvent.keyDown(screen.getByRole("button", { name: "Confirm removal" }), { key: "Escape" });
  fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
  fireEvent.blur(screen.getByRole("button", { name: "Confirm removal" }));
  expect(screen.getByRole("button", { name: "Remove User" })).toBeEnabled();
  expect(remove).not.toHaveBeenCalled();
});

it("allows one request at a time while removal is pending", async () => {
  let finish!: () => void;
  const remove = vi.fn(
    () =>
      new Promise<void>((resolve) => {
        finish = resolve;
      })
  );

  render(<UserRemoveButton user={user} onRemove={remove} />);
  fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm removal" }));

  const pending = screen.getByRole("button", { name: "Removing…" });
  expect(pending).toBeDisabled();

  fireEvent.click(pending);
  expect(remove).toHaveBeenCalledExactlyOnceWith(user);

  finish();
  await waitFor(() => expect(screen.getByRole("button", { name: "Remove User" })).toBeEnabled());
});

it("requires a fresh confirmation before retrying a failed removal", async () => {
  const remove = vi.fn().mockRejectedValueOnce(new Error("Unavailable")).mockResolvedValueOnce(undefined);
  render(<UserRemoveButton user={user} onRemove={remove} />);

  fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
  fireEvent.click(screen.getByRole("button", { name: "Confirm removal" }));
  await waitFor(() => expect(screen.getByRole("button", { name: "Remove User" })).toBeEnabled());

  fireEvent.click(screen.getByRole("button", { name: "Remove User" }));
  expect(remove).toHaveBeenCalledTimes(1);

  fireEvent.click(screen.getByRole("button", { name: "Confirm removal" }));
  await waitFor(() => expect(remove).toHaveBeenCalledTimes(2));
});
