import { act, fireEvent, render, screen } from "@testing-library/react";
import { ProfilePage } from "./ProfilePage";
import { VectisAPIError } from "../api/client";

function fill(confirm = "new-password") {
  fireEvent.change(screen.getByLabelText("Current password"), { target: { value: "old-password" } });
  fireEvent.change(screen.getByLabelText("New password"), { target: { value: "new-password" } });
  fireEvent.change(screen.getByLabelText("Confirm new password"), { target: { value: confirm } });
}

describe("Profile password change", () => {
  it("requires matching confirmation and focuses the field", () => {
    const change = vi.fn();
    render(<ProfilePage onChangePassword={change} />);

    fill("different");
    fireEvent.click(screen.getByRole("button", { name: "Change password" }));
    expect(change).not.toHaveBeenCalled();
    expect(screen.getByText("The new passwords do not match.")).toBeInTheDocument();
    expect(screen.getByLabelText("Confirm new password")).toHaveFocus();
  });

  it("locks pending changes, sends the current password, and clears credentials on success", async () => {
    let finish!: () => void;
    const change = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        })
    );

    render(<ProfilePage onChangePassword={change} />);
    fill();
    fireEvent.click(screen.getByRole("button", { name: "Change password" }));
    const button = screen.getByRole("button", { name: "Changing…" });
    expect(button).toBeDisabled();

    fireEvent.click(button);
    expect(screen.getByLabelText("Current password")).toBeDisabled();
    expect(change).toHaveBeenCalledExactlyOnceWith("old-password", "new-password");

    await act(async () => finish());
    expect(screen.getByLabelText("Current password")).toHaveValue("");
    expect(screen.getByLabelText("New password")).toHaveValue("");
  });

  it("explains external sign-in and allows retry after a rejected request", async () => {
    const change = vi.fn().mockRejectedValue(new VectisAPIError(400, "password_auth_disabled", "disabled"));
    render(<ProfilePage onChangePassword={change} />);

    fill();
    fireEvent.click(screen.getByRole("button", { name: "Change password" }));
    expect(await screen.findByText(/This account uses external sign-in/)).toBeInTheDocument();
    expect(screen.getByRole("button", { name: "Change password" })).toBeEnabled();
  });

  it("does not show a password form without a password action", () => {
    render(<ProfilePage />);
    expect(screen.queryByLabelText("Current password")).not.toBeInTheDocument();
  });
});
