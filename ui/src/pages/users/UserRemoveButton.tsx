import { useRef, useState } from "react";
import { Button } from "../../components";
import type { User } from "../../domain/console";
import styles from "../UsersPage.module.css";

export function UserRemoveButton({
  user,
  onRemove,
  disabled
}: {
  user: User;
  disabled?: boolean;
  onRemove: (user: User) => Promise<void> | void;
}) {
  const [confirm, setConfirm] = useState(false);
  const [busy, setBusy] = useState(false);
  const inFlight = useRef(false);

  async function remove() {
    if (inFlight.current) return;
    inFlight.current = true;
    setBusy(true);

    try {
      await onRemove(user);
    } catch {
      // NOTE(garrett): The app-level action alert owns removal failures. Require confirmation again for a retry.
    } finally {
      inFlight.current = false;
      setBusy(false);
      setConfirm(false);
    }
  }

  return (
    <Button
      className={styles.removeUserButton}
      disabled={disabled || busy || user.username === "admin"}
      onClick={() => {
        if (confirm) void remove();
        else setConfirm(true);
      }}
      onBlur={() => {
        if (!inFlight.current) setConfirm(false);
      }}
      onKeyDown={(event) => {
        if (event.key === "Escape") setConfirm(false);
      }}
      title={confirm ? `Click again to remove ${user.username}. Escape keeps the account.` : undefined}
      variant="danger"
    >
      {busy ? "Removing…" : confirm ? "Confirm removal" : "Remove User"}
    </Button>
  );
}
