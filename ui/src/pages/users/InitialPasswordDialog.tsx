import { useLayoutEffect, useRef, type RefObject } from "react";
import { Button, CopyValue } from "../../components";
import type { CreatedUserCredential } from "../../data/consoleDataSource";
import styles from "../UsersPage.module.css";

type InitialPasswordDialogProps = {
  credential: CreatedUserCredential | null;
  returnFocusRef?: RefObject<HTMLButtonElement | null>;
  onDismissCredential: () => void;
};

export function InitialPasswordDialog({ credential, onDismissCredential, returnFocusRef }: InitialPasswordDialogProps) {
  const panelRef = useRef<HTMLElement>(null);
  useLayoutEffect(() => {
    if (!credential) return;
    const previous = document.activeElement;
    const fallback = returnFocusRef?.current;

    panelRef.current?.querySelector<HTMLButtonElement>("button")?.focus({ preventScroll: true });
    return () => {
      if (previous instanceof HTMLElement && previous !== document.body && previous.isConnected)
        previous.focus({ preventScroll: true });

      else if (fallback?.isConnected) fallback.focus({ preventScroll: true });
    };
  }, [credential, returnFocusRef]);

  if (!credential) return null;
  return (
    <div className={styles.modalOverlay} role="presentation">
      <section
        ref={panelRef}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            event.stopPropagation();
            onDismissCredential();

            return;
          }

          if (event.key !== "Tab") return;
          const buttons = Array.from(
            panelRef.current?.querySelectorAll<HTMLButtonElement>("button:not([disabled])") ?? []
          );

          const first = buttons[0];
          const last = buttons.at(-1);
          if (event.shiftKey && document.activeElement === first) {
            event.preventDefault();
            last?.focus();
          } else if (!event.shiftKey && document.activeElement === last) {
            event.preventDefault();
            first?.focus();
          }
        }}
        aria-describedby="new-user-credential-description"
        aria-labelledby="new-user-credential-title"
        aria-modal="true"
        className={`${styles.credentialPanel} polished-panel`}
        role="dialog"
      >
        <div className={styles.modalCopy}>
          <p className="eyebrow">One-Time Credential</p>
          <h2 id="new-user-credential-title">Initial Password</h2>
          <p id="new-user-credential-description">
            Share this password with {credential.username}. It will not be shown again after you leave this page.
          </p>
        </div>
        <div className={styles.credentialField}>
          <CopyValue value={credential.password} label={`initial password for ${credential.username}`}>
            <span className={styles.credentialValue} aria-label={`Initial password for ${credential.username}`}>
              {credential.password}
            </span>
          </CopyValue>
        </div>
        <Button className={styles.credentialDismiss} onClick={onDismissCredential} type="button" variant="quiet">
          Dismiss
        </Button>
      </section>
    </div>
  );
}
