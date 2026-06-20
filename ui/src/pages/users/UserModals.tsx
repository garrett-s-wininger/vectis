import { Button } from "../../components";
import type { CreatedUserCredential } from "../../data/consoleDataSource";
import type { User } from "../../domain/console";
import styles from "../UsersPage.module.css";

type UserModalsProps = {
  canCopyCredential: boolean;
  credential: CreatedUserCredential | null;
  credentialCopyState: "copied" | "idle";
  onConfirmDeleteUser: () => Promise<void>;
  onCopyCredential: () => Promise<void>;
  onDismissCredential: () => void;
  onDismissDeleteUser: () => void;
  pendingDeleteUser: User | null;
};

export function UserModals({
  canCopyCredential,
  credential,
  credentialCopyState,
  onConfirmDeleteUser,
  onCopyCredential,
  onDismissCredential,
  onDismissDeleteUser,
  pendingDeleteUser
}: UserModalsProps) {
  return (
    <>
      {credential ? (
        <div className={styles.modalOverlay} role="presentation">
          <section
            aria-labelledby="new-user-credential-title"
            aria-modal="true"
            className={`${styles.credentialPanel} polished-panel`}
            role="dialog"
          >
            <div className={styles.modalCopy}>
              <p className="eyebrow">One-Time Credential</p>
              <h2 id="new-user-credential-title">Initial Password</h2>
              <p>Share this password with {credential.username}. It will not be shown again after you leave this page.</p>
            </div>
            <div className={styles.credentialField}>
              <div className={styles.credentialValue} aria-label={`Initial password for ${credential.username}`}>
                {credential.password}
              </div>
              <Button
                aria-label={`Copy initial password for ${credential.username}`}
                disabled={!canCopyCredential}
                onClick={onCopyCredential}
                type="button"
                variant="quiet"
              >
                {credentialCopyState === "copied" ? "Copied" : "Copy"}
              </Button>
            </div>
            <Button onClick={onDismissCredential} type="button" variant="quiet">
              Dismiss
            </Button>
          </section>
        </div>
      ) : null}
      {pendingDeleteUser ? (
        <div className={styles.modalOverlay} role="presentation">
          <section
            aria-labelledby="remove-user-title"
            aria-modal="true"
            className={`${styles.confirmPanel} polished-panel`}
            role="dialog"
          >
            <div className={styles.modalCopy}>
              <p className="eyebrow">Remove</p>
              <h2 id="remove-user-title">Remove User</h2>
              <p>Remove {pendingDeleteUser.username}? This cannot be undone.</p>
            </div>
            <div className={styles.formActions}>
              <Button onClick={onDismissDeleteUser} type="button" variant="quiet">
                Cancel
              </Button>
              <Button onClick={onConfirmDeleteUser} type="button" variant="danger">
                Remove User
              </Button>
            </div>
          </section>
        </div>
      ) : null}
    </>
  );
}
