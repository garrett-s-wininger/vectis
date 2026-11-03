import { useRef, useState, type FormEvent } from "react";
import { Button, FormError, FormField, PageHeader } from "../components";
import { ThemePreference } from "../components/ThemePreference";
import { useConsoleAccess } from "../domain/access";
import { VectisAPIError } from "../api/client";
import styles from "./ProfilePage.module.css";

type ProfilePageProps = { onChangePassword?: (currentPassword: string, newPassword: string) => Promise<void> };

export function ProfilePage({ onChangePassword }: ProfilePageProps) {
  const { can } = useConsoleAccess();
  const [values, setValues] = useState({ current: "", next: "", confirm: "" });
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const inFlight = useRef(false);
  const confirmRef = useRef<HTMLInputElement>(null);

  async function changePassword(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (inFlight.current || !onChangePassword) return;

    if (values.next !== values.confirm) {
      setError("The new passwords do not match.");
      confirmRef.current?.focus();

      return;
    }

    inFlight.current = true;
    setBusy(true);
    setError("");

    try {
      await onChangePassword(values.current, values.next);
      setValues({ current: "", next: "", confirm: "" });
    } catch (cause) {
      setError(
        cause instanceof VectisAPIError && cause.code === "password_auth_disabled"
          ? "This account uses external sign-in. Change its password with your identity provider."
          : cause instanceof Error
            ? cause.message
            : "Unable to change password."
      );
    } finally {
      inFlight.current = false;
      setBusy(false);
    }
  }

  return (
    <>
      <PageHeader title="Profile" description="Your account and console preferences." />
      <div className={styles.layout}>
        {onChangePassword && can("api:any") ? (
          <section className={`${styles.panel} polished-panel`} aria-labelledby="password-title">
            <div>
              <h2 id="password-title">Change Password</h2>
              <p>
                For local password sign-in. Changing your password signs you out and revokes existing sessions and API
                tokens.
              </p>
            </div>
            <form className={styles.form} onSubmit={changePassword} aria-busy={busy}>
              <FormField
                label="Current password"
                name="currentPassword"
                type="password"
                autoComplete="current-password"
                required
                disabled={busy}
                value={values.current}
                onChange={(event) => setValues({ ...values, current: event.target.value })}
              />
              <FormField
                label="New password"
                name="newPassword"
                type="password"
                autoComplete="new-password"
                required
                disabled={busy}
                value={values.next}
                onChange={(event) => setValues({ ...values, next: event.target.value })}
              />
              <FormField
                ref={confirmRef}
                label="Confirm new password"
                name="confirmNewPassword"
                type="password"
                autoComplete="new-password"
                required
                disabled={busy}
                value={values.confirm}
                onChange={(event) => setValues({ ...values, confirm: event.target.value })}
              />
              <FormError message={error} title="Unable to change password" />
              <Button className={styles.submit} type="submit" pending={busy} pendingLabel="Changing…">
                Change password
              </Button>
            </form>
          </section>
        ) : null}
        <section
          className={`${styles.panel} polished-panel polished-panel--overlay-safe`}
          aria-labelledby="preferences-title"
        >
          <div>
            <h2 id="preferences-title">Preferences</h2>
            <p>Saved in this browser for future visits, including sign-in. System follows your browser’s preference.</p>
          </div>
          <ThemePreference />
        </section>
      </div>
    </>
  );
}
