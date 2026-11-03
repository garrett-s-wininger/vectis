import { useRef, useState, type FormEvent } from "react";
import { Button, ResourceStatus, SelectField, type SelectOption } from "../../components";
import type { RoleBindingRole } from "../../domain/console";
import { InlineEmptyState } from "./InlineEmptyState";
import styles from "./RoleBindingPanel.module.css";

export type RoleBindingPanelRow = {
  caption: string;
  id: string;
  label: string;
  onRevoke: () => Promise<void> | void;
  role: RoleBindingRole;
};

type RoleBindingPanelProps = {
  ariaLabel: string;
  disabled?: boolean;
  onPendingChange?: (pending: boolean) => void;
  submitLabel?: string;
  canGrant: boolean;
  emptyMessage: string;
  onGrant: (event: FormEvent<HTMLFormElement>) => Promise<void> | void;
  onPrimaryChange: (value: string) => void;
  onRoleChange: (role: RoleBindingRole) => void;
  primaryLabel: string;
  primaryName: string;
  primaryOptions: SelectOption[];
  primaryValue: string;
  roleOptions: SelectOption[];
  roleTone: (role: RoleBindingRole) => "active" | "enabled" | "paused" | "disabled";
  roleValue: RoleBindingRole;
  rows: RoleBindingPanelRow[];
};

export function RoleBindingPanel({
  ariaLabel,
  disabled = false,
  onPendingChange,
  submitLabel = "Grant",
  canGrant,
  emptyMessage,
  onGrant,
  onPrimaryChange,
  onRoleChange,
  primaryLabel,
  primaryName,
  primaryOptions,
  primaryValue,
  roleOptions,
  roleTone,
  roleValue,
  rows
}: RoleBindingPanelProps) {
  const [pending, setPending] = useState<{ kind: "save"; label: string } | { kind: "revoke"; id: string } | null>(null);
  const inFlight = useRef(false);
  const busy = pending !== null;

  async function perform(action: NonNullable<typeof pending>, operation: () => Promise<void> | void) {
    if (disabled || inFlight.current) return;
    inFlight.current = true;
    setPending(action);
    onPendingChange?.(true);

    try {
      await operation();
    } catch {
      // NOTE(garrett): The app-level alert owns request failures; leave controls available for a retry.
    } finally {
      inFlight.current = false;
      setPending(null);
      onPendingChange?.(false);
    }
  }

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canGrant) return;

    void perform({ kind: "save", label: submitLabel === "Update Role" ? "Updating…" : "Granting…" }, () =>
      onGrant(event)
    );
  }
  return (
    <>
      <form className={styles.form} onSubmit={submit} aria-busy={busy}>
        <SelectField
          disabled={disabled || busy}
          label={primaryLabel}
          name={primaryName}
          onChange={(event) => onPrimaryChange(event.target.value)}
          options={primaryOptions}
          value={primaryValue}
          wide
        />
        <SelectField
          disabled={disabled || busy}
          label="Role"
          name={`${primaryName}Role`}
          onChange={(event) => onRoleChange(event.target.value as RoleBindingRole)}
          options={roleOptions}
          value={roleValue}
          wide
        />
        <Button
          disabled={disabled || busy || !canGrant}
          pending={pending?.kind === "save"}
          pendingLabel={
            pending?.kind === "save" ? pending.label : submitLabel === "Update Role" ? "Updating…" : "Granting…"
          }
          type="submit"
        >
          {submitLabel}
        </Button>
      </form>
      <div className={styles.list} role="group" aria-label={ariaLabel}>
        {rows.length > 0 ? (
          rows.map((row) => (
            <div className={styles.row} key={row.id}>
              <div>
                <strong>{row.label}</strong>
                <span>{row.caption}</span>
              </div>
              <ResourceStatus tone={roleTone(row.role)}>{row.role}</ResourceStatus>
              <Button
                disabled={disabled || busy}
                pending={pending?.kind === "revoke" && pending.id === row.id}
                pendingLabel="Revoking…"
                onClick={() => void perform({ kind: "revoke", id: row.id }, row.onRevoke)}
                type="button"
                variant="quiet"
              >
                Revoke
              </Button>
            </div>
          ))
        ) : (
          <InlineEmptyState>{emptyMessage}</InlineEmptyState>
        )}
      </div>
    </>
  );
}
