import type { FormEvent } from "react";
import { Button, ResourceStatus, SelectField, type SelectOption } from "../../components";
import type { RoleBindingRole } from "../../domain/console";
import { InlineEmptyState } from "./InlineEmptyState";
import styles from "./RoleBindingPanel.module.css";

export type RoleBindingPanelRow = {
  caption: string;
  id: string;
  label: string;
  onRevoke: () => void;
  role: RoleBindingRole;
};

type RoleBindingPanelProps = {
  ariaLabel: string;
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
  return (
    <>
      <form className={styles.form} onSubmit={onGrant}>
        <SelectField
          label={primaryLabel}
          name={primaryName}
          onChange={(event) => onPrimaryChange(event.target.value)}
          options={primaryOptions}
          value={primaryValue}
          wide
        />
        <SelectField
          label="Role"
          name={`${primaryName}Role`}
          onChange={(event) => onRoleChange(event.target.value as RoleBindingRole)}
          options={roleOptions}
          value={roleValue}
          wide
        />
        <Button disabled={!canGrant} type="submit">
          Grant
        </Button>
      </form>
      <div className={styles.list} aria-label={ariaLabel}>
        {rows.length > 0 ? (
          rows.map((row) => (
            <div className={styles.row} key={row.id}>
              <div>
                <strong>{row.label}</strong>
                <span>{row.caption}</span>
              </div>
              <ResourceStatus tone={roleTone(row.role)}>{row.role}</ResourceStatus>
              <Button onClick={row.onRevoke} type="button" variant="quiet">
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
