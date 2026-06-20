import type { FormEvent } from "react";
import { useState } from "react";
import { BreadcrumbTrail } from "../components";
import { Button } from "../components";
import { DataTable, type DataTableColumn } from "../components";
import { EmptyStatePanel } from "../components";
import { FormField } from "../components";
import { MetricCard } from "../components";
import { PageHeader } from "../components";
import { SelectField } from "../components";
import type { CreatedUserCredential } from "../data/consoleDataSource";
import type { NewUser, User, UserRole, UserStatus } from "../domain/console";
import { userRoleOptions } from "../domain/consoleOptions";
import { EmptyStateRail, ResourceStatus, ResourceTitle, TableActions } from "./shared";
import styles from "./UsersPage.module.css";

type UsersPageProps = {
  onCreateUser: (input: NewUser) => Promise<CreatedUserCredential | undefined> | CreatedUserCredential | undefined;
  onDeleteUser: (userID: string) => Promise<void> | void;
  onUpdateUserStatus: (userID: string, status: UserStatus) => Promise<void> | void;
  users: User[];
};

export function UsersPage({ onCreateUser, onDeleteUser, onUpdateUserStatus, users }: UsersPageProps) {
  const [credential, setCredential] = useState<CreatedUserCredential | null>(null);
  const [credentialCopyState, setCredentialCopyState] = useState<"copied" | "idle">("idle");
  const [isCreating, setIsCreating] = useState(false);
  const [values, setValues] = useState<NewUser>({
    username: "",
    role: "Viewer"
  });

  async function handleSubmit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!values.username.trim()) {
      return;
    }

    try {
      const createdCredential = await onCreateUser(values);
      setCredentialCopyState("idle");
      setCredential(createdCredential ?? null);
      setValues({ username: "", role: "Viewer" });
      setIsCreating(false);
    } catch {
      // The app-level action alert owns the visible error; keep the form open for correction or retry.
    }
  }

  async function handleCopyCredential() {
    if (!credential || !canCopyCredential) {
      return;
    }

    await navigator.clipboard.writeText(credential.password);
    setCredentialCopyState("copied");
  }

  const canCopyCredential = hasSecureClipboard();
  const userStats = summarizeUsers(users);

  const columns: DataTableColumn<User>[] = [
    {
      header: "User",
      cell: (user) => <ResourceTitle subtitle={user.lastSeen} title={user.username} />
    },
    {
      header: "Role",
      cell: (user) => (
        <ResourceStatus className={styles.userChip} tone={roleTone(user.role)}>
          {user.role}
        </ResourceStatus>
      ),
      width: "120px"
    },
    {
      align: "end",
      header: "Status",
      cell: (user) => (
        <ResourceStatus className={styles.userChip} tone={user.status}>
          {user.status === "active" ? "Active" : "Disabled"}
        </ResourceStatus>
      )
    },
    {
      align: "end",
      header: "Actions",
      cell: (user) => (
        <TableActions className={styles.userActions}>
          <Button
            aria-label={user.status === "active" ? `Disable ${user.username}` : `Activate ${user.username}`}
            className={styles.userActionButton}
            onClick={() => onUpdateUserStatus(user.id, user.status === "active" ? "disabled" : "active")}
            variant="quiet"
          >
            {user.status === "active" ? "Disable" : "Activate"}
          </Button>
          <Button
            aria-label={`Remove ${user.username}`}
            className={styles.userActionButton}
            disabled={user.username === "admin"}
            onClick={() => onDeleteUser(user.id)}
            variant="quiet"
          >
            Remove
          </Button>
        </TableActions>
      ),
      width: "128px"
    }
  ];

  return (
    <>
      <PageHeader
        actions={
          !isCreating ? (
            <Button onClick={() => setIsCreating(true)} type="button">
              Create
            </Button>
          ) : null
        }
        description="Local accounts that can sign in to the console."
        navigation={
          <BreadcrumbTrail
            items={[
              { label: "Root" },
              { current: true, label: "Users" }
            ]}
            label="Users location"
          />
        }
        title="Users"
      />
      <div className={styles.workspace}>
        {isCreating ? (
          <section className={`${styles.createPanel} polished-panel polished-panel--accent-top`} aria-labelledby="create-user-title">
            <div className={styles.createCopy}>
              <p className="eyebrow">Create</p>
              <h2 id="create-user-title">New User</h2>
              <p>Add a local account. The API will generate the initial password until password entry is added here.</p>
            </div>
            <form className={styles.form} onSubmit={handleSubmit}>
              <FormField
                autoComplete="username"
                label="Username"
                name="newUsername"
                onChange={(event) => setValues({ ...values, username: event.target.value })}
                required
                value={values.username}
                wide
              />
              <SelectField
                label="Role"
                name="newUserRole"
                onChange={(event) =>
                  setValues({
                    ...values,
                    role: event.target.value as UserRole
                  })
                }
                options={userRoleOptions}
                value={values.role}
                wide
              />
              <div className={styles.formActions}>
                <Button onClick={() => setIsCreating(false)} type="button" variant="quiet">
                  Cancel
                </Button>
                <Button type="submit">Add User</Button>
              </div>
            </form>
          </section>
        ) : null}

        <section className={`${styles.summary} polished-panel polished-panel--accent-top`} aria-label="User summary">
          <div className={styles.summaryCopy}>
            <h2>Access</h2>
            <p>Account state for the local authentication provider.</p>
          </div>
          <div className={styles.summaryFacts} aria-label="User totals">
            <MetricCard detail="Local console accounts" label="Total" value={userStats.total} variant="plain" />
            <MetricCard detail="Can sign in" label="Active" value={userStats.active} variant="plain" />
            <MetricCard detail="Cannot sign in" label="Disabled" value={userStats.disabled} variant="plain" />
          </div>
        </section>

        {users.length > 0 ? (
          <DataTable columns={columns} emptyMessage="No users found." getRowKey={(user) => user.id} rows={users} />
        ) : (
          <EmptyStateRail>
            <EmptyStatePanel
              actions={
                <Button onClick={() => setIsCreating(true)} variant="quiet">
                  Create
                </Button>
              }
              description="Create a local account so another operator can sign in and manage Vectis."
              eyebrow="No Users"
              title="No Accounts Found"
            />
          </EmptyStateRail>
        )}
      </div>
      {credential ? (
        <div className={styles.credentialOverlay} role="presentation">
          <section
            aria-labelledby="new-user-credential-title"
            aria-modal="true"
            className={`${styles.credentialPanel} polished-panel`}
            role="dialog"
          >
            <div className={styles.credentialCopy}>
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
                onClick={handleCopyCredential}
                type="button"
                variant="quiet"
              >
                {credentialCopyState === "copied" ? "Copied" : "Copy"}
              </Button>
            </div>
            <Button onClick={() => setCredential(null)} type="button" variant="quiet">
              Dismiss
            </Button>
          </section>
        </div>
      ) : null}
    </>
  );
}

function hasSecureClipboard() {
  return Boolean(window.isSecureContext && navigator.clipboard?.writeText);
}

function summarizeUsers(users: User[]) {
  return users.reduce(
    (summary, user) => ({
      active: summary.active + (user.status === "active" ? 1 : 0),
      disabled: summary.disabled + (user.status === "disabled" ? 1 : 0),
      total: summary.total + 1
    }),
    { active: 0, disabled: 0, total: 0 }
  );
}

function roleTone(role: UserRole) {
  if (role === "Admin") {
    return "active";
  }

  if (role === "Operator") {
    return "enabled";
  }

  return "paused";
}
